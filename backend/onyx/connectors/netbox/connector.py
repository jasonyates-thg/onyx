from datetime import datetime
from datetime import timezone
from typing import Any

import requests
import pynetbox

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import GenerateDocumentsOutput
from onyx.connectors.interfaces import LoadConnector
from onyx.connectors.interfaces import PollConnector
from onyx.connectors.interfaces import SecondsSinceUnixEpoch
from onyx.connectors.models import ConnectorMissingCredentialError
from onyx.connectors.models import Document
from onyx.connectors.models import Section
from onyx.utils.logger import setup_logger

logger = setup_logger()

class NetboxConnector(LoadConnector, PollConnector):
    def __init__(
        self, batch_size: int = INDEX_BATCH_SIZE, 
        url: str | None = None, 
        token: str | None = None,
        tags: list[str] = []
    ) -> None:
        self.batch_size = batch_size
        self.netbox_url = url
        self.api_token = token
        self.tags = tags or []
        self.client = None
        self.base_device_url = f"{url}/dcim/devices/"

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        """
        Load Netbox credentials from the provided dictionary
        
        Expected credentials:
        - netbox_url: Base URL of the Netbox instance
        - netbox_api_token: API token for authentication
        """
        self.netbox_url = credentials["netbox_base_url"]
        self.api_token = credentials["netbox_api_token"]

        if not self.netbox_url or not self.api_token:
            raise ConnectorMissingCredentialError("Netbox")

        # Initialize Netbox client
        self.client = pynetbox.api(
            url=self.netbox_url, 
            token=self.api_token
        )
        
        # Validate connection
        try:
            # Quick test to ensure connection works
            self.client.status()
        except Exception as e:
            raise Exception(f"Failed to connect to Netbox: {e}")

        return None

    def _parse_datetime(self, dt_str: str) -> datetime:
        """
        Parse datetime string and convert to UTC datetime
        """
        try:
            # Parse the datetime string to a datetime object
            parsed_dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            
            # Ensure it's in UTC
            return parsed_dt.astimezone(timezone.utc)
        except Exception as e:
            logger.warning(f"Failed to parse datetime {dt_str}: {e}")
            return datetime.now(timezone.utc)

    def _ensure_utc(self, dt: datetime) -> datetime:
        """
        Ensure datetime is in UTC timezone, converting if necessary
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _process_devices(
        self, start: datetime | None = None, end: datetime | None = None
    ) -> GenerateDocumentsOutput:
        """
        Retrieve and process Netbox devices, optionally filtered by last updated time and tags
        """
        if self.client is None:
            raise ConnectorMissingCredentialError("Netbox")

        # Ensure start and end datetimes are in UTC if provided
        if start is not None:
            start = self._ensure_utc(start)
        if end is not None:
            end = self._ensure_utc(end)

        doc_batch: list[Document] = []

        # Retrieve devices with tag filtering
        if self.tags:
            # Use list comprehension to filter devices with matching tags
            devices = [
                device for device in self.client.dcim.devices.all() 
                if any(tag.name in self.tags for tag in device.tags)
            ]
        else:
            # If no tags specified, retrieve all devices
            devices = self.client.dcim.devices.all()

        for device in devices:
            # Convert last updated to datetime and check time filters
            try:
                # Use the custom datetime parsing method
                updated_at = self._parse_datetime(str(device.last_updated))
            except Exception as e:
                logger.warning(f"Could not parse last_updated for device {device.name}: {e}")
                continue

            if start is not None and updated_at < start:
                continue
            if end is not None and updated_at > end:
                continue

            # Construct comprehensive device information
            device_details = {
                "name": device.name or "Unnamed Device",
                "device_type": str(device.device_type.display) if device.device_type else "Unknown Type",
                "device_role": str(device.device_role.display) if device.device_role else "Unassigned",
                "site": str(device.site.display) if device.site else "No Site",
                "status": str(device.status.label) if device.status else "Unknown",
                "serial": device.serial or "N/A",
                "asset_tag": device.asset_tag or "N/A",
                "tags": [str(tag.name) for tag in device.tags]  # Include tags in metadata
            }

            # Construct a detailed text description
            content_text = "\n".join([
                f"{key}: {value}" for key, value in device_details.items()
            ])

            # Add interfaces information
            try:
                interfaces = self.client.dcim.interfaces.filter(device_id=device.id)
                interface_details = "\n\nInterfaces:\n" + "\n".join([
                    f"- {intf.name}: {intf.type.label}, {intf.mac_address or 'No MAC'}" 
                    for intf in interfaces
                ])
                content_text += interface_details
            except Exception as e:
                logger.warning(f"Could not retrieve interfaces for device {device.name}: {e}")

            # Create device URL
            device_url = f"{self.base_device_url}{device.id}"

            doc_batch.append(
                Document(
                    id=str(device.id),
                    sections=[Section(link=device_url, text=content_text)],
                    source=DocumentSource.NETBOX,
                    semantic_identifier=device_details["name"],
                    doc_updated_at=updated_at,
                    metadata=device_details
                )
            )

            # Yield batches of documents
            if len(doc_batch) >= self.batch_size:
                yield doc_batch
                doc_batch = []

        # Yield any remaining documents
        if doc_batch:
            yield doc_batch

    def load_from_state(self) -> GenerateDocumentsOutput:
        """
        Load all devices without time filtering
        """
        return self._process_devices()

    def poll_source(
        self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch
    ) -> GenerateDocumentsOutput:
        """
        Poll devices modified between the specified timestamp range
        """
        start_datetime = datetime.utcfromtimestamp(start)
        end_datetime = datetime.utcfromtimestamp(end)
        return self._process_devices(start_datetime, end_datetime)


if __name__ == "__main__":
    import os

    connector = NetboxConnector()
    connector.load_credentials(
        {"netbox_base_url": os.environ["NETBOX_BASE_URL"]},
        {"netbox_api_token": os.environ["NETBOX_API_TOKEN"]}
    )

    document_batches = connector.load_from_state()
    print(next(document_batches))
