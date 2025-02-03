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
        tags: list[str]
    ) -> None:
        self.batch_size = batch_size
        self.netbox_url = url
        self.api_token = token
        self.client = None
        self.base_device_url = f"{url}/dcim/devices/"

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        """
        Load Netbox credentials from the provided dictionary
        
        Expected credentials:
        - netbox_url: Base URL of the Netbox instance
        - netbox_api_token: API token for authentication
        """
        self.netbox_url = credentials.get("netbox_url")
        self.api_token = credentials.get("netbox_api_token")

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
            self.client.virtualization.clusters.count()
        except Exception as e:
            raise Exception(f"Failed to connect to Netbox: {e}")

        return None

    def _process_devices(
        self, start: datetime | None = None, end: datetime | None = None
    ) -> GenerateDocumentsOutput:
        """
        Retrieve and process Netbox devices, optionally filtered by last updated time
        """
        if self.client is None:
            raise ConnectorMissingCredentialError("Netbox")

        doc_batch: list[Document] = []

        # Retrieve all devices with as much detailed information as possible
        devices = self.client.dcim.devices.all()

        for device in devices:
            # Convert last updated to datetime and check time filters
            updated_at = device.last_updated.replace(tzinfo=timezone.utc)
            if start is not None and updated_at < start:
                continue
            if end is not None and updated_at > end:
                continue

            # Construct comprehensive device information
            device_details = {
                "name": device.name or "Unnamed Device",
                "device_type": device.device_type.display if device.device_type else "Unknown Type",
                "device_role": device.device_role.display if device.device_role else "Unassigned",
                "site": device.site.display if device.site else "No Site",
                "status": device.status.label if device.status else "Unknown",
                "serial": device.serial or "N/A",
                "asset_tag": device.asset_tag or "N/A"
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
        {"netbox_url": os.environ["NETBOX_BASE_URL"]},
        {"netbox_api_token": os.environ["NETBOX_API_TOKEN"]}
    )

    document_batches = connector.load_from_state()
    print(next(document_batches))
