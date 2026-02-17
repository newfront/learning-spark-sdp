"""VolumeUtils: helpers for working with data in Unity Catalog volumes."""

from __future__ import annotations

from pathlib import Path

from databricks.sdk import WorkspaceClient


class VolumeUtils:
    """Helpers for uploading and working with files in Unity Catalog volumes."""

    @staticmethod
    def upload_file(
        workspace_client: WorkspaceClient,
        file: Path,
        destination: str,
        *,
        overwrite: bool = True,
    ) -> bool:
        """Upload a local file to a Unity Catalog volume path.

        The destination is the full volume path (e.g. a directory or the full
        file path). If destination does not end with the file's name, the file
        is uploaded to destination/file.name.

        Example destination (directory):
            /Volumes/scotts_playground/demos/apps/schemas/protos/orders/v1
        The file will be written to:
            /Volumes/scotts_playground/demos/apps/schemas/protos/orders/v1/<file.name>

        Args:
            workspace_client: Authenticated Databricks WorkspaceClient.
            file: Local path to the file to upload.
            destination: Unity Catalog volume path (directory or full file path).
            overwrite: Whether to overwrite an existing file at the destination.

        Returns:
            True if the upload succeeded.

        Raises:
            FileNotFoundError: If the local file does not exist.
            Exception: On upload or permission errors.
        """
        file = Path(file)
        if not file.exists():
            raise FileNotFoundError(f"File not found: {file}")
        if not file.is_file():
            raise ValueError(f"Not a file: {file}")

        destination = destination.rstrip("/")
        if not destination.endswith(file.name):
            volume_path = f"{destination}/{file.name}"
        else:
            volume_path = destination

        with file.open("rb") as f:
            workspace_client.files.upload(
                volume_path,
                f,
                overwrite=overwrite,
            )
        return True
