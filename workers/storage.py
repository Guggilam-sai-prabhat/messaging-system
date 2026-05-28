from minio import Minio
from minio.error import S3Error

from workers.config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_USE_SSL,
)


class StorageClient:
    """Thin synchronous wrapper around MinIO used by the worker."""

    def __init__(self) -> None:
        self._client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_USE_SSL,
        )

    def get_object_bytes(self, object_key: str) -> bytes:
        """Download the full object into memory. Synchronous."""
        response = None
        try:
            response = self._client.get_object(MINIO_BUCKET, object_key)
            return response.read()
        except S3Error as e:
            raise RuntimeError(f"MinIO fetch failed for {object_key}: {e}") from e
        finally:
            if response:
                response.close()
                response.release_conn()
