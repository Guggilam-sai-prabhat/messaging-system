# services/storage_service.py

"""
Object storage via MinIO (S3-compatible).

Why MinIO instead of raw filesystem?
  1. S3 API — when you move to AWS/GCP, change the endpoint
     URL and credentials. Nothing else touches storage directly.
  2. Built-in checksums — MinIO verifies integrity on write.
     We still compute SHA-256 ourselves for dedup, but the
     transport layer has its own checks.
  3. Presigned URLs — when you add download endpoints, you
     generate a time-limited URL and redirect. No file proxy
     through your FastAPI server, no memory pressure.
  4. Multipart upload — MinIO handles chunked uploads natively.
     For files approaching the 10MB limit, this matters.

Why synchronous minio-py in an async service?
  The minio Python client is synchronous (it uses urllib3).
  We wrap calls in run_in_executor so they don't block the
  event loop. The alternative (aiobotocore) is heavier and
  ties you to the AWS SDK. For a service that does one
  put_object per upload, the executor overhead is negligible.

Object key layout:
  {channelId}/{documentId}.pdf

  Same sharding rationale as the filesystem approach — one
  "directory" per channel. S3 doesn't have real directories,
  but the prefix-based listing is O(matching keys), not O(all
  keys), so the channel prefix keeps list operations fast.
"""

import hashlib
import logging
import asyncio
import io
from functools import partial
from typing import BinaryIO

from minio import Minio
from minio.error import S3Error

from app.config import settings

logger = logging.getLogger("storage")


class StorageError(Exception):
    pass


class StorageService:

    def __init__(self):
        self._client = Minio(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_use_ssl,
        )
        self._bucket = settings.minio_bucket

    async def initialize(self) -> None:
        """Ensure the bucket exists. Call from lifespan startup.

        Idempotent — safe to call on every server start.
        If the bucket already exists, this is a no-op.
        """
        loop = asyncio.get_running_loop()
        exists = await loop.run_in_executor(
            None, self._client.bucket_exists, self._bucket
        )
        if not exists:
            await loop.run_in_executor(
                None, self._client.make_bucket, self._bucket
            )
            logger.info(f"Created MinIO bucket: {self._bucket}")
        else:
            logger.info(f"MinIO bucket exists: {self._bucket}")

    async def save_file(
        self,
        channel_id: str,
        document_id: str,
        file_obj: BinaryIO,
        suffix: str = ".pdf",
    ) -> tuple[str, int, str]:
        """Upload file to MinIO.

        Returns (object_key, size_bytes, sha256_hex).

        We read the full file into memory here. "Wait, didn't
        you just say streaming is better?" Yes, for filesystem
        writes. But MinIO's put_object needs either a known
        content length or a seekable stream. Since we already
        enforce a 10MB cap, the worst case is 10MB in memory
        per concurrent upload. With 20 concurrent uploads
        that's 200MB — acceptable for a messaging service.

        The SHA-256 is computed during the read, same single
        pass approach.
        """
        object_key = f"{channel_id}/{document_id}{suffix}"

        # Read into memory + hash in one pass
        hasher = hashlib.sha256()
        chunks = []
        while True:
            chunk = file_obj.read(8192)
            if not chunk:
                break
            chunks.append(chunk)
            hasher.update(chunk)

        data = b"".join(chunks)
        size = len(data)
        sha256 = hasher.hexdigest()

        if size == 0:
            raise StorageError("File is empty")

        # Upload to MinIO in a thread (synchronous client)
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                partial(
                    self._client.put_object,
                    bucket_name=self._bucket,
                    object_name=object_key,
                    data=io.BytesIO(data),
                    length=size,
                    content_type="application/pdf",
                ),
            )
        except S3Error as e:
            raise StorageError(f"MinIO upload failed: {e}")

        logger.info(
            f"Stored {object_key} ({size} bytes, "
            f"sha256={sha256[:12]}...)"
        )
        return object_key, size, sha256

    async def delete_file(self, object_key: str) -> bool:
        """Remove an object from MinIO.

        MinIO's remove_object doesn't error if the key
        doesn't exist, so this is always idempotent.
        """
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                partial(
                    self._client.remove_object,
                    bucket_name=self._bucket,
                    object_name=object_key,
                ),
            )
            return True
        except S3Error as e:
            raise StorageError(f"MinIO delete failed: {e}")

    async def get_presigned_url(
        self,
        object_key: str,
        expires_seconds: int = 3600,
    ) -> str:
        """Generate a presigned download URL.

        This is how you serve files without proxying bytes
        through FastAPI. The client gets a time-limited URL
        that points directly at MinIO. After expiry, the
        URL is dead.

        For production with a CDN in front:
          Replace this with CloudFront signed URLs or
          configure MinIO behind nginx with proxy_pass.
        """
        from datetime import timedelta
        loop = asyncio.get_running_loop()
        try:
            url = await loop.run_in_executor(
                None,
                partial(
                    self._client.presigned_get_object,
                    bucket_name=self._bucket,
                    object_name=object_key,
                    expires=timedelta(seconds=expires_seconds),
                ),
            )
            return url
        except S3Error as e:
            raise StorageError(f"Presigned URL generation failed: {e}")

    async def file_exists(self, object_key: str) -> bool:
        """Check if an object exists in the bucket."""
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                partial(
                    self._client.stat_object,
                    bucket_name=self._bucket,
                    object_name=object_key,
                ),
            )
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            raise StorageError(f"MinIO stat failed: {e}")