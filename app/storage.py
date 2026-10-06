"""
Storage abstraction so the ingestion code doesn't care whether files land in
real MinIO or, for local dev/testing without Docker, a plain folder laid out
the same way MinIO would organize them. Switch via STORAGE_BACKEND in .env.

Includes automatic endpoint liveness check: if STORAGE_BACKEND=minio but Docker
is not running, it falls back to LocalFileStorage instantly without hanging.
"""
from io import BytesIO
from pathlib import Path
import socket
from typing import List, Protocol

from app.config import settings


def _is_endpoint_alive(endpoint: str, timeout: float = 0.5) -> bool:
    try:
        parts = endpoint.split(":", 1)
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else 80
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


class ObjectStorage(Protocol):
    def put_object(self, path: str, data: bytes) -> None: ...
    def list_objects(self, prefix: str = "") -> List[str]: ...
    def get_object(self, path: str) -> bytes: ...


class LocalFileStorage:
    """Mimics MinIO's flat object-path layout on local disk. Zero setup."""

    def __init__(self, root: str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def put_object(self, path: str, data: bytes) -> None:
        full_path = self.root / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)

    def list_objects(self, prefix: str = "") -> List[str]:
        results = []
        for p in self.root.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(self.root)).replace("\\", "/")
                if rel.startswith(prefix):
                    results.append(rel)
        return sorted(results)

    def get_object(self, path: str) -> bytes:
        return (self.root / path).read_bytes()


class MinioStorage:
    """Real MinIO backend — requires `docker compose up -d`."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str, secure: bool):
        from minio import Minio  # imported lazily so `minio` isn't required for local dev

        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        self.bucket = bucket
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)

    def put_object(self, path: str, data: bytes) -> None:
        self.client.put_object(self.bucket, path, BytesIO(data), length=len(data))

    def list_objects(self, prefix: str = "") -> List[str]:
        return [o.object_name for o in self.client.list_objects(self.bucket, prefix=prefix, recursive=True)]

    def get_object(self, path: str) -> bytes:
        response = self.client.get_object(self.bucket, path)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()


_storage_instance: ObjectStorage | None = None


def get_storage() -> ObjectStorage:
    global _storage_instance
    if _storage_instance is not None:
        return _storage_instance

    if settings.storage_backend == "minio":
        if _is_endpoint_alive(settings.minio_endpoint, timeout=0.5):
            try:
                _storage_instance = MinioStorage(
                    endpoint=settings.minio_endpoint,
                    access_key=settings.minio_access_key,
                    secret_key=settings.minio_secret_key,
                    bucket=settings.minio_bucket,
                    secure=settings.minio_secure,
                )
            except Exception as exc:
                print(f"[STORAGE] MinIO init failed ({exc}), falling back to LocalFileStorage")
                _storage_instance = LocalFileStorage(settings.local_storage_root)
        else:
            print(f"[STORAGE] MinIO endpoint {settings.minio_endpoint} is unreachable (Docker not running). Falling back to LocalFileStorage at {settings.local_storage_root}")
            _storage_instance = LocalFileStorage(settings.local_storage_root)
    else:
        _storage_instance = LocalFileStorage(settings.local_storage_root)
    return _storage_instance
