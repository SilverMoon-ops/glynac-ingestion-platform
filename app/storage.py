"""
Storage abstraction so the ingestion code doesn't care whether files land in
real MinIO or, for local dev/testing without Docker, a plain folder laid out
the same way MinIO would organize them. Switch via STORAGE_BACKEND in .env.
"""
from io import BytesIO
from pathlib import Path
from typing import List, Protocol

from app.config import settings


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
        # Load the optional dependency dynamically so type checkers do not
        # report an unresolved import when using the local backend.
        from importlib import import_module

        Minio = import_module("minio").Minio

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
        _storage_instance = MinioStorage(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            bucket=settings.minio_bucket,
            secure=settings.minio_secure,
        )
    else:
        _storage_instance = LocalFileStorage(settings.local_storage_root)
    return _storage_instance
