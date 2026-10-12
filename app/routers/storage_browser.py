"""Preview of raw landed objects (the 'MinIO file browser' in the console)."""
import io
import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.security import require_signed_request
from app.storage import get_storage

router = APIRouter(prefix="/api/storage", tags=["storage"], dependencies=[Depends(require_signed_request)])

MAX_PREVIEW_BYTES = 25 * 1024 * 1024


def _jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


@router.get("/preview")
def preview_object(path: str = Query(..., min_length=1), limit: int = Query(5, ge=1, le=50)):
    """First rows of a landed file: schema + sample for Parquet, records for JSON."""
    if path.startswith("/") or ".." in path.split("/"):
        raise HTTPException(status_code=400, detail="Invalid object path.")
    try:
        data = get_storage().get_object(path)
    except (FileNotFoundError, KeyError):
        raise HTTPException(status_code=404, detail=f"No such object: {path}")
    except Exception as exc:
        if type(exc).__name__ == "S3Error" and getattr(exc, "code", "") in ("NoSuchKey", "NoSuchObject"):
            raise HTTPException(status_code=404, detail=f"No such object: {path}")
        raise
    if len(data) > MAX_PREVIEW_BYTES:
        raise HTTPException(status_code=413, detail="Object too large to preview.")

    if path.endswith(".parquet"):
        import pyarrow.parquet as pq

        table = pq.read_table(io.BytesIO(data))
        return {
            "path": path, "format": "parquet", "size_bytes": len(data), "num_rows": table.num_rows,
            "columns": [{"name": f.name, "type": str(f.type)} for f in table.schema],
            "rows": _jsonable(table.slice(0, limit).to_pylist()),
        }
    if path.endswith(".json") or path.endswith(".jsonl"):
        text = data.decode("utf-8", errors="replace")
        try:
            loaded = json.loads(text)
            records = loaded if isinstance(loaded, list) else [loaded]
        except ValueError:  # JSON lines
            records = [json.loads(line) for line in text.splitlines() if line.strip()]
        return {
            "path": path, "format": "json", "size_bytes": len(data), "num_rows": len(records),
            "columns": [{"name": k, "type": type(v).__name__} for k, v in (records[0].items() if records else [])],
            "rows": _jsonable(records[:limit]),
        }
    return {"path": path, "format": "text", "size_bytes": len(data), "num_rows": None, "columns": [],
            "rows": [], "text": data[:2000].decode("utf-8", errors="replace")}
