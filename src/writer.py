import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


def _to_jsonl(data: list) -> str:
    ingested_at = datetime.now(timezone.utc).isoformat()
    return "\n".join(
        json.dumps({**item, "_ingested_at": ingested_at}, default=str) for item in data
    ) + "\n"


def write_local(data: list, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_to_jsonl(data))
    logger.info(f"Wrote {len(data)} items → {path}")


def write_gcs(data: list, gcs_uri: str) -> None:
    from google.cloud import storage

    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")

    path_without_scheme = gcs_uri[5:]
    bucket_name, blob_name = path_without_scheme.split("/", 1)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(_to_jsonl(data), content_type="application/x-ndjson")
    logger.info(f"Wrote {len(data)} items → {gcs_uri}")


def write(data: list, dest: str) -> None:
    """Write data to either a local path or a GCS URI (gs://...)."""
    if dest.startswith("gs://"):
        write_gcs(data, dest)
    else:
        write_local(data, Path(dest))
