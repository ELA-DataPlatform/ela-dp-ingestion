import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import yaml
from google.cloud import storage

from src.fetch.spotify import DataType as SpotifyDataType
from src.fetch.spotify import SpotifyConnector
from src.load.spotify import load as spotify_load
from src.writer import write

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config" / "ingestion.yaml"


def _load_ingestion_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f) or {}


def _archive_gcs_file(uri: str, project: str) -> None:
    """Move a GCS file from /landing/ to /archive/ after successful ingestion."""
    archive_uri = uri.replace("/landing/", "/archive/", 1)
    if archive_uri == uri:
        logger.warning(f"No /landing/ segment in {uri}, skipping archive")
        return
    path = uri[len("gs://"):]
    bucket_name, blob_name = path.split("/", 1)
    archive_blob_name = archive_uri[len(f"gs://{bucket_name}/"):]
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(blob_name)
    bucket.copy_blob(source_blob, bucket, archive_blob_name)
    source_blob.delete()
    logger.info(f"Archived {uri} → {archive_uri}")


def _get_destination(config: dict, source: str, data_type_value: str, env: str) -> tuple:
    mapping = config.get(source, {}).get(data_type_value, {})
    dataset = mapping.get("dataset")
    table = mapping.get("table")
    if dataset:
        dataset = dataset.format(env=env)
    return dataset, table


SOURCE_MAP = {
    "spotify": {
        "connector_cls": SpotifyConnector,
        "data_type_enum": SpotifyDataType,
    },
}


def _list_gcs_jsonl(gcs_prefix: str, project: str) -> list:
    """List all .jsonl files under a GCS prefix."""
    path = gcs_prefix[len("gs://"):]
    bucket_name, prefix = path.split("/", 1)
    client = storage.Client(project=project)
    blobs = client.list_blobs(bucket_name, prefix=prefix.rstrip("/") + "/")
    return [f"gs://{bucket_name}/{b.name}" for b in blobs if b.name.endswith(".jsonl")]


def _detect_data_type(uri: str, source: str, valid: dict):
    """Extract data_type from filename pattern: {ts}_{source}_{data_type}.jsonl"""
    filename = uri.rsplit("/", 1)[-1]
    match = re.search(rf"_{re.escape(source)}_(.+)\.jsonl$", filename)
    if match:
        return valid.get(match.group(1))
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch data from external sources.")
    parser.add_argument("--mode", choices=["fetch", "load", "all"], default="all",
                        help="fetch: API→GCS only | load: GCS→BQ only | all: fetch+load")
    parser.add_argument("--env", required=True, choices=["dev", "prd"])
    parser.add_argument("--source", required=True, choices=list(SOURCE_MAP))
    parser.add_argument("--data-types", nargs="+", metavar="DATA_TYPE",
                        help="Required for fetch/all. Auto-detected from filenames in load mode.")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument(
        "--output-dir",
        default="/app/output",
        help="Local path or GCS prefix (gs://bucket/path)",
    )
    parser.add_argument(
        "--gcs-dir",
        help="GCS folder to ingest (required for --mode load)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ingestion_config = _load_ingestion_config()
    project = f"ela-dp-{args.env}"

    source_cfg = SOURCE_MAP[args.source]
    data_type_enum = source_cfg["data_type_enum"]
    connector_cls = source_cfg["connector_cls"]

    valid = {dt.value: dt for dt in data_type_enum}

    # --- LOAD ONLY ---
    if args.mode == "load":
        if not args.gcs_dir:
            logger.error("--gcs-dir is required for --mode load")
            sys.exit(1)
        files = _list_gcs_jsonl(args.gcs_dir, project=project)
        if not files:
            logger.error(f"No .jsonl files found in {args.gcs_dir}")
            sys.exit(1)
        results = {"ok": [], "error": []}
        for uri in files:
            dt_enum = _detect_data_type(uri, args.source, valid)
            if dt_enum is None:
                logger.warning(f"Cannot detect data_type from {uri}, skipping")
                continue
            dataset, table = _get_destination(ingestion_config, args.source, dt_enum.value, args.env)
            try:
                spotify_load(uri, dt_enum, project=project,
                             **({} if dataset is None else {"dataset": dataset}),
                             **({} if table is None else {"table": table}))
                _archive_gcs_file(uri, project=project)
                results["ok"].append(uri)
            except Exception as e:
                logger.error(f"[{uri}] load failed: {e}")
                results["error"].append(uri)
        logger.info(f"Done — ok: {len(results['ok'])}, errors: {len(results['error'])}")
        if results["error"]:
            sys.exit(1)
        return

    # --- FETCH (+ optional load) ---
    if not args.data_types:
        logger.error("--data-types is required for --mode fetch/all")
        sys.exit(1)
    unknown = [dt for dt in args.data_types if dt not in valid]
    if unknown:
        logger.error(f"Unknown data types for {args.source}: {unknown}")
        logger.error(f"Available: {list(valid)}")
        sys.exit(1)
    data_type_enums = [valid[dt] for dt in args.data_types]
    connector = connector_cls.from_env()
    connector.authenticate(data_type_enums)

    ts = datetime.now().strftime("%Y_%m_%d_%H_%M")
    output_base = args.output_dir.rstrip("/")
    results = {"ok": [], "error": []}

    for dt_enum in data_type_enums:
        try:
            data = connector.fetch_data(dt_enum, limit=args.limit)
            if isinstance(data, dict):
                data = [data]

            filename = f"{ts}_{args.source}_{dt_enum.value}.jsonl"
            dest = f"{output_base}/{filename}"
            write(data, dest)
            if args.mode == "all" and dest.startswith("gs://"):
                dataset, table = _get_destination(ingestion_config, args.source, dt_enum.value, args.env)
                spotify_load(dest, dt_enum, project=project,
                             **({} if dataset is None else {"dataset": dataset}),
                             **({} if table is None else {"table": table}))
            results["ok"].append(dt_enum.value)
        except Exception as e:
            logger.error(f"[{dt_enum.value}] failed: {e}")
            results["error"].append(dt_enum.value)

    logger.info(f"Done — ok: {results['ok']}, errors: {results['error']}")
    if results["error"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
