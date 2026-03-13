import argparse
import logging
import sys
from datetime import datetime

from src.fetch.spotify import DataType as SpotifyDataType
from src.fetch.spotify import SpotifyConnector
from src.load.spotify import load as spotify_load
from src.writer import write

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

SOURCE_MAP = {
    "spotify": {
        "connector_cls": SpotifyConnector,
        "data_type_enum": SpotifyDataType,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch data from external sources.")
    parser.add_argument("--env", required=True, choices=["dev", "prd"])
    parser.add_argument("--source", required=True, choices=list(SOURCE_MAP))
    parser.add_argument("--data-types", required=True, nargs="+", metavar="DATA_TYPE")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument(
        "--output-dir",
        default="/app/output",
        help="Local path or GCS prefix (gs://bucket/path)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    source_cfg = SOURCE_MAP[args.source]
    data_type_enum = source_cfg["data_type_enum"]
    connector_cls = source_cfg["connector_cls"]

    # Validate data types
    valid = {dt.value: dt for dt in data_type_enum}
    unknown = [dt for dt in args.data_types if dt not in valid]
    if unknown:
        logger.error(f"Unknown data types for {args.source}: {unknown}")
        logger.error(f"Available: {list(valid)}")
        sys.exit(1)
    data_type_enums = [valid[dt] for dt in args.data_types]

    # Connect & authenticate
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
            if dest.startswith("gs://"):
                spotify_load(dest, dt_enum, project=f"ela-dp-{args.env}")
            results["ok"].append(dt_enum.value)
        except Exception as e:
            logger.error(f"[{dt_enum.value}] fetch failed: {e}")
            results["error"].append(dt_enum.value)

    logger.info(f"Done — ok: {results['ok']}, errors: {results['error']}")
    if results["error"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
