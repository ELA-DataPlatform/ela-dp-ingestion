"""
Lyrics Fetcher — LRCLIB
------------------------
Fetches lyrics from LRCLIB API for Spotify tracks stored in BigQuery.
Writes results to GCS and loads into BigQuery.
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

LRCLIB_BASE_URL = "https://lrclib.net/api/get"
USER_AGENT = "ELA-DATAPLATFORM/1.0"
SLEEP_BETWEEN_CALLS = 0.3
MAX_RETRIES = 2

SOURCE_PROJECT = "ela-dp-prd"
SOURCE_TABLE = "dp_product_prd.pct_d4lyrics__tracks_enriched"


def get_tracks_to_fetch(project: str, env: str, limit: int) -> list[dict]:
    """Query BQ for tracks not yet fetched."""
    client = bigquery.Client(project=SOURCE_PROJECT)
    dataset = f"dp_lake_lyrics_{env}"
    target_table = f"{project}.{dataset}.normalized__lyrics"

    try:
        client_dest = bigquery.Client(project=project)
        client_dest.get_table(target_table)
        has_target = True
    except NotFound:
        has_target = False

    if has_target:
        query = f"""
        SELECT DISTINCT
            t.track_id,
            t.track_name,
            t.main_artist AS artist_name,
            t.album_name,
            CAST(t.duration_seconds AS INT64) AS duration_s
        FROM `{SOURCE_PROJECT}.{SOURCE_TABLE}` t
        LEFT JOIN `{target_table}` l
            ON t.track_name = l.track_name
            AND t.main_artist = l.artist_name
            AND t.album_name = l.album_name
        WHERE l.track_name IS NULL
        LIMIT {limit}
        """
    else:
        query = f"""
        SELECT DISTINCT
            t.track_id,
            t.track_name,
            t.main_artist AS artist_name,
            t.album_name,
            CAST(t.duration_seconds AS INT64) AS duration_s
        FROM `{SOURCE_PROJECT}.{SOURCE_TABLE}` t
        LIMIT {limit}
        """

    rows = client.query(query).result()
    tracks = [dict(row) for row in rows]
    logger.info(f"Found {len(tracks)} tracks to fetch")
    return tracks


def fetch_lyrics(track: dict) -> dict | None:
    """Call LRCLIB API for a single track. Returns result dict or None on error."""
    params = {
        "track_name": track["track_name"],
        "artist_name": track["artist_name"],
        "album_name": track["album_name"],
        "duration": track["duration_s"],
    }
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.get(LRCLIB_BASE_URL, params=params, headers=headers, timeout=10)

            if resp.status_code == 404:
                logger.warning(f"Not found: {track['artist_name']} - {track['track_name']}")
                return {
                    "track_id": track["track_id"],
                    "track_name": track["track_name"],
                    "artist_name": track["artist_name"],
                    "album_name": track["album_name"],
                    "duration_s": track["duration_s"],
                    "lrclib_id": None,
                    "instrumental": None,
                    "plain_lyrics": None,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "found": False,
                }

            resp.raise_for_status()
            data = resp.json()

            return {
                "track_id": track["track_id"],
                "track_name": track["track_name"],
                "artist_name": track["artist_name"],
                "album_name": track["album_name"],
                "duration_s": track["duration_s"],
                "lrclib_id": data.get("id"),
                "instrumental": data.get("instrumental"),
                "plain_lyrics": data.get("plainLyrics"),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "found": True,
            }

        except requests.exceptions.HTTPError:
            if resp.status_code >= 500 and attempt < MAX_RETRIES:
                wait = 2 ** attempt
                logger.warning(f"Server error {resp.status_code}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            logger.error(f"HTTP error for {track['artist_name']} - {track['track_name']}: {resp.status_code}")
            return None

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES:
                wait = 2 ** attempt
                logger.warning(f"Network error, retrying in {wait}s: {e}")
                time.sleep(wait)
                continue
            logger.error(f"Request failed for {track['artist_name']} - {track['track_name']}: {e}")
            return None

    return None


def write_to_gcs(results: list[dict], env: str) -> str:
    """Write results as JSONL to GCS. Returns the GCS URI."""
    timestamp = datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M")
    bucket_name = f"ela-source-{env}"
    blob_path = f"lyrics/landing/lyrics_{timestamp}.jsonl"
    gcs_uri = f"gs://{bucket_name}/{blob_path}"

    ingested_at = datetime.now(timezone.utc).isoformat()
    jsonl = "\n".join(
        json.dumps({**record, "_ingested_at": ingested_at}, default=str)
        for record in results
    ) + "\n"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(jsonl, content_type="application/x-ndjson")
    logger.info(f"Wrote {len(results)} records → {gcs_uri}")
    return gcs_uri


def load_to_bigquery(gcs_uri: str, project: str, env: str) -> None:
    """Load JSONL from GCS into BigQuery."""
    dataset = f"dp_lake_lyrics_{env}"
    table_id = f"{project}.{dataset}.normalized__lyrics"

    client = bigquery.Client(project=project)

    dataset_ref = bigquery.DatasetReference(project, dataset)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = "EU"
        client.create_dataset(ds)
        logger.info(f"Created dataset {dataset}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="_ingested_at",
        ),
    )

    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config, location="EU")
    load_job.result()
    logger.info(f"Loaded {gcs_uri} → {table_id}")


def archive_gcs_file(gcs_uri: str, project: str) -> None:
    """Move file from /landing/ to /archive/."""
    archive_uri = gcs_uri.replace("/landing/", "/archive/", 1)
    path = gcs_uri[len("gs://"):]
    bucket_name, blob_name = path.split("/", 1)
    archive_blob_name = archive_uri[len(f"gs://{bucket_name}/"):]

    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(blob_name)
    bucket.copy_blob(source_blob, bucket, archive_blob_name)
    source_blob.delete()
    logger.info(f"Archived {gcs_uri} → {archive_uri}")


def main():
    parser = argparse.ArgumentParser(description="Fetch lyrics from LRCLIB for Spotify tracks")
    parser.add_argument("--env", required=True, choices=["dev", "prd"])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--output", help="Local file path for JSONL output (skips GCS/BQ/archive)")
    args = parser.parse_args()

    project = f"ela-dp-{args.env}"

    # 1. Get tracks to fetch
    tracks = get_tracks_to_fetch(project, args.env, args.limit)
    if not tracks:
        logger.info("No new tracks to fetch, exiting")
        return

    # 2. Fetch lyrics
    results = []
    stats = {"found": 0, "not_found": 0, "errors": 0}

    for i, track in enumerate(tracks, 1):
        if i > 1:
            time.sleep(SLEEP_BETWEEN_CALLS)

        logger.info(f"[{i}/{len(tracks)}] {track['artist_name']} - {track['track_name']}")
        result = fetch_lyrics(track)

        if result is None:
            stats["errors"] += 1
            continue

        results.append(result)
        if result["found"]:
            stats["found"] += 1
        else:
            stats["not_found"] += 1

    if not results:
        logger.warning("No results to write")
        return

    if args.output:
        # Local mode: write JSONL to local file
        ingested_at = datetime.now(timezone.utc).isoformat()
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            for record in results:
                f.write(json.dumps({**record, "_ingested_at": ingested_at}, default=str) + "\n")
        logger.info(f"Wrote {len(results)} records → {path}")
    else:
        # GCS + BQ pipeline
        gcs_uri = write_to_gcs(results, args.env)
        load_to_bigquery(gcs_uri, project, args.env)
        archive_gcs_file(gcs_uri, project)

    # Summary
    logger.info(
        f"Done — {len(tracks)} tracks processed: "
        f"{stats['found']} found, {stats['not_found']} not found, {stats['errors']} errors"
    )


if __name__ == "__main__":
    main()
