"""
Spotify Data Loader
-------------------
Loads Spotify JSONL data from GCS into BigQuery.

Each data type maps to a table: {project}.spotify.{data_type}
"""

import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.fetch.spotify import DataType

logger = logging.getLogger(__name__)

DATASET = "spotify"


class SpotifyLoaderError(Exception):
    pass


def _ensure_dataset(client: bigquery.Client, project: str, dataset: str) -> None:
    dataset_ref = bigquery.DatasetReference(project, dataset)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = "europe-west1"
        client.create_dataset(ds)
        logger.info(f"Created dataset {project}.{dataset}")


def _build_bq_schema(fields: list) -> list:
    result = []
    for f in fields:
        mode = f.get("mode", "NULLABLE")
        sub_fields = f.get("fields")
        if sub_fields:
            nested = _build_bq_schema(sub_fields)
            result.append(bigquery.SchemaField(f["name"], f["type"], mode=mode, fields=nested))
        else:
            result.append(bigquery.SchemaField(f["name"], f["type"], mode=mode))
    return result


def load(
    gcs_uri: str,
    data_type: DataType,
    project: str,
    dataset: str = DATASET,
    table: str = None,
    schema: list = None,
) -> None:
    """Load a Spotify JSONL file from GCS into BigQuery (append)."""
    table_id = f"{project}.{dataset}.{table or data_type.value}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
        ),
    )

    if schema:
        job_config.schema = _build_bq_schema(schema)
        job_config.autodetect = False
    else:
        job_config.autodetect = True

    try:
        client = bigquery.Client(project=project)
        _ensure_dataset(client, project, dataset)
        load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result()
        logger.info(f"Loaded {gcs_uri} → {table_id} (schema={'explicit' if schema else 'autodetect'})")
    except Exception as e:
        raise SpotifyLoaderError(f"Failed to load {gcs_uri} into {table_id}: {e}") from e
