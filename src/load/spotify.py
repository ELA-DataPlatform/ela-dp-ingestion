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


def load(
    gcs_uri: str,
    data_type: DataType,
    project: str,
    dataset: str = DATASET,
    table: str = None,
) -> None:
    """Load a Spotify JSONL file from GCS into BigQuery."""
    table_name = table or data_type.value
    table_id = f"{project}.{dataset}.{table_name}"

    try:
        client = bigquery.Client(project=project)
        _ensure_dataset(client, project, dataset)

        # Reuse the existing schema so autodetect cannot reinterpret values
        # between files (for example, a STRING release_date as a DATE).
        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(project, dataset), table_name
        )
        try:
            existing_schema = client.get_table(table_ref).schema
            autodetect = False
        except NotFound:
            existing_schema = None
            autodetect = True

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=autodetect,
            schema=existing_schema,
            ignore_unknown_values=True,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field="_ingested_at",
            ),
        )

        load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result()
        logger.info(f"Loaded {gcs_uri} → {table_id}")
    except SpotifyLoaderError:
        raise
    except Exception as e:
        raise SpotifyLoaderError(f"Failed to load {gcs_uri} into {table_id}: {e}") from e
