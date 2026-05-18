#!/usr/bin/env python3
"""
wake_watcher.py — triggers a Cloud Workflow when today's Garmin sleep data is available.

Designed to run as a Cloud Run Job on a frequent schedule (e.g. every 15 min,
05:00–12:00). Uses a GCS marker file to ensure the workflow is triggered at
most once per day.

Required environment variables:
    GARMIN_USERNAME
    GARMIN_PASSWORD
    GARMIN_TOKENSTORE_GCS
    WATCHER_BUCKET      gs://ela-source-{env}/garmin/watcher
    WORKFLOW_PROJECT    ela-dp-dev
    WORKFLOW_LOCATION   europe-west1
    WORKFLOW_NAME       PLACEHOLDER — set to the actual Cloud Workflow name
"""

import logging
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import google.auth
import google.auth.transport.requests
import requests
from google.cloud import storage

from fetch.garmin import DataType, GarminConnector, GarminConnectorError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    path = uri[len("gs://"):]
    bucket, prefix = path.split("/", 1)
    return bucket, prefix


def _marker_blob(bucket_name: str, prefix: str, today: str) -> storage.Blob:
    client = storage.Client()
    return client.bucket(bucket_name).blob(f"{prefix.rstrip('/')}/{today}.triggered")


def _marker_exists(bucket_name: str, prefix: str, today: str) -> bool:
    return _marker_blob(bucket_name, prefix, today).exists()


def _write_marker(bucket_name: str, prefix: str, today: str) -> None:
    _marker_blob(bucket_name, prefix, today).upload_from_string(
        b"", content_type="text/plain"
    )
    logger.info(f"Marker written: gs://{bucket_name}/{prefix.rstrip('/')}/{today}.triggered")


def _trigger_workflow(project: str, location: str, workflow: str) -> None:
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(google.auth.transport.requests.Request())

    url = (
        f"https://workflowexecutions.googleapis.com/v1"
        f"/projects/{project}/locations/{location}/workflows/{workflow}/executions"
    )
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {creds.token}"},
        json={},
        timeout=30,
    )
    resp.raise_for_status()
    logger.info(f"Workflow triggered: {resp.json().get('name', 'unknown')}")


def main() -> None:
    watcher_bucket = os.environ.get("WATCHER_BUCKET", "")
    workflow_project = os.environ.get("WORKFLOW_PROJECT", "PLACEHOLDER_PROJECT")
    workflow_location = os.environ.get("WORKFLOW_LOCATION", "PLACEHOLDER_LOCATION")
    workflow_name = os.environ.get("WORKFLOW_NAME", "PLACEHOLDER_WORKFLOW")

    if not watcher_bucket:
        logger.error("WATCHER_BUCKET is required (e.g. gs://ela-source-dev/garmin/watcher)")
        sys.exit(1)

    bucket_name, prefix = _parse_gcs_uri(watcher_bucket)
    today = date.today().isoformat()

    if _marker_exists(bucket_name, prefix, today):
        logger.info(f"Workflow already triggered today ({today}). Nothing to do.")
        sys.exit(0)

    try:
        connector = GarminConnector.from_env()
        connector.authenticate()
    except GarminConnectorError as e:
        logger.error(f"Garmin auth failed: {e}")
        sys.exit(1)

    records = connector.fetch_data(DataType.SLEEP, days=0)

    if not records:
        logger.info(f"No sleep data for {today} yet. Will retry on next schedule.")
        sys.exit(0)

    logger.info(f"Sleep data found for {today} ({len(records)} record(s)). Triggering workflow...")
    _trigger_workflow(workflow_project, workflow_location, workflow_name)
    _write_marker(bucket_name, prefix, today)


if __name__ == "__main__":
    main()
