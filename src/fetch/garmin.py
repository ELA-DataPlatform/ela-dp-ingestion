"""
Garmin Data Fetcher
-------------------
Fetches various types of Garmin Connect user data via the garminconnect library.

Supported data types (29):
 - activities, activity_details, activity_splits, activity_weather,
   activity_hr_zones, activity_exercise_sets
 - sleep, steps, heart_rate, body_battery, stress, weight, body_composition
 - user_summary, stats_and_body, training_readiness, rhr_daily, spo2,
   respiration, intensity_minutes, max_metrics, all_day_events
 - device_info, training_status, hrv, race_predictions, floors,
   endurance_score, hill_score
"""

import io
import json
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from garminconnect import Garmin

logger = logging.getLogger(__name__)

DEFAULT_DAYS = 3


class DataType(Enum):
    """Supported Garmin data types."""

    ACTIVITIES = "activities"
    ACTIVITY_DETAILS = "activity_details"
    ACTIVITY_SPLITS = "activity_splits"
    ACTIVITY_WEATHER = "activity_weather"
    ACTIVITY_HR_ZONES = "activity_hr_zones"
    ACTIVITY_EXERCISE_SETS = "activity_exercise_sets"
    SLEEP = "sleep"
    STEPS = "steps"
    HEART_RATE = "heart_rate"
    BODY_BATTERY = "body_battery"
    STRESS = "stress"
    WEIGHT = "weight"
    BODY_COMPOSITION = "body_composition"
    USER_SUMMARY = "user_summary"
    STATS_AND_BODY = "stats_and_body"
    TRAINING_READINESS = "training_readiness"
    RHR_DAILY = "rhr_daily"
    SPO2 = "spo2"
    RESPIRATION = "respiration"
    INTENSITY_MINUTES = "intensity_minutes"
    MAX_METRICS = "max_metrics"
    ALL_DAY_EVENTS = "all_day_events"
    DEVICE_INFO = "device_info"
    TRAINING_STATUS = "training_status"
    HRV = "hrv"
    RACE_PREDICTIONS = "race_predictions"
    FLOORS = "floors"
    ENDURANCE_SCORE = "endurance_score"
    HILL_SCORE = "hill_score"


METRICS_CONFIG = {
    "sleep":              {"method": "get_sleep_data",             "type": "daily"},
    "steps":              {"method": "get_steps_data",             "type": "daily"},
    "heart_rate":         {"method": "get_heart_rates",            "type": "daily"},
    "stress":             {"method": "get_all_day_stress",         "type": "daily"},
    "user_summary":       {"method": "get_user_summary",           "type": "daily"},
    "stats_and_body":     {"method": "get_stats_and_body",         "type": "daily"},
    "training_readiness": {"method": "get_training_readiness",     "type": "daily"},
    "rhr_daily":          {"method": "get_rhr_day",                "type": "daily"},
    "spo2":               {"method": "get_spo2_data",              "type": "daily"},
    "respiration":        {"method": "get_respiration_data",       "type": "daily"},
    "intensity_minutes":  {"method": "get_intensity_minutes_data", "type": "daily"},
    "max_metrics":        {"method": "get_max_metrics",            "type": "daily"},
    "all_day_events":     {"method": "get_all_day_events",         "type": "daily"},
    "training_status":    {"method": "get_training_status",        "type": "daily"},
    "hrv":                {"method": "get_hrv_data",               "type": "daily"},
    "floors":             {"method": "get_floors",                 "type": "daily"},
    "activities":             {"method": "get_activities_by_date",      "type": "range"},
    "body_battery":           {"method": "get_body_battery",            "type": "range", "chunk_days": 28},
    "weight":                 {"method": "get_weigh_ins",               "type": "range"},
    "body_composition":       {"method": "get_body_composition",        "type": "range"},
    "endurance_score":        {"method": "get_endurance_score",         "type": "range", "chunk_days": 28},
    "hill_score":             {"method": "get_hill_score",              "type": "range", "chunk_days": 28},
    "device_info":            {"method": "get_devices",                 "type": "simple"},
    "race_predictions":       {"method": "get_race_predictions",        "type": "simple"},
    "activity_details":       {"method": "get_activity_details",        "type": "activity_detail"},
    "activity_splits":        {"method": "get_activity_splits",         "type": "activity_subdata"},
    "activity_weather":       {"method": "get_activity_weather",        "type": "activity_subdata"},
    "activity_hr_zones":      {"method": "get_activity_hr_in_timezones", "type": "activity_subdata"},
    "activity_exercise_sets": {"method": "get_activity_exercise_sets",  "type": "activity_subdata"},
}


class GarminConnectorError(Exception):
    pass


class GarminConnector:
    """Garmin Connect data connector with support for multiple data types."""

    def __init__(self, username: str, password: str, tokenstore_gcs: str = ""):
        self.username = username
        self.password = password
        self.tokenstore_gcs = tokenstore_gcs
        self._client: Optional[Garmin] = None

    @classmethod
    def from_env(cls) -> "GarminConnector":
        """Create a GarminConnector from environment variables."""
        missing = [k for k in ("GARMIN_USERNAME", "GARMIN_PASSWORD") if not os.getenv(k)]
        if missing:
            raise GarminConnectorError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
        return cls(
            username=os.environ["GARMIN_USERNAME"],
            password=os.environ["GARMIN_PASSWORD"],
            tokenstore_gcs=os.getenv("GARMIN_TOKENSTORE_GCS", ""),
        )

    _TOKEN_FILES = ["oauth1_token.json", "oauth2_token.json"]

    def _download_token_from_gcs(self, local_dir: str) -> bool:
        """Download garth token files from GCS into a local directory."""
        if not self.tokenstore_gcs:
            return False
        try:
            from google.cloud import storage
            gcs_path = self.tokenstore_gcs[len("gs://"):]
            bucket_name, prefix = gcs_path.split("/", 1)
            gcs_prefix = prefix.rstrip("/")
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            found = False
            for filename in self._TOKEN_FILES:
                blob = bucket.blob(f"{gcs_prefix}/{filename}")
                if blob.exists():
                    blob.download_to_filename(os.path.join(local_dir, filename))
                    logger.info(f"Downloaded gs://{bucket_name}/{gcs_prefix}/{filename}")
                    found = True
            return found
        except Exception as e:
            logger.warning(f"Failed to download tokens from GCS: {e}")
            return False

    def _upload_token_to_gcs(self, local_dir: str) -> None:
        """Upload garth token files from a local directory to GCS."""
        if not self.tokenstore_gcs:
            return
        try:
            from google.cloud import storage
            gcs_path = self.tokenstore_gcs[len("gs://"):]
            bucket_name, prefix = gcs_path.split("/", 1)
            gcs_prefix = prefix.rstrip("/")
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            for filename in self._TOKEN_FILES:
                local_path = os.path.join(local_dir, filename)
                if os.path.exists(local_path):
                    blob = bucket.blob(f"{gcs_prefix}/{filename}")
                    blob.upload_from_filename(local_path)
                    logger.info(f"Uploaded gs://{bucket_name}/{gcs_prefix}/{filename}")
        except Exception as e:
            logger.warning(f"Failed to upload tokens to GCS: {e}")

    def authenticate(self, data_types: List[DataType]) -> None:
        """Authenticate to Garmin Connect with token caching on GCS.

        1. Try to load cached tokens from GCS (no SSO login needed)
        2. If no cache or expired, fall back to username/password login
        3. Save tokens to GCS after successful login
        """
        with tempfile.TemporaryDirectory() as token_dir:
            self._client = Garmin(self.username, self.password)

            # Try cached tokens first
            if self._download_token_from_gcs(token_dir):
                try:
                    self._client.login(tokenstore=token_dir)
                    logger.info("Authenticated via cached tokens")
                    self._client.garth.dump(token_dir)
                    self._upload_token_to_gcs(token_dir)
                    return
                except Exception as e:
                    logger.warning(f"Cached tokens invalid, falling back to login: {e}")

            # Fall back to username/password
            try:
                self._client.login()
                logger.info("Authenticated via username/password")
                self._client.garth.dump(token_dir)
                self._upload_token_to_gcs(token_dir)
            except Exception as e:
                raise GarminConnectorError(f"Authentication failed: {e}") from e

    def _call_with_retry(self, fn: Callable, *args, max_retries: int = 3, **kwargs) -> Any:
        """Call a Garmin API function with retry and exponential backoff on 429 errors."""
        for attempt in range(max_retries + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                if "429" in str(e) and attempt < max_retries:
                    wait = 2 ** (attempt + 1)  # 2s, 4s, 8s
                    logger.warning(f"429 Too Many Requests, retry {attempt + 1}/{max_retries} in {wait}s...")
                    time.sleep(wait)
                else:
                    raise

    @property
    def client(self) -> Garmin:
        if self._client is None:
            raise GarminConnectorError("Not authenticated. Call authenticate() first.")
        return self._client

    def fetch_data(self, data_type: DataType, **kwargs) -> List[Dict[str, Any]]:
        """Dispatch fetch by DataType using METRICS_CONFIG."""
        metric_name = data_type.value
        config = METRICS_CONFIG.get(metric_name)
        if not config:
            raise GarminConnectorError(f"Unsupported data type: {data_type}")

        days = kwargs.get("days", DEFAULT_DAYS)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        method_name = config["method"]
        fetch_type = config["type"]

        if not hasattr(self.client, method_name):
            raise GarminConnectorError(f"Client missing method: {method_name}")

        method = getattr(self.client, method_name)

        logger.info(f"Fetching {metric_name} ({fetch_type}, {days} days)...")

        if fetch_type == "daily":
            return self._fetch_daily(method, metric_name, start_date, end_date)
        elif fetch_type == "range":
            chunk_days = config.get("chunk_days", 364)
            return self._fetch_range(method, metric_name, start_date, end_date, chunk_days)
        elif fetch_type == "simple":
            return self._fetch_simple(method, metric_name)
        elif fetch_type == "activity_detail":
            return self._fetch_activity_details(self.client, start_date, end_date)
        elif fetch_type == "activity_subdata":
            return self._fetch_activity_subdata(self.client, metric_name, method_name, start_date, end_date)
        else:
            raise GarminConnectorError(f"Unknown fetch type: {fetch_type}")

    # -------------------------------------------------------------------------
    # Fetch strategies (ported from old/connectors/garmin/fetcher.py)
    # -------------------------------------------------------------------------

    def _fetch_daily(
        self, method: Callable, metric_name: str, start: datetime, end: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch data day by day."""
        results = []
        current = start

        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            try:
                data = self._call_with_retry(method, date_str)
                if data:
                    data = flatten_nested_arrays(data, path=f"{metric_name}.{date_str}")

                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                item["date"] = date_str
                                item["data_type"] = metric_name
                                results.append(item)
                    elif isinstance(data, dict):
                        data["date"] = date_str
                        data["data_type"] = metric_name
                        results.append(data)
                    else:
                        results.append({
                            "date": date_str,
                            "data": data,
                            "data_type": metric_name,
                        })

                time.sleep(0.3)
            except Exception as e:
                logger.warning(f"Error fetching {metric_name} for {date_str}: {e}")

            current += timedelta(days=1)

        logger.info(f"Fetched {metric_name}: {len(results)} entries")
        return results

    def _fetch_range(
        self,
        method: Callable,
        metric_name: str,
        start: datetime,
        end: datetime,
        chunk_days: int = 364,
    ) -> List[Dict[str, Any]]:
        """Fetch data using a date range, chunking if necessary."""
        try:
            results = []
            current_start = start

            while current_start <= end:
                current_end = min(current_start + timedelta(days=chunk_days), end)
                start_str = current_start.strftime("%Y-%m-%d")
                end_str = current_end.strftime("%Y-%m-%d")

                logger.info(f"  Fetching chunk: {start_str} to {end_str}")

                chunk_data = None
                try:
                    chunk_data = self._call_with_retry(method, start_str, end_str)
                except TypeError:
                    logger.debug(f"Method {method.__name__} rejected range args, trying without")
                    chunk_data = self._call_with_retry(method)
                    current_start = end + timedelta(days=1)

                if chunk_data:
                    # Special handling for weight
                    if metric_name == "weight" and isinstance(chunk_data, dict):
                        if "dailyWeightSummaries" in chunk_data and isinstance(chunk_data["dailyWeightSummaries"], list):
                            all_weight_entries = []
                            for daily_summary in chunk_data["dailyWeightSummaries"]:
                                if isinstance(daily_summary, dict) and "allWeightMetrics" in daily_summary:
                                    summary_date = daily_summary.get("summaryDate")
                                    for entry in daily_summary["allWeightMetrics"]:
                                        if isinstance(entry, dict):
                                            if summary_date and "summaryDate" not in entry:
                                                entry["summaryDate"] = summary_date
                                            all_weight_entries.append(entry)
                            chunk_data = all_weight_entries
                            logger.info(f"Flattened weight data: {len(chunk_data)} entries")
                        elif "allWeightMetrics" in chunk_data:
                            summary_date = chunk_data.get("summaryDate")
                            chunk_data = chunk_data["allWeightMetrics"]
                            if summary_date and isinstance(chunk_data, list):
                                for entry in chunk_data:
                                    if isinstance(entry, dict) and "summaryDate" not in entry:
                                        entry["summaryDate"] = summary_date
                            logger.info(f"Flattened weight data: {len(chunk_data)} entries")

                    # Special handling for body_composition
                    elif metric_name == "body_composition" and isinstance(chunk_data, dict) and "dateWeightList" in chunk_data:
                        chunk_data = chunk_data["dateWeightList"]
                        logger.info(f"Flattened body_composition data: {len(chunk_data)} entries")
                    else:
                        chunk_data = flatten_nested_arrays(chunk_data, path=metric_name)

                    if isinstance(chunk_data, list):
                        for item in chunk_data:
                            if isinstance(item, dict):
                                item["data_type"] = metric_name
                            results.append(item)
                    elif isinstance(chunk_data, dict):
                        chunk_data["data_type"] = metric_name
                        results.append(chunk_data)
                    else:
                        results.append({"data": chunk_data, "data_type": metric_name})

                current_start = current_end + timedelta(days=1)
                if current_start <= end:
                    time.sleep(1)

            logger.info(f"Fetched {metric_name}: {len(results)} items")
            return results

        except Exception as e:
            logger.error(f"Error fetching {metric_name} (range): {e}")
            return []

    def _fetch_simple(self, method: Callable, metric_name: str) -> List[Dict[str, Any]]:
        """Fetch data without parameters."""
        try:
            data = self._call_with_retry(method)
            if not data:
                return []

            data = flatten_nested_arrays(data, path=metric_name)

            results = []
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        item["data_type"] = metric_name
                    results.append(item)
            elif isinstance(data, dict):
                data["data_type"] = metric_name
                results.append(data)
            else:
                results.append({"data": data, "data_type": metric_name})

            logger.info(f"Fetched {metric_name}: {len(results)} items")
            return results
        except Exception as e:
            logger.error(f"Error fetching {metric_name} (simple): {e}")
            return []

    def _fetch_activity_details(
        self, client: Garmin, start: datetime, end: datetime
    ) -> List[Dict[str, Any]]:
        """Special handling for activity details (requires 2 steps)."""
        try:
            activities = self._call_with_retry(
                client.get_activities_by_date,
                start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
            )

            results = []
            for activity in activities:
                activity_id = activity.get("activityId")
                if not activity_id:
                    continue

                try:
                    details = self._call_with_retry(client.get_activity_details, activity_id, maxchart=2000, maxpoly=4000)

                    clean_activity = flatten_nested_arrays(activity, path=f"activity_{activity_id}")
                    clean_details = flatten_nested_arrays(details, path=f"details_{activity_id}")

                    enriched = {
                        **clean_activity,
                        "detailed_data": clean_details,
                        "data_type": "activity_details",
                    }
                    results.append(enriched)
                    time.sleep(0.5)
                except Exception as e:
                    logger.warning(f"Failed details for {activity_id}: {e}")

            logger.info(f"Fetched details for {len(results)} activities")
            return results
        except Exception as e:
            logger.error(f"Error fetching activity details: {e}")
            return []

    def _fetch_activity_subdata(
        self,
        client: Garmin,
        metric_name: str,
        method_name: str,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        """Generic fetcher for activity-related subdata (splits, weather, etc)."""
        try:
            activities = self._call_with_retry(
                client.get_activities_by_date,
                start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
            )

            results = []
            method = getattr(client, method_name)

            for activity in activities:
                activity_id = activity.get("activityId")
                if not activity_id:
                    continue

                try:
                    if metric_name == "activity_splits":
                        splits = self._call_with_retry(client.get_activity_splits, activity_id)
                        typed_splits = self._call_with_retry(client.get_activity_typed_splits, activity_id)
                        split_summaries = self._call_with_retry(client.get_activity_split_summaries, activity_id)

                        clean_splits = flatten_nested_arrays(splits, path=f"splits_{activity_id}")
                        clean_typed = flatten_nested_arrays(typed_splits, path=f"typed_splits_{activity_id}")
                        clean_summaries = flatten_nested_arrays(split_summaries, path=f"summaries_{activity_id}")

                        data = {
                            "activityId": activity_id,
                            "activityName": activity.get("activityName", ""),
                            "activityType": activity.get("activityType", ""),
                            "startTimeLocal": activity.get("startTimeLocal", ""),
                            "splits": clean_splits,
                            "typed_splits": clean_typed,
                            "split_summaries": clean_summaries,
                            "data_type": metric_name,
                        }
                    else:
                        subdata = self._call_with_retry(method, activity_id)
                        if not subdata:
                            continue

                        clean_subdata = flatten_nested_arrays(subdata, path=f"{metric_name}_{activity_id}")

                        data = {
                            "activityId": activity_id,
                            "activityName": activity.get("activityName", ""),
                            "activityType": activity.get("activityType", ""),
                            "startTimeLocal": activity.get("startTimeLocal", ""),
                            "data_type": metric_name,
                        }
                        # Key naming matches original script
                        if metric_name == "activity_weather":
                            data["weather_data"] = clean_subdata
                        elif metric_name == "activity_hr_zones":
                            data["hr_zones_data"] = clean_subdata
                        elif metric_name == "activity_exercise_sets":
                            data["exercise_sets_data"] = clean_subdata
                        else:
                            data[f"{metric_name}_data"] = clean_subdata

                    results.append(data)
                    time.sleep(0.3)
                except Exception as e:
                    logger.warning(f"Failed {metric_name} for {activity_id}: {e}")

            logger.info(f"Fetched {metric_name} for {len(results)} activities")
            return results
        except Exception as e:
            logger.error(f"Error fetching {metric_name}: {e}")
            return []


# -----------------------------------------------------------------------------
# flatten_nested_arrays — ported from old/connectors/garmin/utils.py
# -----------------------------------------------------------------------------

def flatten_nested_arrays(
    obj: Any,
    known_mappings: Dict[str, Any] = None,
    path: str = "",
) -> Any:
    """
    Recursively transform nested arrays for BigQuery compatibility.

    BigQuery does not support nested arrays ([[a,b]]).
    This function transforms them into arrays of objects ([{x:a, y:b}]).
    """
    if known_mappings is None:
        known_mappings = {
            "stressValuesArray": ["timestamp", "type", "value", "score"],
            "respirationAveragesValuesArray": ["timestamp", "average", "high", "low"],
            "floorValuesArray": ["start_time", "end_time", "ascended", "descended"],
            "spO2SingleValues": ["timestamp", "value", "type"],
            "bodyBatteryValuesArray": {
                2: ["timestamp", "value"],
                4: ["timestamp", "type", "value", "score"],
            },
        }

    # Case 1: Dict — recurse on each key
    if isinstance(obj, dict):
        # Special handling for Garmin activity details metrics
        if "metricDescriptors" in obj and "activityDetailMetrics" in obj:
            try:
                descriptors = obj["metricDescriptors"]
                metrics_list = obj["activityDetailMetrics"]

                index_map = {
                    d["metricsIndex"]: d["key"]
                    for d in descriptors
                    if "metricsIndex" in d and "key" in d
                }

                new_metrics = []
                for item in metrics_list:
                    if not isinstance(item, dict) or "metrics" not in item:
                        continue

                    raw_values = item["metrics"]
                    if not isinstance(raw_values, list):
                        continue

                    structured_metric = {}
                    for i, value in enumerate(raw_values):
                        if value is not None and i in index_map:
                            structured_metric[index_map[i]] = value

                    new_metrics.append(structured_metric)

                obj["activityDetailMetrics"] = new_metrics
                logger.debug(f"Transformed activityDetailMetrics at '{path}' using descriptors")
            except Exception as e:
                logger.warning(f"Failed to transform activityDetailMetrics at '{path}': {e}")

        result = {}
        for key, value in obj.items():
            # Skip None values to avoid BigQuery REQUIRED field errors
            if value is None:
                continue

            # Replace empty dicts — BigQuery auto-detection can't handle empty structs
            if isinstance(value, dict) and not value:
                result[key] = None
                continue

            # Check known mappings for nested arrays
            if key in known_mappings and isinstance(value, list) and value and isinstance(value[0], list):
                mapping = known_mappings[key]
                field_names = None

                item_len = len(value[0])
                if isinstance(mapping, dict):
                    field_names = mapping.get(item_len)
                elif isinstance(mapping, list):
                    field_names = mapping

                if field_names:
                    result[key] = [
                        dict(zip(field_names, item[: len(field_names)]))
                        for item in value
                    ]
                    logger.debug(f"Transformed nested array at '{path}.{key}' using mapping: {field_names}")
                else:
                    result[key] = flatten_nested_arrays(value, known_mappings, f"{path}.{key}")
            else:
                result[key] = flatten_nested_arrays(value, known_mappings, f"{path}.{key}")
        return result

    # Case 2: List — check for nested arrays
    elif isinstance(obj, list):
        if not obj:
            return obj

        # Nested array detected: [[...], [...]]
        if isinstance(obj[0], list):
            first_item_length = len(obj[0])

            # 2-element arrays → {timestamp, value}
            if first_item_length == 2:
                result = [{"timestamp": item[0], "value": item[1]} for item in obj]
                logger.debug(f"Transformed generic 2-element nested array at '{path}'")
                return result

            # >2 elements without mapping → generic keys
            else:
                logger.warning(
                    f"Nested array with {first_item_length} elements found at '{path}' "
                    f"without explicit mapping. Using generic keys: val_0, val_1, ..."
                )
                return [
                    {f"val_{i}": val for i, val in enumerate(item)}
                    for item in obj
                ]

        # Not a nested array — recurse on each element
        else:
            return [flatten_nested_arrays(item, known_mappings, f"{path}[{i}]") for i, item in enumerate(obj)]

    # Case 3: Primitive — return as-is
    else:
        return obj
