"""
Garmin Data Fetcher
-------------------
Fetches Garmin Connect data using OAuth DI tokens (mobile SSO flow).

Authentication: tokens stored in GCS (GARMIN_TOKENSTORE_GCS).
Bootstrap: run scripts/garmin_bootstrap_tokens.py once to create initial tokens.
Cloud Run: loads tokens from GCS, auto-refreshes, re-uploads after each job.

Supported data types (29):
 - activities, activity_details, activity_splits, activity_weather,
   activity_hr_zones, activity_exercise_sets
 - sleep, steps, heart_rate, body_battery, stress, weight, body_composition
 - user_summary, stats_and_body, training_readiness, rhr_daily, spo2,
   respiration, intensity_minutes, max_metrics, all_day_events
 - device_info, training_status, hrv, race_predictions, floors,
   endurance_score, hill_score
"""

import logging
import os
import time
from datetime import date, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from garminconnect import (
    Garmin,
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
)

logger = logging.getLogger(__name__)

GQL_PATH = "graphql-gateway/graphql"
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


class GarminConnectorError(Exception):
    pass


class GarminConnector:
    """
    Garmin Connect data connector.

    Loads OAuth DI tokens from GCS, authenticates via garminconnect library
    (mobile SSO — no browser required), and calls Garmin Connect REST and
    GraphQL endpoints. Re-uploads tokens to GCS after each fetch to persist
    any auto-refresh.

    Bootstrap: run scripts/garmin_bootstrap_tokens.py once from a machine
    with credentials to create the initial token file in GCS.
    """

    TOKEN_FILE = "garmin_tokens.json"
    TMP_TOKEN_DIR = "/tmp/garmin_tokens"

    def __init__(self, username: str, password: str, tokenstore_gcs: str = ""):
        self.username = username
        self.password = password
        self.tokenstore_gcs = tokenstore_gcs
        self._garmin: Optional[Garmin] = None
        self._display_name: Optional[str] = None

    @classmethod
    def from_env(cls) -> "GarminConnector":
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

    # -------------------------------------------------------------------------
    # Token storage helpers
    # -------------------------------------------------------------------------

    def _download_tokens_from_gcs(self) -> str:
        """Download garmin_tokens.json from GCS and return its content."""
        if not self.tokenstore_gcs:
            raise GarminConnectorError(
                "GARMIN_TOKENSTORE_GCS not set. "
                "Run scripts/garmin_bootstrap_tokens.py first."
            )
        try:
            from google.cloud import storage

            gcs_path = self.tokenstore_gcs[len("gs://"):]
            bucket_name, prefix = gcs_path.split("/", 1)
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(f"{prefix.rstrip('/')}/{self.TOKEN_FILE}")
            if not blob.exists():
                raise GarminConnectorError(
                    f"Token file not found in GCS: {self.TOKEN_FILE}. "
                    "Run scripts/garmin_bootstrap_tokens.py first."
                )
            return blob.download_as_text()
        except GarminConnectorError:
            raise
        except Exception as e:
            raise GarminConnectorError(
                f"Failed to download tokens from GCS: {e}"
            ) from e

    def _upload_tokens_to_gcs(self, token_json: str) -> None:
        """Upload token JSON string back to GCS."""
        if not self.tokenstore_gcs:
            return
        try:
            from google.cloud import storage

            gcs_path = self.tokenstore_gcs[len("gs://"):]
            bucket_name, prefix = gcs_path.split("/", 1)
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(f"{prefix.rstrip('/')}/{self.TOKEN_FILE}")
            blob.upload_from_string(token_json, content_type="application/json")
            logger.info("Tokens re-uploaded to GCS")
        except Exception as e:
            logger.warning(f"Failed to upload tokens to GCS: {e}")

    # -------------------------------------------------------------------------
    # Authentication
    # -------------------------------------------------------------------------

    def authenticate(self, data_types=None) -> None:
        """
        Load OAuth tokens from GCS and authenticate via garminconnect.

        Writes tokens to a temp directory, calls garmin.login() which loads
        them and fetches the user profile (display_name). Cleans up the temp
        file afterward.
        """
        token_json = self._download_tokens_from_gcs()

        tmp_dir = Path(self.TMP_TOKEN_DIR)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        token_file = tmp_dir / self.TOKEN_FILE
        token_file.write_text(token_json)

        try:
            garmin = Garmin()
            garmin.login(str(tmp_dir))
        except GarminConnectAuthenticationError as e:
            raise GarminConnectorError(
                "Garmin authentication failed — tokens may be expired or revoked. "
                f"Re-run scripts/garmin_bootstrap_tokens.py. ({e})"
            ) from e
        except GarminConnectConnectionError as e:
            raise GarminConnectorError(
                f"Garmin connection error during auth: {e}"
            ) from e
        finally:
            token_file.unlink(missing_ok=True)

        self._garmin = garmin
        self._display_name = garmin.display_name
        logger.info(f"Authenticated as {self._display_name}")

    def save_tokens(self) -> None:
        """Re-upload tokens to GCS (persists any auto-refresh that occurred)."""
        if self._garmin is None:
            return
        self._upload_tokens_to_gcs(self._garmin.client.dumps())

    # -------------------------------------------------------------------------
    # HTTP helpers
    # -------------------------------------------------------------------------

    def _get(self, path: str, retries: int = 3) -> Optional[Any]:
        """GET request via garminconnect (connectapi.garmin.com)."""
        for attempt in range(retries):
            try:
                return self._garmin.client.connectapi(path)
            except GarminConnectAuthenticationError as e:
                raise GarminConnectorError(
                    f"Auth error — re-run bootstrap script. ({e})"
                ) from e
            except GarminConnectConnectionError as e:
                if "429" in str(e):
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"429 on {path}, retrying in {wait}s")
                    time.sleep(wait)
                    continue
                logger.warning(f"GET {path} error: {e}")
                return None
            except Exception as e:
                logger.warning(f"GET {path} unexpected error: {e}")
                return None
        return None

    def _gql(self, query: str, retries: int = 3) -> Optional[Any]:
        """GraphQL POST via garminconnect (connectapi.garmin.com)."""
        for attempt in range(retries):
            try:
                data = self._garmin.client.post(
                    "", GQL_PATH, json={"query": query}, api=True
                )
                if isinstance(data, dict) and "data" in data:
                    inner = data["data"]
                    if isinstance(inner, dict) and len(inner) == 1:
                        return list(inner.values())[0]
                    return inner
                return data
            except GarminConnectAuthenticationError as e:
                raise GarminConnectorError(
                    f"Auth error — re-run bootstrap script. ({e})"
                ) from e
            except GarminConnectConnectionError as e:
                if "429" in str(e):
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"429 on GQL, retrying in {wait}s")
                    time.sleep(wait)
                    continue
                logger.warning(f"GQL error: {e}")
                return None
            except Exception as e:
                logger.warning(f"GQL unexpected error: {e}")
                return None
        return None

    # -------------------------------------------------------------------------
    # Normalization
    # -------------------------------------------------------------------------

    def _to_list(
        self, data: Any, metric: str, date_str: str = None
    ) -> List[Dict]:
        """Normalize any API response to list[dict] with data_type (+ date)."""
        if data is None:
            return []
        items = data if isinstance(data, list) else [data]
        results = []
        for item in items:
            if not isinstance(item, dict):
                item = {"value": item}
            item["data_type"] = metric
            if date_str:
                item.setdefault("date", date_str)
            results.append(item)
        return results

    # -------------------------------------------------------------------------
    # Fetch strategies
    # -------------------------------------------------------------------------

    def _daily_rest(self, url_fn, metric: str, start: date, end: date) -> List[Dict]:
        """Call url_fn(date_str) once per day."""
        results = []
        d = start
        while d <= end:
            ds = d.isoformat()
            data = self._get(url_fn(ds))
            results.extend(self._to_list(data, metric, ds))
            time.sleep(0.3)
            d += timedelta(days=1)
        logger.info(f"Fetched {metric}: {len(results)} entries")
        return results

    def _daily_gql(self, query_fn, metric: str, start: date, end: date) -> List[Dict]:
        """Call query_fn(date_str) as GraphQL once per day."""
        results = []
        d = start
        while d <= end:
            ds = d.isoformat()
            data = self._gql(query_fn(ds))
            results.extend(self._to_list(data, metric, ds))
            time.sleep(0.3)
            d += timedelta(days=1)
        logger.info(f"Fetched {metric}: {len(results)} entries")
        return results

    def _range_rest(self, path: str, metric: str) -> List[Dict]:
        data = self._get(path)
        result = self._to_list(data, metric)
        logger.info(f"Fetched {metric}: {len(result)} entries")
        return result

    def _range_gql(self, query: str, metric: str) -> List[Dict]:
        data = self._gql(query)
        result = self._to_list(data, metric)
        logger.info(f"Fetched {metric}: {len(result)} entries")
        return result

    def _simple_rest(self, path: str, metric: str) -> List[Dict]:
        data = self._get(path)
        result = self._to_list(data, metric)
        logger.info(f"Fetched {metric}: {len(result)} entries")
        return result

    def _fetch_activities(
        self, start_str: str, end_str: str, metric: str
    ) -> List[Dict]:
        """Fetch activities with pagination."""
        results = []
        offset = 0
        while True:
            path = (
                f"activitylist-service/activities/search/activities"
                f"?limit=100&start={offset}"
                f"&startDate={start_str}&endDate={end_str}"
            )
            data = self._get(path)
            if not data or not isinstance(data, list):
                break
            for item in data:
                if isinstance(item, dict):
                    item["data_type"] = metric
                    results.append(item)
            if len(data) < 100:
                break
            offset += 100
            time.sleep(0.5)
        logger.info(f"Fetched {metric}: {len(results)} activities")
        return results

    def _fetch_per_activity(
        self, endpoint_key: str, metric: str, start: date, end: date
    ) -> List[Dict]:
        """Fetch per-activity sub-data (splits, weather, HR zones, etc.)."""
        activities = self._fetch_activities(
            start.isoformat(), end.isoformat(), "activities"
        )

        url_fns = {
            "activity_details": lambda aid: f"activity-service/activity/{aid}",
            "activity_splits": lambda aid: f"activity-service/activity/{aid}/splits",
            "activity_weather": lambda aid: f"activity-service/activity/{aid}/weather",
            "activity_hr_zones": lambda aid: f"activity-service/activity/{aid}/hrTimeInZones",
            "activity_exercise_sets": lambda aid: f"activity-service/activity/{aid}/exerciseSets",
        }
        url_fn = url_fns[endpoint_key]

        results = []
        for activity in activities:
            aid = activity.get("activityId")
            if not aid:
                continue
            data = self._get(url_fn(aid))
            if not data:
                continue
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict):
                    item.setdefault("activityId", aid)
                    item["data_type"] = metric
                    results.append(item)
            time.sleep(0.3)

        logger.info(
            f"Fetched {metric}: {len(results)} entries "
            f"from {len(activities)} activities"
        )
        return results

    # -------------------------------------------------------------------------
    # Main dispatch
    # -------------------------------------------------------------------------

    def fetch_data(self, data_type: DataType, **kwargs) -> List[Dict[str, Any]]:
        """Dispatch fetch by DataType. Saves tokens to GCS after completion."""
        days = kwargs.get("days", DEFAULT_DAYS)
        end = date.today()
        start = end - timedelta(days=days)
        s = start.isoformat()
        e = end.isoformat()
        dn = self._display_name
        metric = data_type.value

        logger.info(f"Fetching {metric} ({days} days, {s} → {e})...")

        if data_type == DataType.ACTIVITIES:
            result = self._fetch_activities(s, e, metric)

        elif data_type == DataType.SLEEP:
            result = self._daily_rest(
                lambda d: f"wellness-service/wellness/dailySleepData/{dn}?date={d}",
                metric, start, end,
            )

        elif data_type == DataType.STEPS:
            result = self._daily_rest(
                lambda d: f"usersummary-service/stats/steps/daily/{d}/{d}",
                metric, start, end,
            )

        elif data_type == DataType.HEART_RATE:
            result = self._daily_rest(
                lambda d: f"wellness-service/wellness/dailyHeartRate/{dn}?date={d}",
                metric, start, end,
            )

        elif data_type == DataType.STRESS:
            result = self._daily_rest(
                lambda d: f"wellness-service/wellness/dailyStress/{d}",
                metric, start, end,
            )

        elif data_type == DataType.USER_SUMMARY:
            result = self._daily_rest(
                lambda d: f"usersummary-service/usersummary/daily/{dn}?calendarDate={d}",
                metric, start, end,
            )

        elif data_type == DataType.STATS_AND_BODY:
            result = self._daily_rest(
                lambda d: f"usersummary-service/stats/daily/{d}/{d}",
                metric, start, end,
            )

        elif data_type == DataType.RHR_DAILY:
            result = self._daily_rest(
                lambda d: f"wellness-service/wellness/dailyHeartRate/{dn}?date={d}",
                metric, start, end,
            )

        elif data_type == DataType.SPO2:
            result = self._daily_rest(
                lambda d: f"wellness-service/wellness/dailySpo2/{d}",
                metric, start, end,
            )

        elif data_type == DataType.RESPIRATION:
            result = self._daily_rest(
                lambda d: f"wellness-service/wellness/daily/respiration/{d}",
                metric, start, end,
            )

        elif data_type == DataType.INTENSITY_MINUTES:
            result = self._daily_rest(
                lambda d: f"wellness-service/wellness/daily/im/{d}",
                metric, start, end,
            )

        elif data_type == DataType.MAX_METRICS:
            result = self._range_rest(
                f"metrics-service/metrics/maxmet/daily/{s}/{e}",
                metric,
            )

        elif data_type == DataType.ALL_DAY_EVENTS:
            result = self._daily_gql(
                lambda d: f'query{{dailyEventsScalar(date:"{d}")}}',
                metric, start, end,
            )

        elif data_type == DataType.TRAINING_STATUS:
            result = self._daily_gql(
                lambda d: f'query{{trainingStatusDailyScalar(calendarDate:"{d}")}}',
                metric, start, end,
            )

        elif data_type == DataType.TRAINING_READINESS:
            result = self._range_gql(
                f'query{{trainingReadinessRangeScalar(startDate:"{s}", endDate:"{e}")}}',
                metric,
            )

        elif data_type == DataType.HRV:
            result = self._range_gql(
                f'query{{heartRateVariabilityScalar(startDate:"{s}", endDate:"{e}")}}',
                metric,
            )

        elif data_type == DataType.FLOORS:
            result = self._daily_rest(
                lambda d: f"wellness-service/wellness/floorsChartData/daily/{d}",
                metric, start, end,
            )

        elif data_type == DataType.BODY_BATTERY:
            result = self._daily_gql(
                lambda d: f'query{{epochChartScalar(date:"{d}", include:["bodyBattery","stress"])}}',
                metric, start, end,
            )

        elif data_type == DataType.WEIGHT:
            result = self._range_gql(
                f'query{{weightScalar(startDate:"{s}", endDate:"{e}")}}',
                metric,
            )

        elif data_type == DataType.BODY_COMPOSITION:
            result = self._range_rest(
                f"weight-service/weight/dateRange?startDate={s}&endDate={e}",
                metric,
            )

        elif data_type == DataType.ENDURANCE_SCORE:
            result = self._daily_rest(
                lambda d: f"metrics-service/metrics/endurancescore?calendarDate={d}",
                metric, start, end,
            )

        elif data_type == DataType.HILL_SCORE:
            result = self._daily_rest(
                lambda d: f"metrics-service/metrics/hillscore?calendarDate={d}",
                metric, start, end,
            )

        elif data_type == DataType.DEVICE_INFO:
            result = self._simple_rest(
                "device-service/deviceregistration/devices",
                metric,
            )

        elif data_type == DataType.RACE_PREDICTIONS:
            result = self._daily_rest(
                lambda d: (
                    f"metrics-service/metrics/racepredictions/daily/{dn}"
                    f"?fromCalendarDate={d}&toCalendarDate={d}"
                ),
                metric, start, end,
            )

        elif data_type == DataType.ACTIVITY_DETAILS:
            result = self._fetch_per_activity("activity_details", metric, start, end)

        elif data_type == DataType.ACTIVITY_SPLITS:
            result = self._fetch_per_activity("activity_splits", metric, start, end)

        elif data_type == DataType.ACTIVITY_WEATHER:
            result = self._fetch_per_activity("activity_weather", metric, start, end)

        elif data_type == DataType.ACTIVITY_HR_ZONES:
            result = self._fetch_per_activity("activity_hr_zones", metric, start, end)

        elif data_type == DataType.ACTIVITY_EXERCISE_SETS:
            result = self._fetch_per_activity("activity_exercise_sets", metric, start, end)

        else:
            raise GarminConnectorError(f"Unsupported data type: {data_type}")

        self.save_tokens()
        return result
