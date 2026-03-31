"""
Garmin Data Fetcher
-------------------
Fetches Garmin Connect data using a browser session (cookies) maintained by
scripts/garmin_session_refresh.py running on Mac mini and stored in GCS.

Authentication is decoupled: the Mac mini handles Camoufox/Cloudflare login
and uploads garmin_session.json to GCS. Cloud Run simply loads those cookies
and calls the Garmin Connect REST/GraphQL APIs directly with requests.

Supported data types (29):
 - activities, activity_details, activity_splits, activity_weather,
   activity_hr_zones, activity_exercise_sets
 - sleep, steps, heart_rate, body_battery, stress, weight, body_composition
 - user_summary, stats_and_body, training_readiness, rhr_daily, spo2,
   respiration, intensity_minutes, max_metrics, all_day_events
 - device_info, training_status, hrv, race_predictions, floors,
   endurance_score, hill_score
"""

import json
import logging
import os
import time
from datetime import date, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

GARMIN_BASE = "https://connect.garmin.com"
GARMIN_GQL = f"{GARMIN_BASE}/gc-api/graphql-gateway/graphql"

DEFAULT_DAYS = 3

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) "
        "Gecko/20100101 Firefox/132.0"
    ),
    "Origin": "https://connect.garmin.com",
    "Referer": "https://connect.garmin.com/modern/",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "DNT": "1",
}


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

    Loads a browser session (cookies + CSRF token) from GCS — maintained by
    scripts/garmin_session_refresh.py on Mac mini — and uses it to call
    Garmin Connect REST and GraphQL endpoints directly with requests.
    """

    SESSION_FILE = "garmin_session.json"

    def __init__(self, username: str, password: str, tokenstore_gcs: str = ""):
        self.username = username
        self.password = password
        self.tokenstore_gcs = tokenstore_gcs
        self._session: Optional[requests.Session] = None
        self._csrf: Optional[str] = None
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

    def _download_session_from_gcs(self) -> Optional[dict]:
        if not self.tokenstore_gcs:
            return None
        try:
            from google.cloud import storage

            gcs_path = self.tokenstore_gcs[len("gs://"):]
            bucket_name, prefix = gcs_path.split("/", 1)
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(f"{prefix.rstrip('/')}/{self.SESSION_FILE}")
            if not blob.exists():
                logger.warning(f"Session file not found in GCS: {self.SESSION_FILE}")
                return None
            return json.loads(blob.download_as_text())
        except Exception as e:
            logger.warning(f"Failed to download session from GCS: {e}")
            return None

    def authenticate(self, data_types=None) -> None:
        """
        Load the Garmin session from GCS and build a requests.Session.

        The session (cookies + CSRF token) is maintained by
        scripts/garmin_session_refresh.py running on Mac mini.
        """
        session_data = self._download_session_from_gcs()
        if not session_data:
            raise GarminConnectorError(
                "No Garmin session found in GCS. "
                "Run scripts/garmin_session_refresh.py on Mac mini first."
            )

        age_h = (time.time() - session_data.get("saved_at", 0)) / 3600
        if age_h > 1:
            logger.warning(f"Garmin session is {age_h:.1f}h old — may have expired")

        self._csrf = session_data.get("csrf_token", "")
        self._display_name = session_data.get("display_name", "")
        cookies = session_data.get("cookies", [])

        self._session = requests.Session()
        self._session.headers.update({
            **_BROWSER_HEADERS,
            "connect-csrf-token": self._csrf,
        })
        for c in cookies:
            self._session.cookies.set(
                c["name"],
                c["value"],
                domain=c.get("domain", ""),
                path=c.get("path", "/"),
            )

        logger.info(
            f"Session loaded: {len(cookies)} cookies, "
            f"display_name={self._display_name}, "
            f"age={age_h:.1f}h"
        )

    # -------------------------------------------------------------------------
    # HTTP helpers
    # -------------------------------------------------------------------------

    def _get(self, path: str, retries: int = 3) -> Optional[Any]:
        url = f"{GARMIN_BASE}{path}" if path.startswith("/") else path
        for attempt in range(retries):
            try:
                resp = self._session.get(url, timeout=30)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"429 on {path}, retrying in {wait}s")
                    time.sleep(wait)
                    continue
                logger.warning(f"GET {path} → {resp.status_code}")
                return None
            except Exception as e:
                logger.warning(f"GET {path} error: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
        return None

    def _gql(self, query: str, retries: int = 3) -> Optional[Any]:
        for attempt in range(retries):
            try:
                resp = self._session.post(
                    GARMIN_GQL,
                    json={"query": query},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    # Unwrap {"data": {"queryName": result}}
                    if isinstance(data, dict) and "data" in data:
                        inner = data["data"]
                        if isinstance(inner, dict) and len(inner) == 1:
                            return list(inner.values())[0]
                        return inner
                    return data
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"429 on GQL, retrying in {wait}s")
                    time.sleep(wait)
                    continue
                logger.warning(f"GQL → {resp.status_code}")
                return None
            except Exception as e:
                logger.warning(f"GQL error: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
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
                f"/gc-api/activitylist-service/activities/search/activities"
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
            "activity_details": lambda aid: f"/gc-api/activity-service/activity/{aid}",
            "activity_splits": lambda aid: f"/gc-api/activity-service/activity/{aid}/splits",
            "activity_weather": lambda aid: f"/gc-api/activity-service/activity/{aid}/weather",
            "activity_hr_zones": lambda aid: f"/gc-api/activity-service/activity/{aid}/hrTimeInZones",
            "activity_exercise_sets": lambda aid: f"/gc-api/activity-service/activity/{aid}/exerciseSets",
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
        """Dispatch fetch by DataType."""
        days = kwargs.get("days", DEFAULT_DAYS)
        end = date.today()
        start = end - timedelta(days=days)
        s = start.isoformat()
        e = end.isoformat()
        dn = self._display_name
        metric = data_type.value

        logger.info(f"Fetching {metric} ({days} days, {s} → {e})...")

        if data_type == DataType.ACTIVITIES:
            return self._fetch_activities(s, e, metric)

        elif data_type == DataType.SLEEP:
            return self._daily_rest(
                lambda d: f"/gc-api/wellness-service/wellness/dailySleepData/{dn}?date={d}",
                metric, start, end,
            )

        elif data_type == DataType.STEPS:
            return self._daily_rest(
                lambda d: f"/gc-api/usersummary-service/stats/steps/daily/{d}/{d}",
                metric, start, end,
            )

        elif data_type == DataType.HEART_RATE:
            return self._daily_rest(
                lambda d: f"/gc-api/wellness-service/wellness/dailyHeartRate/{dn}?date={d}",
                metric, start, end,
            )

        elif data_type == DataType.STRESS:
            return self._daily_rest(
                lambda d: f"/gc-api/wellness-service/wellness/dailyStress/{d}",
                metric, start, end,
            )

        elif data_type == DataType.USER_SUMMARY:
            return self._daily_rest(
                lambda d: f"/gc-api/usersummary-service/usersummary/daily/{dn}?calendarDate={d}",
                metric, start, end,
            )

        elif data_type == DataType.STATS_AND_BODY:
            return self._daily_rest(
                lambda d: f"/gc-api/usersummary-service/stats/daily/{d}/{d}",
                metric, start, end,
            )

        elif data_type == DataType.RHR_DAILY:
            # Resting HR is embedded in the daily heart rate response
            return self._daily_rest(
                lambda d: f"/gc-api/wellness-service/wellness/dailyHeartRate/{dn}?date={d}",
                metric, start, end,
            )

        elif data_type == DataType.SPO2:
            return self._daily_rest(
                lambda d: f"/gc-api/wellness-service/wellness/dailySpo2/{d}",
                metric, start, end,
            )

        elif data_type == DataType.RESPIRATION:
            return self._daily_rest(
                lambda d: f"/gc-api/wellness-service/wellness/daily/respiration/{d}",
                metric, start, end,
            )

        elif data_type == DataType.INTENSITY_MINUTES:
            return self._daily_rest(
                lambda d: f"/gc-api/wellness-service/wellness/daily/im/{d}",
                metric, start, end,
            )

        elif data_type == DataType.MAX_METRICS:
            return self._range_rest(
                f"/gc-api/metrics-service/metrics/maxmet/daily/{s}/{e}",
                metric,
            )

        elif data_type == DataType.ALL_DAY_EVENTS:
            return self._daily_gql(
                lambda d: f'query{{dailyEventsScalar(date:"{d}")}}',
                metric, start, end,
            )

        elif data_type == DataType.TRAINING_STATUS:
            return self._daily_gql(
                lambda d: f'query{{trainingStatusDailyScalar(calendarDate:"{d}")}}',
                metric, start, end,
            )

        elif data_type == DataType.TRAINING_READINESS:
            return self._range_gql(
                f'query{{trainingReadinessRangeScalar(startDate:"{s}", endDate:"{e}")}}',
                metric,
            )

        elif data_type == DataType.HRV:
            return self._range_gql(
                f'query{{heartRateVariabilityScalar(startDate:"{s}", endDate:"{e}")}}',
                metric,
            )

        elif data_type == DataType.FLOORS:
            return self._daily_rest(
                lambda d: f"/gc-api/wellness-service/wellness/floorsChartData/daily/{d}",
                metric, start, end,
            )

        elif data_type == DataType.BODY_BATTERY:
            return self._daily_gql(
                lambda d: f'query{{epochChartScalar(date:"{d}", include:["bodyBattery","stress"])}}',
                metric, start, end,
            )

        elif data_type == DataType.WEIGHT:
            return self._range_gql(
                f'query{{weightScalar(startDate:"{s}", endDate:"{e}")}}',
                metric,
            )

        elif data_type == DataType.BODY_COMPOSITION:
            return self._range_rest(
                f"/gc-api/weight-service/weight/dateRange?startDate={s}&endDate={e}",
                metric,
            )

        elif data_type == DataType.ENDURANCE_SCORE:
            return self._daily_rest(
                lambda d: f"/gc-api/metrics-service/metrics/endurancescore?calendarDate={d}",
                metric, start, end,
            )

        elif data_type == DataType.HILL_SCORE:
            return self._daily_rest(
                lambda d: f"/gc-api/metrics-service/metrics/hillscore?calendarDate={d}",
                metric, start, end,
            )

        elif data_type == DataType.DEVICE_INFO:
            return self._simple_rest(
                "/gc-api/device-service/deviceregistration/devices",
                metric,
            )

        elif data_type == DataType.RACE_PREDICTIONS:
            return self._daily_rest(
                lambda d: (
                    f"/gc-api/metrics-service/metrics/racepredictions/daily/{dn}"
                    f"?fromCalendarDate={d}&toCalendarDate={d}"
                ),
                metric, start, end,
            )

        elif data_type == DataType.ACTIVITY_DETAILS:
            return self._fetch_per_activity("activity_details", metric, start, end)

        elif data_type == DataType.ACTIVITY_SPLITS:
            return self._fetch_per_activity("activity_splits", metric, start, end)

        elif data_type == DataType.ACTIVITY_WEATHER:
            return self._fetch_per_activity("activity_weather", metric, start, end)

        elif data_type == DataType.ACTIVITY_HR_ZONES:
            return self._fetch_per_activity("activity_hr_zones", metric, start, end)

        elif data_type == DataType.ACTIVITY_EXERCISE_SETS:
            return self._fetch_per_activity("activity_exercise_sets", metric, start, end)

        else:
            raise GarminConnectorError(f"Unsupported data type: {data_type}")
