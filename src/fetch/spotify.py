"""
Spotify Data Fetcher
--------------------
Fetches various types of Spotify user data via the Spotify Web API.

Supported data types:
 - recently_played
 - saved_tracks
 - saved_albums
 - followed_artists
 - playlists
 - user_profile
 - top_tracks
 - top_artists
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from zoneinfo import ZoneInfo

import spotipy
from spotipy.oauth2 import SpotifyOAuth

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 50
DEFAULT_TIMEZONE = "Europe/Paris"


class DataType(Enum):
    """Supported Spotify data types."""

    RECENTLY_PLAYED = "recently_played"
    SAVED_TRACKS = "saved_tracks"
    SAVED_ALBUMS = "saved_albums"
    FOLLOWED_ARTISTS = "followed_artists"
    PLAYLISTS = "playlists"
    USER_PROFILE = "user_profile"
    TOP_TRACKS = "top_tracks"
    TOP_ARTISTS = "top_artists"
    ARTIST_DETAIL = "artist_detail"
    ALBUM_DETAIL = "album_detail"
    ALBUM_TRACKS = "album_tracks"
    ARTIST_ALBUMS = "artist_albums"


@dataclass
class SpotifyConfig:
    """Configuration for the Spotify fetcher."""

    client_id: str
    client_secret: str
    redirect_uri: str
    refresh_token: str
    cache_path: Path
    timezone: str = DEFAULT_TIMEZONE


class SpotifyConnectorError(Exception):
    pass


class SpotifyConnector:
    """Spotify data connector with support for multiple data types."""

    SCOPES = {
        DataType.RECENTLY_PLAYED: "user-read-recently-played",
        DataType.SAVED_TRACKS: "user-library-read",
        DataType.SAVED_ALBUMS: "user-library-read",
        DataType.FOLLOWED_ARTISTS: "user-follow-read",
        DataType.PLAYLISTS: "playlist-read-private playlist-read-collaborative",
        DataType.USER_PROFILE: "user-read-private user-read-email",
        DataType.TOP_TRACKS: "user-top-read",
        DataType.TOP_ARTISTS: "user-top-read",
        # ARTIST_DETAIL and ALBUM_DETAIL use public endpoints, no OAuth scope required
    }

    def __init__(self, config: SpotifyConfig):
        self.config = config
        self._client: Optional[spotipy.Spotify] = None

    def authenticate(self, data_types: List[DataType]) -> None:
        """Authenticate with Spotify for the given data types."""
        required_scopes = set()
        for data_type in data_types:
            if data_type in self.SCOPES:
                required_scopes.update(self.SCOPES[data_type].split())

        scope_string = " ".join(sorted(required_scopes))
        logger.info(f"Authenticating with scopes: {scope_string}")

        try:
            auth_manager = SpotifyOAuth(
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
                redirect_uri=self.config.redirect_uri,
                scope=scope_string,
                cache_path=str(self.config.cache_path),
            )

            if self.config.cache_path.exists():
                self.config.cache_path.unlink()
                logger.debug("Cleared existing token cache")

            try:
                token_info = auth_manager.refresh_access_token(self.config.refresh_token)
                access_token = token_info.get("access_token")
            except Exception as refresh_error:
                raise SpotifyConnectorError(
                    f"Refresh token authentication failed: {refresh_error}. "
                    "Check your refresh token or re-run the OAuth flow."
                )

            if not access_token:
                raise SpotifyConnectorError("Failed to get access token")

            self._client = spotipy.Spotify(auth=access_token)

            try:
                self._client.current_user()
                logger.info("Authenticated successfully and verified")
            except Exception as test_error:
                raise SpotifyConnectorError(
                    f"Authentication verification failed: {test_error}"
                )

        except SpotifyConnectorError:
            raise
        except Exception as e:
            raise SpotifyConnectorError(f"Authentication failed: {e}") from e

    @classmethod
    def from_env(cls, cache_path: Optional[Path] = None) -> "SpotifyConnector":
        """Create a SpotifyConnector from environment variables."""
        required = {
            "SPOTIFY_CLIENT_ID": None,
            "SPOTIFY_CLIENT_SECRET": None,
            "SPOTIFY_REDIRECT_URI": None,
            "SPOTIFY_REFRESH_TOKEN": None,
        }
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise SpotifyConnectorError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        config = SpotifyConfig(
            client_id=os.environ["SPOTIFY_CLIENT_ID"],
            client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
            redirect_uri=os.environ["SPOTIFY_REDIRECT_URI"],
            refresh_token=os.environ["SPOTIFY_REFRESH_TOKEN"],
            cache_path=cache_path or Path(".spotify_cache"),
        )
        return cls(config)

    @property
    def client(self) -> spotipy.Spotify:
        if self._client is None:
            raise SpotifyConnectorError("Not authenticated. Call authenticate() first.")
        return self._client

    def fetch_recently_played(self, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
        """Fetch recently played tracks since 23:00 yesterday (1-hour safety buffer)."""
        try:
            paris_tz = ZoneInfo("Europe/Paris")
            now_paris = datetime.now(paris_tz)
            today_midnight_paris = now_paris.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_23h_paris = today_midnight_paris - timedelta(hours=1)
            after_timestamp = int(yesterday_23h_paris.timestamp() * 1000)

            results = self.client.current_user_recently_played(limit=limit, after=after_timestamp)
            items = results.get("items", [])
            logger.info(f"Fetched {len(items)} tracks played since 23:00 yesterday")
            return items
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching recently played: {e}") from e

    def fetch_saved_tracks(self, **kwargs) -> List[Dict[str, Any]]:
        """Fetch all saved tracks (full backfill, paginated)."""
        try:
            items = []
            offset = 0
            batch_size = 50

            while True:
                results = self.client.current_user_saved_tracks(limit=batch_size, offset=offset)
                batch_items = results.get("items", [])
                if not batch_items:
                    break
                items.extend(batch_items)
                offset += len(batch_items)
                if len(batch_items) < batch_size:
                    break

            logger.info(f"Fetched {len(items)} saved tracks (full backfill)")
            return items
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching saved tracks: {e}") from e

    def fetch_saved_albums(self, **kwargs) -> List[Dict[str, Any]]:
        """Fetch all saved albums (full backfill, paginated)."""
        try:
            items = []
            offset = 0
            batch_size = 50

            while True:
                results = self.client.current_user_saved_albums(limit=batch_size, offset=offset)
                batch_items = results.get("items", [])
                if not batch_items:
                    break
                items.extend(batch_items)
                offset += len(batch_items)
                if len(batch_items) < batch_size:
                    break

            logger.info(f"Fetched {len(items)} saved albums (full backfill)")
            return items
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching saved albums: {e}") from e

    def fetch_followed_artists(self, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
        """Fetch user's followed artists."""
        try:
            items = []
            after = None
            batch_size = min(limit, 50)

            while len(items) < limit:
                remaining = limit - len(items)
                current_limit = min(batch_size, remaining)
                results = self.client.current_user_followed_artists(
                    limit=current_limit, after=after
                )
                artists_data = results.get("artists", {})
                batch_items = artists_data.get("items", [])
                if not batch_items:
                    break

                items.extend(batch_items)
                cursors = artists_data.get("cursors", {})
                after = cursors.get("after")
                if not after or len(batch_items) < current_limit:
                    break

            logger.info(f"Fetched {len(items)} followed artists")
            return items[:limit]
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching followed artists: {e}") from e

    def fetch_playlists(self, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
        """Fetch user's playlists."""
        try:
            results = self.client.current_user_playlists(limit=limit)
            items = results.get("items", [])
            logger.info(f"Fetched {len(items)} playlists")
            return items
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching playlists: {e}") from e

    def fetch_user_profile(self) -> Dict[str, Any]:
        """Fetch user profile information."""
        try:
            profile = self.client.current_user()
            logger.info("Fetched user profile")
            return profile
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching user profile: {e}") from e

    def fetch_top_tracks(
        self, limit: int = DEFAULT_LIMIT, time_range: str = "medium_term"
    ) -> List[Dict[str, Any]]:
        """Fetch user's top tracks."""
        try:
            results = self.client.current_user_top_tracks(limit=limit, time_range=time_range)
            items = results.get("items", [])
            logger.info(f"Fetched {len(items)} top tracks ({time_range})")
            return items
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching top tracks: {e}") from e

    def fetch_top_artists(
        self, limit: int = DEFAULT_LIMIT, time_range: str = "medium_term"
    ) -> List[Dict[str, Any]]:
        """Fetch user's top artists."""
        try:
            results = self.client.current_user_top_artists(limit=limit, time_range=time_range)
            items = results.get("items", [])
            logger.info(f"Fetched {len(items)} top artists ({time_range})")
            return items
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching top artists: {e}") from e

    def fetch_artist_details(self, ids: List[str], **kwargs) -> List[Dict[str, Any]]:
        """Fetch artist details for a list of Spotify artist IDs (batched, 50 per call)."""
        if not ids:
            return []
        try:
            results = []
            batch_size = 50
            for i in range(0, len(ids), batch_size):
                batch = ids[i : i + batch_size]
                response = self.client.artists(batch)
                results.extend(response.get("artists", []))
            logger.info(f"Fetched details for {len(results)} artists")
            return results
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching artist details: {e}") from e

    def fetch_album_details(self, ids: List[str], **kwargs) -> List[Dict[str, Any]]:
        """Fetch album details for a list of Spotify album IDs (batched, 20 per call)."""
        if not ids:
            return []
        try:
            results = []
            batch_size = 20  # Spotify /albums endpoint limit
            for i in range(0, len(ids), batch_size):
                batch = ids[i : i + batch_size]
                response = self.client.albums(batch)
                results.extend(a for a in response.get("albums", []) if a is not None)
            logger.info(f"Fetched details for {len(results)} albums")
            return results
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching album details: {e}") from e

    def fetch_album_tracks(self, ids: List[str], **kwargs) -> List[Dict[str, Any]]:
        """Fetch all tracks for each album ID (paginated). Injects album_id into each record."""
        import time

        results = []
        failed = []
        batch_size = 50
        for album_id in ids:
            try:
                offset = 0
                while True:
                    response = self.client.album_tracks(album_id, limit=batch_size, offset=offset)
                    items = response.get("items", [])
                    for track in items:
                        track["album_id"] = album_id
                    results.extend(items)
                    offset += len(items)
                    if len(items) < batch_size:
                        break
                time.sleep(0.1)
            except Exception as e:
                logger.warning(f"Skipping album {album_id}: {e}")
                failed.append(album_id)

        if failed:
            logger.warning(f"Failed to fetch tracks for {len(failed)}/{len(ids)} albums: {failed}")
        logger.info(f"Fetched {len(results)} tracks across {len(ids) - len(failed)} albums")
        return results

    def fetch_artist_albums(self, ids: List[str], **kwargs) -> List[Dict[str, Any]]:
        """Fetch all albums for each artist ID (paginated). Injects artist_id into each record."""
        import time

        results = []
        failed = []
        batch_size = 50
        for artist_id in ids:
            try:
                offset = 0
                while True:
                    response = self.client.artist_albums(artist_id, album_type="album", limit=batch_size, offset=offset)
                    items = response.get("items", [])
                    for album in items:
                        album["artist_id"] = artist_id
                    results.extend(items)
                    offset += len(items)
                    if len(items) < batch_size:
                        break
                time.sleep(0.1)
            except Exception as e:
                logger.warning(f"Skipping artist {artist_id}: {e}")
                failed.append(artist_id)

        if failed:
            logger.warning(f"Failed to fetch albums for {len(failed)}/{len(ids)} artists: {failed}")
        logger.info(f"Fetched {len(results)} albums across {len(ids) - len(failed)} artists")
        return results

    def fetch_data(
        self, data_type: DataType, **kwargs
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Dispatch fetch by DataType."""
        method_map = {
            DataType.RECENTLY_PLAYED: self.fetch_recently_played,
            DataType.SAVED_TRACKS: self.fetch_saved_tracks,
            DataType.SAVED_ALBUMS: self.fetch_saved_albums,
            DataType.FOLLOWED_ARTISTS: self.fetch_followed_artists,
            DataType.PLAYLISTS: self.fetch_playlists,
            DataType.USER_PROFILE: self.fetch_user_profile,
            DataType.TOP_TRACKS: self.fetch_top_tracks,
            DataType.TOP_ARTISTS: self.fetch_top_artists,
            DataType.ARTIST_DETAIL: self.fetch_artist_details,
            DataType.ALBUM_DETAIL: self.fetch_album_details,
            DataType.ALBUM_TRACKS: self.fetch_album_tracks,
            DataType.ARTIST_ALBUMS: self.fetch_artist_albums,
        }

        if data_type not in method_map:
            raise SpotifyConnectorError(f"Unsupported data type: {data_type}")

        # Filter out kwargs not relevant to Spotify (e.g. days from Garmin)
        kwargs.pop("days", None)
        return method_map[data_type](**kwargs)
