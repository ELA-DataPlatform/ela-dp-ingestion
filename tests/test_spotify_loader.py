import unittest
from unittest.mock import patch

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import run
from src.fetch.spotify import DataType
from src.load.spotify import load


class SpotifyLoaderTest(unittest.TestCase):
    def test_worklists_use_dbt_managed_hub_dataset(self):
        config = run._load_loading_config()
        expected = {
            "artist_detail": "hub_music_stg.artists_pending_detail",
            "album_detail": "hub_music_stg.albums_pending_detail",
            "album_tracks": "hub_music_stg.all_album_ids",
            "artist_albums": "hub_music_stg.all_artist_ids",
        }

        for data_type, table in expected.items():
            source, _ = run._get_ids_source(config, "spotify", data_type, "prd")
            self.assertEqual(source, table)

    @patch("src.load.spotify.bigquery.Client")
    def test_reuses_existing_table_schema(self, client_cls):
        client = client_cls.return_value
        schema = [bigquery.SchemaField("release_date", "STRING")]
        client.get_table.return_value.schema = schema
        client.load_table_from_uri.return_value.result.return_value = None

        load(
            "gs://bucket/spotify_album_detail.jsonl",
            DataType.ALBUM_DETAIL,
            project="ela-dp-dev",
            dataset="dp_lake_spotify_dev",
            table="normalized_album_detail",
        )

        job_config = client.load_table_from_uri.call_args.kwargs["job_config"]
        self.assertFalse(job_config.autodetect)
        self.assertEqual(job_config.schema, schema)
        self.assertTrue(job_config.ignore_unknown_values)

    @patch("src.load.spotify.bigquery.Client")
    def test_uses_autodetect_when_table_does_not_exist(self, client_cls):
        client = client_cls.return_value
        client.get_table.side_effect = NotFound("missing")
        client.load_table_from_uri.return_value.result.return_value = None

        load(
            "gs://bucket/spotify_saved_tracks.jsonl",
            DataType.SAVED_TRACKS,
            project="ela-dp-dev",
            dataset="dp_lake_spotify_dev",
            table="normalized_saved_tracks",
        )

        job_config = client.load_table_from_uri.call_args.kwargs["job_config"]
        self.assertTrue(job_config.autodetect)
        self.assertIsNone(job_config.schema)


if __name__ == "__main__":
    unittest.main()
