import unittest
from unittest.mock import patch

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.fetch.spotify import DataType
from src.load.spotify import load


class SpotifyLoaderTest(unittest.TestCase):
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
