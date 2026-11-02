"""Unit tests for the GCS streaming uploader."""

import datetime
import io
import json
import unittest
from unittest.mock import patch

from octopulse.ingestion.gcs_uploader import (
    build_gcs_blob_path,
    project_event,
    run_daily_ingestion_to_writer,
)


class TestGCSUploader(unittest.TestCase):
    """Test suite for GCS uploader logic."""

    def test_build_gcs_blob_path(self):
        """Verify canonical GCS object path format conforming to DS-001."""
        target_date = datetime.date(2026, 10, 31)
        path = build_gcs_blob_path(target_date)
        self.assertEqual(path, "raw/2026/10/31/events_20261031.jsonl")

    @patch("octopulse.ingestion.gcs_uploader.extract_target_events")
    @patch("octopulse.ingestion.gcs_uploader.fetch_hourly_stream")
    def test_run_daily_ingestion_to_writer_success(self, mock_fetch, mock_extract):
        """Verify that events are streamed directly to writer without local files."""
        # Mock 2 events for hour 0 and hour 1
        mock_fetch.return_value = [b"dummy_chunk"]
        mock_extract.return_value = [
            {"id": "101", "type": "WatchEvent", "repo": {"name": "fastapi/fastapi"}},
            {"id": "102", "type": "ForkEvent", "repo": {"name": "astral-sh/uv"}},
        ]

        string_buffer = io.StringIO()
        target_date = datetime.date(2026, 11, 1)

        summary = run_daily_ingestion_to_writer(
            target_date=target_date,
            writer=string_buffer,
            destination_uri="gs://test-bucket/raw/2026/11/01/events_20261101.jsonl",
            hours=[0, 1],  # Test with 2 hours
        )

        self.assertEqual(summary.status, "SUCCESS")
        self.assertEqual(summary.total_events_extracted, 4)  # 2 events * 2 hours
        self.assertEqual(summary.processed_hours, [0, 1])
        self.assertEqual(summary.failed_hours, [])

        # Verify output content in buffer
        written_lines = [line for line in string_buffer.getvalue().split("\n") if line]
        self.assertEqual(len(written_lines), 4)
        first_event = json.loads(written_lines[0])
        self.assertEqual(first_event["id"], "101")
        self.assertEqual(first_event["type"], "WatchEvent")

    @patch("octopulse.ingestion.gcs_uploader.time.sleep")
    @patch("octopulse.ingestion.gcs_uploader.extract_target_events")
    @patch("octopulse.ingestion.gcs_uploader.fetch_hourly_stream")
    def test_run_daily_ingestion_partial_failure(self, mock_fetch, mock_extract, _mock_sleep):
        """Verify handling when some hours fail."""
        def side_effect(date, hour):
            if hour == 1:
                raise RuntimeError("HTTP 500 Server Error")
            return [b"chunk"]

        mock_fetch.side_effect = side_effect
        mock_extract.return_value = [{"id": "201", "type": "IssuesEvent", "repo": {"name": "org/repo"}}]

        string_buffer = io.StringIO()
        target_date = datetime.date(2026, 11, 1)

        summary = run_daily_ingestion_to_writer(
            target_date=target_date,
            writer=string_buffer,
            destination_uri="gs://test-bucket/raw/test.jsonl",
            hours=[0, 1],
        )

        self.assertEqual(summary.status, "PARTIAL_SUCCESS")
        self.assertEqual(summary.total_events_extracted, 1)
        self.assertEqual(summary.processed_hours, [0])
        self.assertEqual(summary.failed_hours, [1])

    @patch("octopulse.ingestion.gcs_uploader.time.sleep")
    @patch("octopulse.ingestion.gcs_uploader.extract_target_events")
    @patch("octopulse.ingestion.gcs_uploader.fetch_hourly_stream")
    def test_failed_hour_is_retried_without_duplicate_rows(self, mock_fetch, mock_extract, _mock_sleep):
        """A mid-stream failure must retry the whole hour and write each row once."""
        mock_fetch.return_value = [b"chunk"]
        calls = {"n": 0}

        def flaky_extract(_stream):
            calls["n"] += 1
            yield {"id": "1", "type": "WatchEvent", "repo": {"name": "a/b"}}
            if calls["n"] == 1:
                raise ValueError("Truncated gzip stream")
            yield {"id": "2", "type": "ForkEvent", "repo": {"name": "a/b"}}

        mock_extract.side_effect = flaky_extract

        buffer = io.StringIO()
        summary = run_daily_ingestion_to_writer(
            target_date=datetime.date(2026, 11, 1),
            writer=buffer,
            destination_uri="gs://b/raw/x.jsonl",
            hours=[0],
        )

        ids = [json.loads(line)["id"] for line in buffer.getvalue().splitlines() if line]
        self.assertEqual(summary.status, "SUCCESS")
        self.assertEqual(ids, ["1", "2"])

    @patch("octopulse.ingestion.gcs_uploader.time.sleep")
    @patch("octopulse.ingestion.gcs_uploader.extract_target_events")
    @patch("octopulse.ingestion.gcs_uploader.fetch_hourly_stream")
    def test_exhausted_retries_leave_no_partial_rows(self, mock_fetch, mock_extract, _mock_sleep):
        """If every attempt fails, nothing from that hour is written."""
        mock_fetch.return_value = [b"chunk"]

        def always_broken(_stream):
            yield {"id": "1", "type": "WatchEvent", "repo": {"name": "a/b"}}
            raise ValueError("Truncated gzip stream")

        mock_extract.side_effect = always_broken

        buffer = io.StringIO()
        summary = run_daily_ingestion_to_writer(
            target_date=datetime.date(2026, 11, 1),
            writer=buffer,
            destination_uri="gs://b/raw/x.jsonl",
            hours=[0],
        )

        self.assertEqual(summary.status, "FAILED")
        self.assertEqual(summary.failed_hours, [0])
        self.assertEqual(buffer.getvalue(), "")

    def test_project_event_drops_heavy_payload(self):
        """Only the fields used downstream are kept (DS-001 2.2)."""
        raw = {
            "id": "9",
            "type": "PullRequestEvent",
            "actor": {"id": 1, "login": "octocat", "avatar_url": "x"},
            "repo": {"id": 2, "name": "o/r", "url": "y"},
            "payload": {"action": "closed", "pull_request": {"merged": True, "body": "z" * 20000}},
            "public": True,
            "created_at": "2026-11-01T00:00:00Z",
        }

        slim = project_event(raw)

        self.assertEqual(slim["payload"], {"action": "closed", "merged": True})
        self.assertEqual(slim["actor"], {"id": 1, "login": "octocat"})
        self.assertEqual(slim["repo"], {"id": 2, "name": "o/r"})
        self.assertLess(len(json.dumps(slim)), 500)
