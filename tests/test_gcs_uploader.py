"""Unit tests for the GCS streaming uploader."""

import datetime
import io
import json
from typing import Any
import unittest
from unittest.mock import patch

from octopulse.ingestion.gcs_uploader import (
    build_gcs_blob_path,
    project_event,
    run_daily_ingestion_to_writer,
)

BRONZE_KEYS = {"id", "type", "actor_login", "repo_id", "repo_name", "payload", "created_at", "event_date"}


def make_event(
    event_id: str = "1",
    event_type: str = "WatchEvent",
    repo_name: str = "a/b",
    created_at: str = "2026-11-01T00:00:00Z",
    **extra: Any,
) -> dict[str, Any]:
    """Build a raw GH Archive event that has every REQUIRED column."""
    event: dict[str, Any] = {
        "id": event_id,
        "type": event_type,
        "actor": {"id": 1, "login": "octocat"},
        "repo": {"id": 2, "name": repo_name},
        "payload": {"action": "started"},
        "public": True,
        "created_at": created_at,
    }
    event.update(extra)
    return event


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
        mock_fetch.return_value = [b"dummy_chunk"]
        mock_extract.return_value = [
            make_event("101", "WatchEvent", "fastapi/fastapi"),
            make_event("102", "ForkEvent", "astral-sh/uv"),
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
        self.assertEqual(summary.skipped_events, 0)
        self.assertEqual(summary.processed_hours, [0, 1])
        self.assertEqual(summary.failed_hours, [])

        written_lines = [line for line in string_buffer.getvalue().split("\n") if line]
        self.assertEqual(len(written_lines), 4)
        first_event = json.loads(written_lines[0])
        self.assertEqual(first_event["id"], "101")
        self.assertEqual(first_event["type"], "WatchEvent")
        self.assertEqual(set(first_event), BRONZE_KEYS)

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
        mock_extract.return_value = [make_event("201", "IssuesEvent", "org/repo")]

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
            yield make_event("1", "WatchEvent")
            if calls["n"] == 1:
                raise ValueError("Truncated gzip stream")
            yield make_event("2", "ForkEvent")

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
            yield make_event("1", "WatchEvent")
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

    @patch("octopulse.ingestion.gcs_uploader.extract_target_events")
    @patch("octopulse.ingestion.gcs_uploader.fetch_hourly_stream")
    def test_events_missing_required_columns_are_skipped_and_counted(self, mock_fetch, mock_extract):
        """REQUIRED 欠落イベントは出力せず、skipped_events に集計される。"""
        mock_fetch.return_value = [b"chunk"]
        no_actor = make_event("2")
        del no_actor["actor"]
        no_created_at = make_event("3")
        del no_created_at["created_at"]
        no_repo_name = make_event("4")
        no_repo_name["repo"] = {"id": 2}
        mock_extract.return_value = [make_event("1"), no_actor, no_created_at, no_repo_name]

        buffer = io.StringIO()
        summary = run_daily_ingestion_to_writer(
            target_date=datetime.date(2026, 11, 1),
            writer=buffer,
            destination_uri="gs://b/raw/x.jsonl",
            hours=[0, 1],
        )

        ids = [json.loads(line)["id"] for line in buffer.getvalue().splitlines() if line]
        self.assertEqual(ids, ["1", "1"])  # 1 valid event per hour
        self.assertEqual(summary.total_events_extracted, 2)
        self.assertEqual(summary.skipped_events, 6)  # 3 invalid events * 2 hours
        self.assertEqual(summary.status, "SUCCESS")

    def test_project_event_drops_heavy_payload(self):
        """Only the fields used downstream are kept (DS-001 2.2)."""
        raw = make_event(
            "9",
            "PullRequestEvent",
            "o/r",
            payload={"action": "closed", "pull_request": {"merged": True, "body": "z" * 20000}},
        )

        slim = project_event(raw)

        assert slim is not None
        self.assertEqual(slim["payload"], {"action": "closed", "merged": True})
        self.assertLess(len(json.dumps(slim)), 500)

    def test_project_event_is_flat_with_event_date(self):
        """Bronze と 1 対 1 のフラット形式で、event_date が created_at の日付部分になる。"""
        raw = make_event("5", "WatchEvent", "o/r", created_at="2026-11-01T23:59:59Z")

        row = project_event(raw)

        assert row is not None
        self.assertEqual(set(row), BRONZE_KEYS)
        self.assertEqual(row["actor_login"], "octocat")
        self.assertEqual(row["repo_id"], 2)
        self.assertEqual(row["repo_name"], "o/r")
        self.assertEqual(row["created_at"], "2026-11-01T23:59:59Z")
        self.assertEqual(row["event_date"], "2026-11-01")
        self.assertEqual(row["payload"], {"action": "started"})
        self.assertNotIn("public", row)
        self.assertNotIn("actor", row)

    def test_project_event_merged_only_for_pull_request(self):
        """merged は PullRequestEvent の payload にのみ入る。"""
        pr = project_event(
            make_event("6", "PullRequestEvent", payload={"action": "closed", "pull_request": {"merged": False}})
        )
        issue = project_event(make_event("7", "IssuesEvent", payload={"action": "opened"}))

        assert pr is not None and issue is not None
        self.assertEqual(pr["payload"], {"action": "closed", "merged": False})
        self.assertEqual(issue["payload"], {"action": "opened"})

    def test_project_event_create_event_repository(self):
        """CreateEvent(repository) は payload に ref_type を持つ。"""
        raw = make_event(
            "8",
            "CreateEvent",
            "new/repo",
            payload={"ref": None, "ref_type": "repository", "master_branch": "main", "description": "x" * 5000},
        )

        row = project_event(raw)

        assert row is not None
        self.assertEqual(row["payload"], {"action": None, "ref_type": "repository"})
        self.assertLess(len(json.dumps(row)), 500)

    def test_project_event_repo_id_nullable(self):
        """repo_id は NULLABLE: 欠落していても出力され、null になる。"""
        raw = make_event("10")
        raw["repo"] = {"name": "a/b"}

        row = project_event(raw)

        assert row is not None
        self.assertIsNone(row["repo_id"])

    def test_project_event_returns_none_when_required_missing(self):
        """REQUIRED 列(id/type/actor_login/repo_name/created_at)の欠落は None。"""
        for key in ("id", "type", "actor", "repo", "created_at"):
            raw = make_event()
            del raw[key]
            self.assertIsNone(project_event(raw), msg=f"{key} 欠落")

        self.assertIsNone(project_event(make_event(created_at="not-a-date")))
        self.assertIsNone(project_event(make_event(created_at="")))


if __name__ == "__main__":
    unittest.main()
