"""Unit tests for the stream engine."""

import datetime
import gzip
import json
import unittest
import urllib.error
from unittest.mock import MagicMock, patch

from octopulse.ingestion.stream_engine import (
    TARGET_EVENT_TYPES,
    build_gh_archive_url,
    extract_target_events,
    fetch_hourly_stream,
)


class TestStreamEngine(unittest.TestCase):
    """Test suite for stream engine."""

    def test_build_gh_archive_url(self):
        """Verify URL generation and hour validation."""
        target_date = datetime.date(2026, 10, 31)
        url = build_gh_archive_url(target_date, 15)
        self.assertEqual(url, "https://data.gharchive.org/2026-10-31-15.json.gz")

        with self.assertRaises(ValueError):
            build_gh_archive_url(target_date, 24)

        with self.assertRaises(ValueError):
            build_gh_archive_url(target_date, -1)

    def test_extract_target_events_filters_correctly(self):
        """Verify that only the 4 target events are extracted from a compressed stream."""
        sample_events = [
            {"id": "1", "type": "WatchEvent", "repo": {"name": "fastapi/fastapi"}},
            {"id": "2", "type": "PushEvent", "repo": {"name": "fastapi/fastapi"}},  # Ignored
            {"id": "3", "type": "ForkEvent", "repo": {"name": "astral-sh/uv"}},
            {"id": "4", "type": "CreateEvent", "repo": {"name": "other/repo"}},  # Ignored
            {"id": "5", "type": "IssuesEvent", "repo": {"name": "astral-sh/ruff"}},
            {"id": "6", "type": "PullRequestEvent", "repo": {"name": "pydantic/pydantic"}},
            {"id": "7", "type": "MemberEvent", "repo": {"name": "team/repo"}},  # Ignored
        ]

        # Compress sample events into gzip byte stream
        json_lines = "\n".join(json.dumps(event) for event in sample_events).encode("utf-8")
        compressed_bytes = gzip.compress(json_lines)

        # Simulate chunked stream reading (e.g. 16-byte chunks)
        def chunk_generator(data: bytes, chunk_size: int = 16):
            for i in range(0, len(data), chunk_size):
                yield data[i : i + chunk_size]

        extracted = list(extract_target_events(chunk_generator(compressed_bytes)))

        self.assertEqual(len(extracted), 4)
        extracted_types = {e["type"] for e in extracted}
        self.assertEqual(extracted_types, TARGET_EVENT_TYPES)
        extracted_ids = [e["id"] for e in extracted]
        self.assertEqual(extracted_ids, ["1", "3", "5", "6"])


    def test_extract_target_events_rejects_truncated_gzip(self):
        """A stream that ends before the gzip trailer must fail, not silently succeed."""
        lines = "\n".join(json.dumps({"id": str(i), "type": "WatchEvent"}) for i in range(500)).encode("utf-8")
        compressed = gzip.compress(lines)

        with self.assertRaises(ValueError):
            list(extract_target_events(iter([compressed[: len(compressed) // 2]])))

    @patch("octopulse.ingestion.stream_engine.time.sleep")
    @patch("octopulse.ingestion.stream_engine.urllib.request.urlopen")
    def test_404_is_retried_twice_then_raised(self, mock_urlopen, mock_sleep):
        """DD-001 3.3: 404 is retried 2 times (5s, 15s) before being reported."""
        mock_urlopen.side_effect = urllib.error.HTTPError("u", 404, "Not Found", {}, None)

        with self.assertRaises(urllib.error.HTTPError):
            list(fetch_hourly_stream(datetime.date(2026, 11, 1), 23))

        self.assertEqual(mock_urlopen.call_count, 3)
        self.assertEqual([c.args[0] for c in mock_sleep.call_args_list], [5, 15])

    @patch("octopulse.ingestion.stream_engine.time.sleep")
    @patch("octopulse.ingestion.stream_engine.urllib.request.urlopen")
    def test_5xx_backoff_follows_dd_schedule(self, mock_urlopen, mock_sleep):
        """DD-001 3.3: 5xx is retried 3 times with 5s, 15s, 45s waits."""
        mock_urlopen.side_effect = urllib.error.HTTPError("u", 503, "Unavailable", {}, None)

        with self.assertRaises(urllib.error.HTTPError):
            list(fetch_hourly_stream(datetime.date(2026, 11, 1), 1))

        self.assertEqual(mock_urlopen.call_count, 4)
        self.assertEqual([c.args[0] for c in mock_sleep.call_args_list], [5, 15, 45])

    @patch("octopulse.ingestion.stream_engine.time.sleep")
    @patch("octopulse.ingestion.stream_engine.urllib.request.urlopen")
    def test_mid_stream_error_is_not_retried_inside_stream(self, mock_urlopen, mock_sleep):
        """After bytes were yielded, a drop must propagate instead of restarting the stream."""
        response = MagicMock()
        response.__enter__.return_value = response
        response.read.side_effect = [b"abc", urllib.error.URLError("connection reset")]
        mock_urlopen.return_value = response

        stream = fetch_hourly_stream(datetime.date(2026, 11, 1), 2)
        self.assertEqual(next(stream), b"abc")
        with self.assertRaises(urllib.error.URLError):
            next(stream)

        self.assertEqual(mock_urlopen.call_count, 1)
        mock_sleep.assert_not_called()
