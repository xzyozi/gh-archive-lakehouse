"""Unit tests for the stream engine."""

import datetime
import gzip
import json
import unittest

from octopulse.ingestion.stream_engine import (
    TARGET_EVENT_TYPES,
    build_gh_archive_url,
    extract_target_events,
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

