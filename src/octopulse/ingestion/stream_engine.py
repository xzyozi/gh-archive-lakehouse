"""Stream Engine for In-flight GH Archive Ingestion (Zero-Disk I/O).

This module handles streaming HTTP downloads of GH Archive (.json.gz) files,
on-the-fly decompression using zlib, and extraction of target events (Watch,
Fork, Issues, PullRequest) without writing any temporary files to disk.
"""

from __future__ import annotations

import datetime
import json
import logging
import time
import urllib.error
import urllib.request
import zlib
from collections.abc import Iterator
from typing import Any

logger = logging.getLogger(__name__)

TARGET_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "WatchEvent",
        "ForkEvent",
        "IssuesEvent",
        "PullRequestEvent",
    }
)

DEFAULT_USER_AGENT = "OctoPulse/0.1.0 (CHAOSS-compliant OSS Lakehouse; +https://github.com/xzyozi/gh-archive-lakehouse)"
DEFAULT_CHUNK_SIZE = 65536  # 64KB buffer


def build_gh_archive_url(target_date: datetime.date, hour: int) -> str:
    """Build the download URL for a specific hour of GH Archive."""
    if not 0 <= hour <= 23:
        raise ValueError(f"Hour must be between 0 and 23, got {hour}")
    date_str = target_date.strftime("%Y-%m-%d")
    return f"https://data.gharchive.org/{date_str}-{hour}.json.gz"


def fetch_hourly_stream(
    target_date: datetime.date,
    hour: int,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_retries: int = 3,
    timeout: float = 60.0,
) -> Iterator[bytes]:
    """Fetch compressed byte chunks from GH Archive over HTTP stream.

    Args:
        target_date: The date to fetch.
        hour: The hour (0-23) to fetch.
        chunk_size: The byte chunk size for stream reading.
        max_retries: Maximum retry attempts for transient errors.
        timeout: HTTP request timeout in seconds.

    Yields:
        Byte chunks from the compressed HTTP response.
    """
    url = build_gh_archive_url(target_date, hour)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept-Encoding": "gzip",
        },
    )

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Opening HTTP stream: %s (attempt %d/%d)", url, attempt, max_retries)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
            return
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if isinstance(exc, urllib.error.HTTPError) and exc.code == 404:
                logger.warning("GH Archive file not found (404): %s", url)
                raise
            wait_seconds = 2**attempt
            logger.warning("HTTP error on %s: %s. Retrying in %ds...", url, exc, wait_seconds)
            time.sleep(wait_seconds)

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts") from last_error


def extract_target_events(
    byte_stream: Iterator[bytes],
) -> Iterator[dict[str, Any]]:
    """Decompress gzip chunks in-memory and yield target GitHub events.

    Uses zlib.decompressobj with wbits=31 (16 + MAX_WBITS) for transparent
    gzip streaming decompression without local disk I/O.

    Args:
        byte_stream: Iterator of compressed bytes from HTTP stream.

    Yields:
        Parsed GitHub event dicts matching target types.
    """
    # wbits = 16 + zlib.MAX_WBITS (31) instructs zlib to decode gzip header and trailer
    decompressor = zlib.decompressobj(wbits=31)
    buffer = b""

    for chunk in byte_stream:
        decompressed_data = decompressor.decompress(chunk)
        if not decompressed_data:
            continue

        buffer += decompressed_data
        lines = buffer.split(b"\n")
        # Keep the trailing partial line in buffer
        buffer = lines.pop()

        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as err:
                logger.debug("Skipping malformed JSON line: %s", err)
                continue

            event_type = event.get("type")
            if event_type in TARGET_EVENT_TYPES:
                yield event

    # Flush any remaining decompressed data
    remaining_data = decompressor.flush()
    if remaining_data:
        buffer += remaining_data

    if buffer.strip():
        try:
            event = json.loads(buffer.strip().decode("utf-8"))
            if event.get("type") in TARGET_EVENT_TYPES:
                yield event
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass


def stream_gh_archive_hour(
    target_date: datetime.date,
    hour: int,
) -> Iterator[dict[str, Any]]:
    """High-level generator to stream filtered events for a specific hour."""
    byte_stream = fetch_hourly_stream(target_date, hour)
    yield from extract_target_events(byte_stream)
