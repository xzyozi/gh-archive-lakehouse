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
RETRY_BACKOFF_SECONDS: tuple[int, ...] = (5, 15, 45)  # DD-001 3.3


def build_gh_archive_url(target_date: datetime.date, hour: int) -> str:
    """Build the download URL for a specific hour of GH Archive."""
    if not 0 <= hour <= 23:
        raise ValueError(f"Hour must be between 0 and 23, got {hour}")
    date_str = target_date.strftime("%Y-%m-%d")
    return f"https://data.gharchive.org/{date_str}-{hour}.json.gz"


def _open_with_retry(req: urllib.request.Request, timeout: float, max_retries: int, max_404_retries: int) -> Any:
    """Open the HTTP response, retrying only the connection phase (DD-001 3.3).

    Retrying is limited to before the first byte is consumed. A retry after
    streaming has started would feed a fresh gzip stream into a half-consumed
    decompressor, so mid-stream failures are propagated to the caller instead.
    """
    url = req.full_url
    retries = 0
    retries_404 = 0
    while True:
        try:
            return urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                if retries_404 >= max_404_retries:
                    logger.warning("GH Archive file not found (404): %s", url)
                    raise
                retries_404 += 1
            elif exc.code >= 500 or exc.code == 429:
                if retries >= max_retries:
                    raise
                retries += 1
            else:
                raise  # Other 4xx are not transient
            wait_seconds = RETRY_BACKOFF_SECONDS[min(retries + retries_404 - 1, len(RETRY_BACKOFF_SECONDS) - 1)]
            logger.warning("HTTP %d on %s. Retrying in %ds...", exc.code, url, wait_seconds)
            time.sleep(wait_seconds)
        except (urllib.error.URLError, TimeoutError) as exc:
            if retries >= max_retries:
                raise
            retries += 1
            wait_seconds = RETRY_BACKOFF_SECONDS[min(retries - 1, len(RETRY_BACKOFF_SECONDS) - 1)]
            logger.warning("Network error on %s: %s. Retrying in %ds...", url, exc, wait_seconds)
            time.sleep(wait_seconds)


def fetch_hourly_stream(
    target_date: datetime.date,
    hour: int,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_retries: int = 3,
    timeout: float = 60.0,
    max_404_retries: int = 2,
) -> Iterator[bytes]:
    """Fetch compressed byte chunks from GH Archive over HTTP stream.

    Args:
        target_date: The date to fetch.
        hour: The hour (0-23) to fetch.
        chunk_size: The byte chunk size for stream reading.
        max_retries: Maximum retries (after the first attempt) for 5xx/network errors.
        timeout: HTTP request timeout in seconds.
        max_404_retries: Maximum retries for HTTP 404 (file not yet published).

    Yields:
        Byte chunks from the compressed HTTP response.

    Raises:
        urllib.error.HTTPError / URLError / TimeoutError: when the connection cannot be
            established within the retry budget, or the connection drops mid-stream.
    """
    url = build_gh_archive_url(target_date, hour)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept-Encoding": "gzip",
        },
    )

    logger.info("Opening HTTP stream: %s", url)
    with _open_with_retry(req, timeout, max_retries, max_404_retries) as response:
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            yield chunk


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

    # A stream that ends before the gzip trailer was a truncated transfer; silently
    # accepting it would drop the rest of the hour while reporting SUCCESS.
    if not decompressor.eof:
        raise ValueError("Truncated gzip stream: input ended before the gzip trailer")

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
