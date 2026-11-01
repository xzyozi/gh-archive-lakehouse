"""GCS Streaming Uploader for Daily Ingestion Pipeline (Zero-Disk I/O).

Handles streaming of decompressed/filtered GH Archive events directly to GCS
without intermediate local file writes, conforming to DD-001 and DS-001.
"""

from __future__ import annotations

import datetime
import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from octopulse.ingestion.stream_engine import (
    extract_target_events,
    fetch_hourly_stream,
)

if TYPE_CHECKING:
    from google.cloud import storage

logger = logging.getLogger(__name__)


class TextWritable(Protocol):
    """Protocol for text-stream writable destination (like blob.open('w'))."""

    def write(self, s: str) -> int: ...


@dataclass(frozen=True)
class IngestionSummaryDTO:
    """Summary of daily ingestion results conforming to DD-001."""

    target_date: datetime.date
    gcs_uri: str
    total_events_extracted: int
    total_bytes_uploaded: int
    processed_hours: list[int] = field(default_factory=list)
    failed_hours: list[int] = field(default_factory=list)
    status: str = "SUCCESS"  # SUCCESS, PARTIAL_SUCCESS, FAILED


def build_gcs_blob_path(target_date: datetime.date) -> str:
    """Generate canonical GCS object path conforming to DS-001.

    Format: raw/YYYY/MM/DD/events_YYYYMMDD.jsonl
    """
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")
    date_compact = target_date.strftime("%Y%m%d")
    return f"raw/{year}/{month}/{day}/events_{date_compact}.jsonl"


def run_daily_ingestion_to_writer(
    target_date: datetime.date,
    writer: TextWritable,
    destination_uri: str,
    hours: list[int] | None = None,
) -> IngestionSummaryDTO:
    """Stream 24 hours of filtered GH Archive events directly into a text writer.

    Args:
        target_date: Target date to ingest.
        writer: Writable text stream (e.g. GCS blob writer or file-like object).
        destination_uri: Identifier/URI for the destination object.
        hours: Optional list of hours (0-23). Defaults to all 24 hours.

    Returns:
        IngestionSummaryDTO with extraction statistics.
    """
    if hours is None:
        hours = list(range(24))

    total_events = 0
    total_bytes = 0
    processed_hours: list[int] = []
    failed_hours: list[int] = []

    logger.info("Starting daily streaming ingestion for %s (destination: %s)", target_date, destination_uri)

    for hour in hours:
        try:
            byte_stream = fetch_hourly_stream(target_date, hour)
            hourly_event_count = 0
            for event in extract_target_events(byte_stream):
                line = json.dumps(event, ensure_ascii=False) + "\n"
                bytes_written = writer.write(line)
                total_bytes += bytes_written if bytes_written is not None else len(line.encode("utf-8"))
                hourly_event_count += 1

            total_events += hourly_event_count
            processed_hours.append(hour)
            logger.info("  Hour %02d: Extracted %d target events", hour, hourly_event_count)
        except Exception as exc:
            logger.warning("  Hour %02d failed: %s", hour, exc)
            failed_hours.append(hour)

    if not processed_hours:
        status = "FAILED"
    elif failed_hours:
        status = "PARTIAL_SUCCESS"
    else:
        status = "SUCCESS"

    logger.info(
        "Daily ingestion finished: status=%s, total_events=%d, total_bytes=%d",
        status,
        total_events,
        total_bytes,
    )

    return IngestionSummaryDTO(
        target_date=target_date,
        gcs_uri=destination_uri,
        total_events_extracted=total_events,
        total_bytes_uploaded=total_bytes,
        processed_hours=processed_hours,
        failed_hours=failed_hours,
        status=status,
    )


def run_daily_ingestion(
    target_date: datetime.date,
    gcs_bucket_name: str,
    storage_client: storage.Client | None = None,
    hours: list[int] | None = None,
) -> IngestionSummaryDTO:
    """Execute full daily ingestion streaming directly to Google Cloud Storage.

    Conforms to DD-001 F-ING-004 specification.
    """
    if storage_client is None:
        from google.cloud import storage

        storage_client = storage.Client()

    blob_path = build_gcs_blob_path(target_date)
    destination_uri = f"gs://{gcs_bucket_name}/{blob_path}"

    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(blob_path)

    # Open GCS stream in write mode (zero local disk write)
    with blob.open("w", encoding="utf-8") as writer:
        return run_daily_ingestion_to_writer(
            target_date=target_date,
            writer=writer,
            destination_uri=destination_uri,
            hours=hours,
        )
