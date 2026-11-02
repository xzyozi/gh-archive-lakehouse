"""GCS Streaming Uploader for Daily Ingestion Pipeline (Zero-Disk I/O).

Handles streaming of decompressed/filtered GH Archive events directly to GCS
without intermediate local file writes, conforming to DD-001 and DS-001.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import datetime
import json
import logging
import time
from typing import TYPE_CHECKING, Any, Protocol
import urllib.error

from octopulse.ingestion.stream_engine import (
    extract_target_events,
    fetch_hourly_stream,
)

if TYPE_CHECKING:
    from google.cloud import storage

logger = logging.getLogger(__name__)

HOUR_MAX_RETRIES = 2  # whole-hour retries for mid-stream failures / truncated gzip
HOUR_RETRY_WAIT_SECONDS = 15


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
    skipped_events: int = 0  # events dropped because a REQUIRED column was missing


def build_gcs_blob_path(target_date: datetime.date) -> str:
    """Generate canonical GCS object path conforming to DS-001.

    Format: raw/YYYY/MM/DD/events_YYYYMMDD.jsonl
    """
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")
    date_compact = target_date.strftime("%Y%m%d")
    return f"raw/{year}/{month}/{day}/events_{date_compact}.jsonl"


def _non_empty_str(value: Any) -> str | None:
    """Return the value if it is a non-empty string, otherwise None."""
    return value if isinstance(value, str) and value else None


def project_event(event: dict[str, Any]) -> dict[str, Any] | None:
    """Flatten a raw GH Archive event into a Bronze row (DS-001 2.2 / 2.3).

    The output keys match the BigQuery Bronze table ``stg_github_events`` one to
    one, so a load job can ingest the JSONL as is. The raw PullRequestEvent payload
    embeds the whole pull request (about 23KB per event); keeping only the
    projected payload keeps GCS/BigQuery inside the Always Free tier.

    Returns None when a REQUIRED column (id, type, actor_login, repo_name,
    created_at) is missing or created_at is not an ISO 8601 date-time. One such row
    would fail the whole BigQuery load job, so the caller must skip it.
    """
    actor = event.get("actor") or {}
    repo = event.get("repo") or {}
    payload = event.get("payload") or {}

    event_id = _non_empty_str(event.get("id"))
    event_type = _non_empty_str(event.get("type"))
    actor_login = _non_empty_str(actor.get("login"))
    repo_name = _non_empty_str(repo.get("name"))
    created_at = _non_empty_str(event.get("created_at"))
    if not (event_id and event_type and actor_login and repo_name and created_at):
        return None

    try:
        event_date = datetime.date.fromisoformat(created_at[:10]).isoformat()
    except ValueError:
        return None

    slim_payload: dict[str, Any] = {"action": payload.get("action")}
    if event_type == "PullRequestEvent":
        slim_payload["merged"] = (payload.get("pull_request") or {}).get("merged")
    elif event_type == "CreateEvent":
        slim_payload["ref_type"] = payload.get("ref_type")

    repo_id = repo.get("id")
    return {
        "id": event_id,
        "type": event_type,
        "actor_login": actor_login,
        "repo_id": repo_id if isinstance(repo_id, int) and not isinstance(repo_id, bool) else None,
        "repo_name": repo_name,
        "payload": slim_payload,
        "created_at": created_at,
        "event_date": event_date,
    }


def _collect_hour_lines(target_date: datetime.date, hour: int) -> tuple[list[str], int]:
    """Fetch one hour and return its JSONL lines and the number of skipped events.

    The hour is retried from scratch (new HTTP stream and new decompressor), because
    resuming a half-consumed gzip stream is not possible. A 404 is not retried here:
    the connection layer already retried it (DD-001 3.3). Events lacking a REQUIRED
    column are skipped and counted (project_event returned None).
    """
    attempt = 0
    while True:
        try:
            byte_stream = fetch_hourly_stream(target_date, hour)
            lines: list[str] = []
            skipped = 0
            for event in extract_target_events(byte_stream):
                row = project_event(event)
                if row is None:
                    skipped += 1
                    continue
                lines.append(json.dumps(row, ensure_ascii=False) + "\n")
            return lines, skipped
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise
            error: Exception = exc
        except Exception as exc:
            error = exc
        attempt += 1
        if attempt > HOUR_MAX_RETRIES:
            raise error
        logger.warning("  Hour %02d attempt %d failed: %s. Retrying the hour...", hour, attempt, error)
        time.sleep(HOUR_RETRY_WAIT_SECONDS)


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
    total_skipped = 0
    processed_hours: list[int] = []
    failed_hours: list[int] = []

    logger.info("Starting daily streaming ingestion for %s (destination: %s)", target_date, destination_uri)

    for hour in hours:
        try:
            lines, skipped = _collect_hour_lines(target_date, hour)
        except Exception as exc:
            logger.warning("  Hour %02d failed: %s", hour, exc)
            failed_hours.append(hour)
            continue

        # Write only after the whole hour was extracted successfully, so a failed
        # hour never leaves partial or duplicated rows in the destination.
        for line in lines:
            writer.write(line)
            total_bytes += len(line.encode("utf-8"))
        total_events += len(lines)
        total_skipped += skipped
        processed_hours.append(hour)
        logger.info("  Hour %02d: Extracted %d target events (skipped %d)", hour, len(lines), skipped)

    if not processed_hours:
        status = "FAILED"
    elif failed_hours:
        status = "PARTIAL_SUCCESS"
    else:
        status = "SUCCESS"

    if total_skipped:
        logger.warning(
            "Skipped %d events lacking REQUIRED columns (id/type/actor_login/repo_name/created_at)",
            total_skipped,
        )
    logger.info(
        "Daily ingestion finished: status=%s, total_events=%d, total_bytes=%d, skipped_events=%d",
        status,
        total_events,
        total_bytes,
        total_skipped,
    )

    return IngestionSummaryDTO(
        target_date=target_date,
        gcs_uri=destination_uri,
        total_events_extracted=total_events,
        total_bytes_uploaded=total_bytes,
        processed_hours=processed_hours,
        failed_hours=failed_hours,
        status=status,
        skipped_events=total_skipped,
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
