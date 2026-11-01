"""Airflow DAG: Daily GH Archive In-flight Ingestion to GCS (Zero-Disk I/O).

Schedules daily streaming ingestion from GH Archive to Google Cloud Storage
without local disk storage, conforming to OctoPulse basic design (BD-001).
"""

from __future__ import annotations

import datetime
import os
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default bucket name from environment variable
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "octopulse-data-lake")


def execute_daily_ingestion(ds: str, **context: Any) -> dict[str, Any]:
    """Execute streaming ingestion for the logical execution date (ds).

    Args:
        ds: Airflow logical execution date string in YYYY-MM-DD format.
        context: Airflow task execution context.

    Returns:
        Dictionary representation of IngestionSummaryDTO.
    """
    from octopulse.ingestion.gcs_uploader import run_daily_ingestion

    target_date = datetime.date.fromisoformat(ds)
    bucket_name = os.environ.get("GCS_BUCKET_NAME", GCS_BUCKET_NAME)

    summary = run_daily_ingestion(
        target_date=target_date,
        gcs_bucket_name=bucket_name,
    )

    return {
        "target_date": summary.target_date.isoformat(),
        "gcs_uri": summary.gcs_uri,
        "total_events_extracted": summary.total_events_extracted,
        "total_bytes_uploaded": summary.total_bytes_uploaded,
        "processed_hours": summary.processed_hours,
        "failed_hours": summary.failed_hours,
        "status": summary.status,
    }


default_args = {
    "owner": "octopulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="gh_stream_ingest",
    default_args=default_args,
    description="Stream GH Archive daily 24h events to GCS with zero local disk write",
    schedule_interval="@daily",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["octopulse", "ingestion", "gh-archive", "gcs"],
) as dag:
    ingest_task = PythonOperator(
        task_id="stream_gh_archive_to_gcs",
        python_callable=execute_daily_ingestion,
    )

    ingest_task
