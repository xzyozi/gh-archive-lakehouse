"""Airflow DAG: Daily GH Archive Ingestion -> GCS -> Bronze -> Enrichment -> dbt (Silver/Gold).

Schedules daily streaming ingestion from GH Archive to Google Cloud Storage
without local disk storage, loads it into BigQuery Bronze, enriches top 200
repositories via GitHub GraphQL API (DD-002), then runs the dbt medallion
models (Staging -> Silver -> Gold) through Astronomer Cosmos (BD-001 C-ORCHESTRATE).
"""

from __future__ import annotations

import datetime
import os
from pathlib import Path
from typing import Any

from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior

from airflow import DAG

# Default bucket, project, and dataset prefix from environment variables
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "octopulse-lakehouse")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
DATASET_PREFIX = os.environ.get("GCP_DATASET_PREFIX", "octopulse")

# dbt (Cosmos) settings. Paths match airflow/docker-compose.yaml mounts and airflow/Dockerfile.
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
# Cosmos passes the parent directory of this file to dbt as --profiles-dir, so the file must be named profiles.yml.
DBT_PROFILES_FILE = Path(os.environ.get("DBT_PROFILES_FILE", "/opt/airflow/dbt_profiles/profiles.yml"))
# dbt-core / dbt-bigquery live in a dedicated venv to avoid dependency conflicts with Airflow.
DBT_EXECUTABLE_PATH = os.environ.get("DBT_EXECUTABLE_PATH", "/opt/airflow/dbt_venv/bin/dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "dev")


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

    result = {
        "target_date": summary.target_date.isoformat(),
        "gcs_uri": summary.gcs_uri,
        "total_events_extracted": summary.total_events_extracted,
        "total_bytes_uploaded": summary.total_bytes_uploaded,
        "processed_hours": summary.processed_hours,
        "failed_hours": summary.failed_hours,
        "status": summary.status,
        "skipped_events": summary.skipped_events,
    }

    # A partial day must not look like success: downstream aggregation would silently
    # miss the failed hours. Failing here lets Airflow retry (the whole day is
    # re-uploaded to the same object, so it stays idempotent). This also covers the
    # last hour (23:00 UTC) not being published yet when the run starts at 00:00 UTC.
    if summary.status != "SUCCESS":
        raise RuntimeError(
            f"Ingestion {summary.status} for {summary.target_date}: failed_hours={summary.failed_hours}"
        )
    return result


def execute_daily_enrichment(ds: str, **context: Any) -> dict[str, Any]:
    """Fetch GitHub GraphQL metadata for top 200 repositories and upload JSONL to GCS (DD-002).

    Never returns or logs GITHUB_TOKEN.
    """
    from octopulse.enrichment.pipeline import run_daily_enrichment

    target_date = datetime.date.fromisoformat(ds)
    bucket_name = os.environ.get("GCS_BUCKET_NAME", GCS_BUCKET_NAME)
    project_id = os.environ.get("GCP_PROJECT_ID", GCP_PROJECT_ID)
    dataset_prefix = os.environ.get("GCP_DATASET_PREFIX", DATASET_PREFIX)

    summary = run_daily_enrichment(
        target_date=target_date,
        gcs_bucket_name=bucket_name,
        project_id=project_id,
        dataset_prefix=dataset_prefix,
    )

    return {
        "target_date": summary.target_date.isoformat(),
        "gcs_uri": summary.gcs_uri,
        "candidate_count": summary.candidate_count,
        "fetched_count": summary.fetched_count,
        "skipped_count": summary.skipped_count,
        "total_bytes_uploaded": summary.total_bytes_uploaded,
    }


def check_bronze_load_succeeded(**context: Any) -> bool:
    """Allow dbt to run even if Enrichment failed, but block dbt when Bronze load failed (DD-002 6.5)."""
    dag_run = context.get("dag_run")
    if dag_run is None:
        return False
    bronze_ti = dag_run.get_task_instance("load_gcs_to_bigquery_bronze")
    if bronze_ti is None:
        return False
    return bronze_ti.state == "success"


default_args = {
    "owner": "octopulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=10),
    "execution_timeout": datetime.timedelta(hours=1),
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

    # Phase 2: Load daily flat JSONL from GCS to BigQuery Bronze (stg_github_events)
    # Uses partition decorator ($YYYYMMDD) and WRITE_TRUNCATE to ensure idempotent partition-level overwrites.
    load_bronze_task = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery_bronze",
        bucket=GCS_BUCKET_NAME,
        source_objects=[
            "raw/{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:8] }}/events_{{ ds_nodash }}.jsonl"
        ],
        destination_project_dataset_table=(
            f"{DATASET_PREFIX}_bronze.stg_github_events${{{{ ds_nodash }}}}"
        ),
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
    )

    # Phase 4: GitHub GraphQL API Enrichment -> GCS -> Bronze Staging -> Silver MERGE (DD-002 5)
    with TaskGroup(group_id="enrich_repo_metadata") as enrich_repo_metadata:
        fetch_repo_metadata_task = PythonOperator(
            task_id="fetch_repo_metadata_to_gcs",
            python_callable=execute_daily_enrichment,
        )

        load_repo_metadata_staging_task = GCSToBigQueryOperator(
            task_id="load_repo_metadata_staging",
            bucket=GCS_BUCKET_NAME,
            source_objects=[
                "enrichment/{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:8] }}/"
                "repo_metadata_{{ ds_nodash }}.jsonl"
            ],
            destination_project_dataset_table=f"{DATASET_PREFIX}_bronze.stg_repo_metadata",
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            autodetect=False,
        )

        merge_repo_metadata_task = BigQueryInsertJobOperator(
            task_id="merge_repo_metadata",
            configuration={
                "query": {
                    "query": (
                        f"MERGE `{GCP_PROJECT_ID}.{DATASET_PREFIX}_silver.repo_metadata` AS T "
                        f"USING `{GCP_PROJECT_ID}.{DATASET_PREFIX}_bronze.stg_repo_metadata` AS S "
                        "ON T.repo_name = S.repo_name "
                        "WHEN MATCHED THEN UPDATE SET "
                        "primary_language = S.primary_language, "
                        "topics = S.topics, "
                        "description = S.description, "
                        "repo_created_at = S.repo_created_at, "
                        "total_stars = S.total_stars, "
                        "total_forks = S.total_forks, "
                        "metadata_updated_at = S.metadata_updated_at "
                        "WHEN NOT MATCHED THEN INSERT ("
                        "repo_name, primary_language, topics, description, "
                        "repo_created_at, total_stars, total_forks, metadata_updated_at"
                        ") VALUES ("
                        "S.repo_name, S.primary_language, S.topics, S.description, "
                        "S.repo_created_at, S.total_stars, S.total_forks, S.metadata_updated_at)"
                    ),
                    "useLegacySql": False,
                    "maximumBytesBilled": "1073741824",
                }
            },
        )

        fetch_repo_metadata_task >> load_repo_metadata_staging_task >> merge_repo_metadata_task

    # DD-002 6.5: Run dbt even if Enrichment failed, as long as Bronze load succeeded.
    # Placed outside DbtTaskGroup so internal TestBehavior.AFTER_EACH keeps default ALL_SUCCESS.
    verify_bronze_before_dbt = ShortCircuitOperator(
        task_id="verify_bronze_before_dbt",
        python_callable=check_bronze_load_succeeded,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Phase 5: dbt medallion models (Staging -> Silver -> Gold) as a Cosmos task group.
    # - Tests run right after each model (AFTER_EACH) so bad data never reaches downstream models.
    # - run_date is the logical date: re-running a past ds reprocesses only that window (NFR-004 idempotency).
    # - The 1GB scan guardrail (maximum_bytes_billed) comes from the mounted profiles.yml (BD-001 3.2).
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="octopulse",
            target_name=DBT_TARGET,
            profiles_yml_filepath=DBT_PROFILES_FILE,
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            dbt_executable_path=DBT_EXECUTABLE_PATH,
            dbt_deps=False,
        ),
        operator_args={
            "install_deps": False,
            "vars": {"run_date": "{{ ds }}"},
        },
        # Deterministic failures (e.g. maximum_bytes_billed exceeded, failed dbt test) will not succeed on retry,
        # so retry less than the ingestion tasks, which hit flaky external services.
        default_args={"retries": 1},
    )

    ingest_task >> load_bronze_task >> enrich_repo_metadata >> verify_bronze_before_dbt >> dbt_transform
