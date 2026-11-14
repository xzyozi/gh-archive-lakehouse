"""Enrichment pipeline orchestration helpers for OctoPulse (DD-002).

Selects top daily WatchEvent repositories from BigQuery Bronze excluding
repositories updated within the last 7 days, fetches metadata via GitHub
GraphQL API, and streams JSONL directly to GCS with zero local disk I/O.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import datetime
import json
import logging
import os
from typing import Any

from octopulse.enrichment.graphql_client import (
    EnrichmentConfigError,
    GitHubGraphQLClient,
)

logger = logging.getLogger(__name__)

MAX_BYTES_BILLED = 1_073_741_824  # 1 GB scan guardrail (BD-001 3.2)
DEFAULT_TOP_N = 200
DEFAULT_REFRESH_DAYS = 7


@dataclass(frozen=True)
class EnrichmentSummaryDTO:
    """Summary of daily enrichment execution."""

    target_date: datetime.date
    gcs_uri: str
    candidate_count: int
    fetched_count: int
    skipped_count: int
    total_bytes_uploaded: int


def build_enrichment_blob_path(target_date: datetime.date) -> str:
    """Generate canonical GCS object path for enrichment JSONL (DD-002 5)."""
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")
    date_compact = target_date.strftime("%Y%m%d")
    return f"enrichment/{year}/{month}/{day}/repo_metadata_{date_compact}.jsonl"


def build_candidate_query(project_id: str, dataset_prefix: str) -> str:
    """Build parameterized BigQuery SQL selecting top repositories to enrich (DD-002 3.1, 3.2)."""
    bronze_table = f"`{project_id}.{dataset_prefix}_bronze.stg_github_events`"
    silver_table = f"`{project_id}.{dataset_prefix}_silver.repo_metadata`"
    return f"""
WITH star_ranking AS (
    SELECT
        repo_name,
        COUNT(DISTINCT id) AS daily_stars
    FROM {bronze_table}
    WHERE event_date = @target_date
      AND type = 'WatchEvent'
    GROUP BY repo_name
),
recently_updated AS (
    SELECT
        repo_name
    FROM {silver_table}
    WHERE metadata_updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @refresh_days DAY)
)
SELECT
    r.repo_name
FROM star_ranking AS r
LEFT JOIN recently_updated AS u
  ON r.repo_name = u.repo_name
WHERE u.repo_name IS NULL
ORDER BY r.daily_stars DESC, r.repo_name ASC
LIMIT @limit
""".strip()


def build_merge_sql(project_id: str, dataset_prefix: str) -> str:
    """Build SCD Type 1 MERGE statement from Bronze staging to Silver repo_metadata (DD-002 5)."""
    target_table = f"`{project_id}.{dataset_prefix}_silver.repo_metadata`"
    staging_table = f"`{project_id}.{dataset_prefix}_bronze.stg_repo_metadata`"
    return f"""
MERGE {target_table} AS T
USING {staging_table} AS S
ON T.repo_name = S.repo_name
WHEN MATCHED THEN
    UPDATE SET
        primary_language = S.primary_language,
        topics = S.topics,
        description = S.description,
        repo_created_at = S.repo_created_at,
        total_stars = S.total_stars,
        total_forks = S.total_forks,
        metadata_updated_at = S.metadata_updated_at
WHEN NOT MATCHED THEN
    INSERT (
        repo_name,
        primary_language,
        topics,
        description,
        repo_created_at,
        total_stars,
        total_forks,
        metadata_updated_at
    )
    VALUES (
        S.repo_name,
        S.primary_language,
        S.topics,
        S.description,
        S.repo_created_at,
        S.total_stars,
        S.total_forks,
        S.metadata_updated_at
    )
""".strip()


def select_candidate_repositories(
    bq_client: Any,
    project_id: str,
    dataset_prefix: str,
    target_date: datetime.date,
    *,
    limit: int = DEFAULT_TOP_N,
    refresh_days: int = DEFAULT_REFRESH_DAYS,
) -> list[str]:
    """Query BigQuery Bronze for top uncached WatchEvent repositories."""
    from google.cloud import bigquery  # lazy import: not required in lightweight unit test env

    sql = build_candidate_query(project_id, dataset_prefix)
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=[
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
            bigquery.ScalarQueryParameter("refresh_days", "INT64", refresh_days),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )
    rows = bq_client.query(sql, job_config=job_config).result()
    return [row["repo_name"] for row in rows]


def upload_records_to_gcs(
    records: Sequence[dict[str, Any]],
    bucket_name: str,
    blob_path: str,
    *,
    gcs_client: Any | None = None,
) -> int:
    """Stream enrichment records as NDJSON directly to GCS without local disk writes."""
    if gcs_client is None:
        from google.cloud import storage

        gcs_client = storage.Client()

    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    total_bytes = 0
    with blob.open("w", encoding="utf-8", content_type="application/x-ndjson") as writer:
        for record in records:
            line = json.dumps(record, ensure_ascii=False) + "\n"
            writer.write(line)
            total_bytes += len(line.encode("utf-8"))

    return total_bytes


def run_daily_enrichment(
    target_date: datetime.date,
    *,
    gcs_bucket_name: str,
    project_id: str,
    dataset_prefix: str = "octopulse",
    github_token: str | None = None,
    limit: int = DEFAULT_TOP_N,
    refresh_days: int = DEFAULT_REFRESH_DAYS,
    bq_client: Any | None = None,
    gcs_client: Any | None = None,
    graphql_client: GitHubGraphQLClient | None = None,
) -> EnrichmentSummaryDTO:
    """Execute daily repository metadata enrichment end-to-end (GCS staging output)."""
    token = github_token if github_token is not None else os.environ.get("GITHUB_TOKEN", "")
    if graphql_client is None:
        graphql_client = GitHubGraphQLClient(token)

    if not project_id or not project_id.strip():
        raise EnrichmentConfigError("GCP_PROJECT_ID is not set; enrichment cannot query BigQuery.")

    if bq_client is None:
        from google.cloud import bigquery

        bq_client = bigquery.Client(project=project_id)

    candidates = select_candidate_repositories(
        bq_client=bq_client,
        project_id=project_id,
        dataset_prefix=dataset_prefix,
        target_date=target_date,
        limit=limit,
        refresh_days=refresh_days,
    )
    logger.info("Selected %d enrichment candidates for %s.", len(candidates), target_date.isoformat())

    fetch_result = graphql_client.fetch_repositories(candidates)

    blob_path = build_enrichment_blob_path(target_date)
    total_bytes = upload_records_to_gcs(
        fetch_result.records,
        bucket_name=gcs_bucket_name,
        blob_path=blob_path,
        gcs_client=gcs_client,
    )
    gcs_uri = f"gs://{gcs_bucket_name}/{blob_path}"
    logger.info(
        "Uploaded %d enrichment records (%d bytes) to %s (skipped=%d).",
        len(fetch_result.records),
        total_bytes,
        gcs_uri,
        fetch_result.skipped_count,
    )

    return EnrichmentSummaryDTO(
        target_date=target_date,
        gcs_uri=gcs_uri,
        candidate_count=len(candidates),
        fetched_count=len(fetch_result.records),
        skipped_count=fetch_result.skipped_count,
        total_bytes_uploaded=total_bytes,
    )
