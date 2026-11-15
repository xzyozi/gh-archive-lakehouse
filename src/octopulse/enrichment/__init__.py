"""GitHub GraphQL enrichment package for OctoPulse (Phase 4 / DD-002)."""

from octopulse.enrichment.graphql_client import (
    EnrichmentConfigError,
    EnrichmentFatalError,
    GitHubGraphQLClient,
    RateLimitInfo,
)
from octopulse.enrichment.pipeline import (
    EnrichmentSummaryDTO,
    build_candidate_query,
    build_enrichment_blob_path,
    build_merge_sql,
    run_daily_enrichment,
)

__all__ = [
    "EnrichmentConfigError",
    "EnrichmentFatalError",
    "EnrichmentSummaryDTO",
    "GitHubGraphQLClient",
    "RateLimitInfo",
    "build_candidate_query",
    "build_enrichment_blob_path",
    "build_merge_sql",
    "run_daily_enrichment",
]
