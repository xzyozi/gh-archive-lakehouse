"""GitHub GraphQL API client for repository metadata enrichment (DD-002).

Fetches repository metadata in sequential batches of 50 aliases using only
standard-library ``urllib`` with exponential backoff and rate-limit protection.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import datetime
import json
import logging
import time
from typing import Any
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

DEFAULT_GRAPHQL_ENDPOINT = "https://api.github.com/graphql"
DEFAULT_BATCH_SIZE = 50
DEFAULT_REQUEST_INTERVAL_SECONDS = 1.0
DEFAULT_MIN_RATE_LIMIT_REMAINING = 100
DEFAULT_TIMEOUT_SECONDS = 30
DESCRIPTION_MAX_LENGTH = 1000
TOPICS_LIMIT = 20
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 2.0


class EnrichmentConfigError(RuntimeError):
    """Raised when required configuration such as GITHUB_TOKEN is missing."""


class EnrichmentFatalError(RuntimeError):
    """Raised on unrecoverable enrichment errors (auth, rate limit, 5xx)."""


@dataclass(frozen=True)
class RateLimitInfo:
    """GitHub GraphQL rate-limit metadata returned per query."""

    remaining: int
    reset_at: str
    cost: int


@dataclass(frozen=True)
class FetchResult:
    """Aggregated result of batched GraphQL repository fetches."""

    records: list[dict[str, Any]]
    skipped_count: int
    last_rate_limit: RateLimitInfo | None


def _split_repo_name(repo_name: str) -> tuple[str, str] | None:
    """Split ``owner/repo`` into a 2-tuple, or return None if malformed."""
    parts = repo_name.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None
    return parts[0], parts[1]


def build_graphql_query(repos: Sequence[tuple[str, str]]) -> tuple[str, dict[str, str]]:
    """Build a batched GraphQL query and variable map for up to N repositories.

    Uses GraphQL variables (``$owner_i``, ``$name_i``) rather than inline string
    interpolation so repository names cannot alter query syntax.
    """
    var_declarations: list[str] = []
    aliases: list[str] = []
    variables: dict[str, str] = {}

    for idx, (owner, name) in enumerate(repos):
        owner_var = f"owner_{idx}"
        name_var = f"name_{idx}"
        var_declarations.append(f"${owner_var}: String!, ${name_var}: String!")
        aliases.append(
            f"r_{idx}: repository(owner: ${owner_var}, name: ${name_var}) {{\n"
            f"    primaryLanguage {{ name }}\n"
            f"    repositoryTopics(first: {TOPICS_LIMIT}) {{ nodes {{ topic {{ name }} }} }}\n"
            f"    description\n"
            f"    createdAt\n"
            f"    stargazerCount\n"
            f"    forkCount\n"
            f"  }}"
        )
        variables[owner_var] = owner
        variables[name_var] = name

    decl_block = f"({', '.join(var_declarations)})" if var_declarations else ""
    body_lines = ["  rateLimit { remaining resetAt cost }", *[f"  {a}" for a in aliases]]
    query = f"query FetchRepoMetadata{decl_block} {{\n" + "\n".join(body_lines) + "\n}"
    return query, variables


def parse_repository_node(
    repo_name: str,
    node: dict[str, Any],
    updated_at: str,
) -> dict[str, Any]:
    """Convert a single GraphQL ``repository`` object into a BigQuery-ready row."""
    primary_language_obj = node.get("primaryLanguage")
    primary_language = (
        primary_language_obj.get("name")
        if isinstance(primary_language_obj, dict) and isinstance(primary_language_obj.get("name"), str)
        else None
    )

    topics_conn = node.get("repositoryTopics") or {}
    topic_nodes = topics_conn.get("nodes") or []
    topics: list[str] = []
    for item in topic_nodes:
        if isinstance(item, dict):
            topic_obj = item.get("topic")
            if isinstance(topic_obj, dict) and isinstance(topic_obj.get("name"), str) and topic_obj["name"]:
                topics.append(topic_obj["name"])

    raw_description = node.get("description")
    description = (
        raw_description[:DESCRIPTION_MAX_LENGTH]
        if isinstance(raw_description, str) and raw_description != ""
        else ("" if raw_description == "" else None)
    )

    created_at = node.get("createdAt")
    repo_created_at = created_at if isinstance(created_at, str) and created_at else None

    stars = node.get("stargazerCount")
    total_stars = stars if isinstance(stars, int) and not isinstance(stars, bool) else None

    forks = node.get("forkCount")
    total_forks = forks if isinstance(forks, int) and not isinstance(forks, bool) else None

    return {
        "repo_name": repo_name,
        "primary_language": primary_language,
        "topics": topics,
        "description": description,
        "repo_created_at": repo_created_at,
        "total_stars": total_stars,
        "total_forks": total_forks,
        "metadata_updated_at": updated_at,
    }


def _parse_rate_limit(data: dict[str, Any]) -> RateLimitInfo | None:
    """Extract RateLimitInfo from GraphQL response data if present."""
    rl = data.get("rateLimit")
    if not isinstance(rl, dict):
        return None
    remaining = rl.get("remaining")
    reset_at = rl.get("resetAt")
    cost = rl.get("cost")
    if isinstance(remaining, int) and isinstance(reset_at, str) and isinstance(cost, int):
        return RateLimitInfo(remaining=remaining, reset_at=reset_at, cost=cost)
    return None


def _is_fatal_graphql_error(errors: list[Any]) -> str | None:
    """Return an error summary if GraphQL errors indicate an unrecoverable failure.

    Per-repository ``NOT_FOUND`` errors are partial failures (the corresponding
    alias returns ``null``) and are handled by skipping that repository.
    """
    fatal_types = {"RATE_LIMITED", "FORBIDDEN", "UNAUTHORIZED"}
    for err in errors:
        if not isinstance(err, dict):
            continue
        err_type = str(err.get("type", "")).upper()
        message = str(err.get("message", ""))
        if err_type in fatal_types or "rate limit" in message.lower():
            return f"GraphQL fatal error ({err_type or 'UNKNOWN'}): {message}"
    return None


class GitHubGraphQLClient:
    """Sequential batched client for GitHub GraphQL API v4."""

    def __init__(
        self,
        token: str,
        *,
        endpoint: str = DEFAULT_GRAPHQL_ENDPOINT,
        batch_size: int = DEFAULT_BATCH_SIZE,
        request_interval_seconds: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
        min_rate_limit_remaining: int = DEFAULT_MIN_RATE_LIMIT_REMAINING,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
        initial_backoff_seconds: float = INITIAL_BACKOFF_SECONDS,
        urlopen_fn: Callable[..., Any] | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        now_fn: Callable[[], datetime.datetime] | None = None,
    ) -> None:
        if not token or not token.strip():
            raise EnrichmentConfigError("GITHUB_TOKEN is not set; enrichment cannot authenticate.")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if request_interval_seconds < 1.0:
            raise ValueError("request_interval_seconds must be >= 1.0 per DD-002")

        self._token = token.strip()
        self._endpoint = endpoint
        self._batch_size = batch_size
        self._request_interval_seconds = request_interval_seconds
        self._min_rate_limit_remaining = min_rate_limit_remaining
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._initial_backoff_seconds = initial_backoff_seconds
        self._urlopen = urlopen_fn or urllib.request.urlopen
        self._sleep = sleep_fn
        self._now = now_fn or (lambda: datetime.datetime.now(datetime.timezone.utc))

    def _post_query(self, query: str, variables: dict[str, str]) -> dict[str, Any]:
        """Execute a single GraphQL POST request with exponential backoff on transient failures."""
        payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")

        for attempt in range(self._max_retries + 1):
            request = urllib.request.Request(
                self._endpoint,
                data=payload,
                method="POST",
                headers={
                    "Authorization": f"bearer {self._token}",
                    "Content-Type": "application/json",
                    "User-Agent": "octopulse-enrichment",
                },
            )
            try:
                with self._urlopen(request, timeout=self._timeout_seconds) as response:
                    raw_body = response.read().decode("utf-8")
                parsed = json.loads(raw_body)
                if not isinstance(parsed, dict):
                    raise EnrichmentFatalError("Unexpected non-object JSON response from GitHub GraphQL API.")
                return parsed
            except urllib.error.HTTPError as exc:
                status = exc.code
                if status == 401:
                    logger.error(
                        "GitHub GraphQL API returned HTTP 401 (Unauthorized). "
                        "GITHUB_TOKEN may be expired or invalid."
                    )
                    raise EnrichmentFatalError(
                        "GitHub GraphQL API authentication failed (HTTP 401): token may be expired or invalid."
                    ) from None
                if status in (403, 429) or status >= 500:
                    if attempt < self._max_retries:
                        delay = self._initial_backoff_seconds * (2**attempt)
                        logger.warning(
                            "GitHub GraphQL API returned HTTP %d (attempt %d/%d); retrying in %.1fs.",
                            status,
                            attempt + 1,
                            self._max_retries + 1,
                            delay,
                        )
                        self._sleep(delay)
                        continue
                    raise EnrichmentFatalError(
                        f"GitHub GraphQL API failed with HTTP {status} after {self._max_retries + 1} attempts."
                    ) from None
                raise EnrichmentFatalError(
                    f"GitHub GraphQL API failed with non-retryable HTTP {status}."
                ) from None
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                if attempt < self._max_retries:
                    delay = self._initial_backoff_seconds * (2**attempt)
                    logger.warning(
                        "Transient error calling GitHub GraphQL API (%s, attempt %d/%d); retrying in %.1fs.",
                        type(exc).__name__,
                        attempt + 1,
                        self._max_retries + 1,
                        delay,
                    )
                    self._sleep(delay)
                    continue
                raise EnrichmentFatalError(
                    f"GitHub GraphQL API request failed after {self._max_retries + 1} attempts "
                    f"({type(exc).__name__})."
                ) from None

        raise EnrichmentFatalError("Exhausted retries unexpectedly.")  # pragma: no cover

    def fetch_repositories(self, repo_names: Sequence[str]) -> FetchResult:
        """Fetch metadata sequentially in batches for the given ``owner/repo`` names."""
        valid_pairs: list[tuple[str, str, str]] = []
        skipped_count = 0
        for repo_name in repo_names:
            split = _split_repo_name(repo_name)
            if split is None:
                logger.warning("Skipping malformed repository name: %s", repo_name)
                skipped_count += 1
                continue
            valid_pairs.append((repo_name, split[0], split[1]))

        records: list[dict[str, Any]] = []
        last_rate_limit: RateLimitInfo | None = None

        total_batches = (len(valid_pairs) + self._batch_size - 1) // self._batch_size
        for batch_index in range(total_batches):
            if batch_index > 0:
                self._sleep(self._request_interval_seconds)

            start = batch_index * self._batch_size
            batch = valid_pairs[start : start + self._batch_size]
            query, variables = build_graphql_query([(owner, name) for _, owner, name in batch])
            response = self._post_query(query, variables)

            errors = response.get("errors")
            if isinstance(errors, list) and errors:
                fatal_msg = _is_fatal_graphql_error(errors)
                if fatal_msg:
                    raise EnrichmentFatalError(fatal_msg)

            data = response.get("data")
            if not isinstance(data, dict):
                raise EnrichmentFatalError("GitHub GraphQL response missing 'data' object.")

            last_rate_limit = _parse_rate_limit(data)
            if last_rate_limit is not None:
                logger.info(
                    "GitHub GraphQL batch %d/%d complete (cost=%d, remaining=%d, resetAt=%s).",
                    batch_index + 1,
                    total_batches,
                    last_rate_limit.cost,
                    last_rate_limit.remaining,
                    last_rate_limit.reset_at,
                )
                if last_rate_limit.remaining < self._min_rate_limit_remaining:
                    raise EnrichmentFatalError(
                        f"GitHub GraphQL rate limit remaining ({last_rate_limit.remaining}) "
                        f"dropped below threshold ({self._min_rate_limit_remaining})."
                    )

            updated_at = self._now().astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
            for idx, (repo_name, _, _) in enumerate(batch):
                node = data.get(f"r_{idx}")
                if not isinstance(node, dict):
                    skipped_count += 1
                    continue
                records.append(parse_repository_node(repo_name, node, updated_at))

        if skipped_count:
            logger.info("Skipped %d repositories that returned null or malformed names.", skipped_count)

        return FetchResult(
            records=records,
            skipped_count=skipped_count,
            last_rate_limit=last_rate_limit,
        )
