"""Unit tests for GitHub GraphQL Enrichment (Phase 4 / Issue #38 / DD-002)."""

from __future__ import annotations

import datetime
import io
import json
from typing import Any
import urllib.error

import pytest

from octopulse.enrichment.graphql_client import (
    DESCRIPTION_MAX_LENGTH,
    EnrichmentConfigError,
    EnrichmentFatalError,
    GitHubGraphQLClient,
    build_graphql_query,
    parse_repository_node,
)
from octopulse.enrichment.pipeline import (
    build_candidate_query,
    build_enrichment_blob_path,
    build_merge_sql,
    run_daily_enrichment,
    upload_records_to_gcs,
)


class _FakeHTTPResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> _FakeHTTPResponse:
        return self

    def __exit__(self, *args: Any) -> None:
        return None


class _ClosingCaptureStringIO(io.StringIO):
    def __init__(self, on_close: Any) -> None:
        super().__init__()
        self._on_close = on_close

    def close(self) -> None:
        self._on_close(self.getvalue())
        super().close()


class _FakeBlob:
    def __init__(self) -> None:
        self.written_text = ""
        self.content_type: str | None = None

    def open(self, mode: str, *, encoding: str = "utf-8", content_type: str | None = None) -> _ClosingCaptureStringIO:
        assert mode == "w"
        self.content_type = content_type

        def _capture(value: str) -> None:
            self.written_text = value

        return _ClosingCaptureStringIO(_capture)


class _FakeBucket:
    def __init__(self) -> None:
        self.blobs: dict[str, _FakeBlob] = {}

    def blob(self, path: str) -> _FakeBlob:
        blob = _FakeBlob()
        self.blobs[path] = blob
        return blob


class _FakeGCSClient:
    def __init__(self) -> None:
        self.buckets: dict[str, _FakeBucket] = {}

    def bucket(self, name: str) -> _FakeBucket:
        if name not in self.buckets:
            self.buckets[name] = _FakeBucket()
        return self.buckets[name]


class _FakeBQQueryJob:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def result(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeBQClient:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.last_sql: str | None = None
        self.last_job_config: Any = None

    def query(self, sql: str, job_config: Any = None) -> _FakeBQQueryJob:
        self.last_sql = sql
        self.last_job_config = job_config
        return _FakeBQQueryJob(self._rows)


def test_missing_github_token_raises_config_error() -> None:
    """DD-002 4: GITHUB_TOKEN 未設定時は API を呼ばず即座に失敗する。"""
    with pytest.raises(EnrichmentConfigError):
        GitHubGraphQLClient("")
    with pytest.raises(EnrichmentConfigError):
        GitHubGraphQLClient("   ")


def test_http_401_logs_expiry_hint_and_never_leaks_token(caplog: pytest.LogCaptureFixture) -> None:
    """DD-002 4: 401 時にトークン期限切れの可能性をログに残し、トークン文字列は漏洩させない。"""
    secret_token = "github_pat_SUPER_SECRET_VALUE"

    def fake_urlopen(request: Any, timeout: int = 30) -> Any:
        raise urllib.error.HTTPError(
            url=request.full_url,
            code=401,
            msg="Unauthorized",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,
        )

    client = GitHubGraphQLClient(
        secret_token,
        urlopen_fn=fake_urlopen,
        sleep_fn=lambda _: None,
    )

    with pytest.raises(EnrichmentFatalError) as exc_info:
        client.fetch_repositories(["octocat/Hello-World"])

    assert "401" in str(exc_info.value)
    assert secret_token not in str(exc_info.value)
    assert secret_token not in caplog.text
    assert "expired" in caplog.text.lower()


def test_5xx_retries_with_exponential_backoff_then_fails() -> None:
    """DD-002 6.2: 5xx は指数バックオフでリトライし、上限超過で致命的エラーにする。"""
    sleep_calls: list[float] = []
    attempts = 0

    def fake_urlopen(request: Any, timeout: int = 30) -> Any:
        nonlocal attempts
        attempts += 1
        raise urllib.error.HTTPError(
            url=request.full_url,
            code=503,
            msg="Service Unavailable",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,
        )

    client = GitHubGraphQLClient(
        "github_pat_test",
        max_retries=2,
        initial_backoff_seconds=1.5,
        urlopen_fn=fake_urlopen,
        sleep_fn=sleep_calls.append,
    )

    with pytest.raises(EnrichmentFatalError) as exc_info:
        client.fetch_repositories(["octocat/Hello-World"])

    assert attempts == 3
    assert sleep_calls == [1.5, 3.0]
    assert "503" in str(exc_info.value)


def test_rate_limit_below_threshold_raises_fatal_error() -> None:
    """DD-002 4: rateLimit.remaining が閾値未満になった場合は中断する。"""

    def fake_urlopen(request: Any, timeout: int = 30) -> _FakeHTTPResponse:
        return _FakeHTTPResponse(
            {
                "data": {
                    "rateLimit": {"remaining": 50, "resetAt": "2026-11-14T02:00:00Z", "cost": 1},
                    "r_0": {
                        "primaryLanguage": {"name": "Python"},
                        "repositoryTopics": {"nodes": []},
                        "description": "hello",
                        "createdAt": "2026-11-01T00:00:00Z",
                        "stargazerCount": 100,
                        "forkCount": 10,
                    },
                }
            }
        )

    client = GitHubGraphQLClient(
        "github_pat_test",
        min_rate_limit_remaining=100,
        urlopen_fn=fake_urlopen,
        sleep_fn=lambda _: None,
    )

    with pytest.raises(EnrichmentFatalError) as exc_info:
        client.fetch_repositories(["octocat/Hello-World"])
    assert "rate limit" in str(exc_info.value).lower()


def test_sequential_batching_partial_null_and_truncation() -> None:
    """DD-002 4 / 6.1: 50件バッチ順次実行、1秒以上間隔、部分nullスキップ、description 1,000文字切り詰め。"""
    sleep_calls: list[float] = []
    requests_seen: list[dict[str, Any]] = []
    fixed_now = datetime.datetime(2026, 11, 14, 1, 0, 0, tzinfo=datetime.timezone.utc)

    repos = [f"owner{i}/repo{i}" for i in range(55)]

    def fake_urlopen(request: Any, timeout: int = 30) -> _FakeHTTPResponse:
        body = json.loads(request.data.decode("utf-8"))
        requests_seen.append(body)
        batch_len = len(body["variables"]) // 2
        data: dict[str, Any] = {
            "rateLimit": {"remaining": 4990, "resetAt": "2026-11-14T02:00:00Z", "cost": 1},
        }
        for idx in range(batch_len):
            if len(requests_seen) == 1 and idx == 1:
                # 2件目は削除済み等で null が返るケース (部分エラー)
                data[f"r_{idx}"] = None
            else:
                data[f"r_{idx}"] = {
                    "primaryLanguage": {"name": "Rust"} if idx % 2 == 0 else None,
                    "repositoryTopics": {"nodes": [{"topic": {"name": "cli"}}, {"topic": {"name": "oss"}}]},
                    "description": "x" * 1500,
                    "createdAt": "2026-11-10T00:00:00Z",
                    "stargazerCount": 120 + idx,
                    "forkCount": 15 + idx,
                }
        return _FakeHTTPResponse(
            {
                "data": data,
                "errors": [{"type": "NOT_FOUND", "path": ["r_1"], "message": "Could not resolve"}],
            }
        )

    client = GitHubGraphQLClient(
        "github_pat_test",
        batch_size=50,
        request_interval_seconds=1.0,
        urlopen_fn=fake_urlopen,
        sleep_fn=sleep_calls.append,
        now_fn=lambda: fixed_now,
    )

    result = client.fetch_repositories(repos)

    assert len(requests_seen) == 2
    assert sleep_calls == [1.0]
    assert len(result.records) == 54
    assert result.skipped_count == 1
    assert result.records[0]["description"] == "x" * DESCRIPTION_MAX_LENGTH
    assert result.records[0]["primary_language"] == "Rust"
    assert result.records[0]["topics"] == ["cli", "oss"]
    assert result.records[0]["metadata_updated_at"] == "2026-11-14T01:00:00Z"


def test_build_graphql_query_and_parse_node() -> None:
    """GraphQL クエリがエイリアス・変数・6項目・rateLimit を含むこと。"""
    query, variables = build_graphql_query([("octocat", "Hello-World")])
    assert "rateLimit { remaining resetAt cost }" in query
    assert "r_0: repository(owner: $owner_0, name: $name_0)" in query
    assert variables == {"owner_0": "octocat", "name_0": "Hello-World"}

    row = parse_repository_node(
        "octocat/Hello-World",
        {
            "primaryLanguage": None,
            "repositoryTopics": {"nodes": []},
            "description": None,
            "createdAt": "2026-11-01T00:00:00Z",
            "stargazerCount": 42,
            "forkCount": 7,
        },
        "2026-11-14T01:00:00Z",
    )
    assert row["primary_language"] is None
    assert row["topics"] == []
    assert row["description"] is None
    assert row["total_stars"] == 42
    assert row["total_forks"] == 7


def test_candidate_query_and_merge_sql_contracts() -> None:
    """DD-002 3 / 5: 候補選定 SQL と MERGE SQL が仕様どおりであること。"""
    candidate_sql = build_candidate_query("my-proj", "octopulse")
    assert "`my-proj.octopulse_bronze.stg_github_events`" in candidate_sql
    assert "`my-proj.octopulse_silver.repo_metadata`" in candidate_sql
    assert "COUNT(DISTINCT id)" in candidate_sql
    assert "type = 'WatchEvent'" in candidate_sql
    assert "@refresh_days" in candidate_sql
    assert "@limit" in candidate_sql

    merge_sql = build_merge_sql("my-proj", "octopulse")
    assert "MERGE `my-proj.octopulse_silver.repo_metadata` AS T" in merge_sql
    assert "USING `my-proj.octopulse_bronze.stg_repo_metadata` AS S" in merge_sql
    assert "ON T.repo_name = S.repo_name" in merge_sql
    assert "WHEN MATCHED THEN" in merge_sql
    assert "WHEN NOT MATCHED THEN" in merge_sql


def test_upload_records_and_run_daily_enrichment_zero_disk() -> None:
    """DD-002 5 / NFR-003: メモリ上から GCS へ NDJSON を直接書き込み、パスとバイト数を返すこと。"""
    target_date = datetime.date(2026, 11, 14)
    assert build_enrichment_blob_path(target_date) == "enrichment/2026/11/14/repo_metadata_20261114.jsonl"

    fake_gcs = _FakeGCSClient()
    written_bytes = upload_records_to_gcs(
        [{"repo_name": "a/b", "topics": ["python"]}],
        bucket_name="octopulse-bucket",
        blob_path=build_enrichment_blob_path(target_date),
        gcs_client=fake_gcs,
    )
    blob = fake_gcs.buckets["octopulse-bucket"].blobs["enrichment/2026/11/14/repo_metadata_20261114.jsonl"]
    assert written_bytes > 0
    assert blob.content_type == "application/x-ndjson"
    assert json.loads(blob.written_text.strip()) == {"repo_name": "a/b", "topics": ["python"]}

    # run_daily_enrichment E2E (BQ + GraphQL + GCS すべてモック)
    fake_bq = _FakeBQClient([{"repo_name": "octocat/Hello-World"}])
    fake_gcs_2 = _FakeGCSClient()
    fixed_now = datetime.datetime(2026, 11, 14, 1, 0, 0, tzinfo=datetime.timezone.utc)
    fake_graphql = GitHubGraphQLClient(
        "github_pat_test",
        urlopen_fn=lambda req, timeout=30: _FakeHTTPResponse(
            {
                "data": {
                    "rateLimit": {"remaining": 4999, "resetAt": "2026-11-14T02:00:00Z", "cost": 1},
                    "r_0": {
                        "primaryLanguage": {"name": "Go"},
                        "repositoryTopics": {"nodes": [{"topic": {"name": "k8s"}}]},
                        "description": "desc",
                        "createdAt": "2026-11-01T00:00:00Z",
                        "stargazerCount": 10,
                        "forkCount": 2,
                    },
                }
            }
        ),
        sleep_fn=lambda _: None,
        now_fn=lambda: fixed_now,
    )

    # google.cloud.bigquery がローカル venv に無い場合でも QueryJobConfig をモックしてテスト可能にする
    import sys
    import types

    fake_bq_mod = types.ModuleType("google.cloud.bigquery")
    fake_bq_mod.QueryJobConfig = lambda **kwargs: kwargs  # type: ignore[attr-defined]
    fake_bq_mod.ScalarQueryParameter = lambda name, typ, val: (name, typ, val)  # type: ignore[attr-defined]
    monkeypatch_restore = sys.modules.get("google.cloud.bigquery")
    sys.modules["google.cloud.bigquery"] = fake_bq_mod
    try:
        summary = run_daily_enrichment(
            target_date,
            gcs_bucket_name="octopulse-bucket",
            project_id="my-proj",
            bq_client=fake_bq,
            gcs_client=fake_gcs_2,
            graphql_client=fake_graphql,
        )
    finally:
        if monkeypatch_restore is None:
            sys.modules.pop("google.cloud.bigquery", None)
        else:
            sys.modules["google.cloud.bigquery"] = monkeypatch_restore

    assert summary.candidate_count == 1
    assert summary.fetched_count == 1
    assert summary.skipped_count == 0
    assert summary.gcs_uri == "gs://octopulse-bucket/enrichment/2026/11/14/repo_metadata_20261114.jsonl"
    assert fake_bq.last_job_config["maximum_bytes_billed"] == 1_073_741_824
