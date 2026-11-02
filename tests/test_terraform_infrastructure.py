"""Terraform インフラ構成および契約の単体テスト (Phase 1 / Issue #22).

BD-001 (基本設計書) および DS-001 (データ構造仕様書) の非機能要件・契約が
Terraform コード上で確実に定義されているかを検証する。
"""

from __future__ import annotations

from pathlib import Path
import re

TERRAFORM_DIR = Path(__file__).resolve().parent.parent / "terraform"


def test_terraform_files_exist() -> None:
    """必要な Terraform ファイルがすべて存在することを確認。"""
    required_files = [
        "versions.tf",
        "variables.tf",
        "main.tf",
        "storage.tf",
        "bigquery.tf",
        "iam.tf",
        "outputs.tf",
        "terraform.tfvars.example",
        "README.md",
    ]
    for filename in required_files:
        assert (TERRAFORM_DIR / filename).is_file(), f"Missing file: {filename}"


def test_no_hardcoded_service_account_keys() -> None:
    """NFR-002: 固定 JSON キー発行リソース (google_service_account_key) が存在しないこと。"""
    for tf_file in TERRAFORM_DIR.glob("*.tf"):
        content = tf_file.read_text(encoding="utf-8")
        assert "google_service_account_key" not in content, (
            f"Found forbidden resource 'google_service_account_key' in {tf_file.name} (NFR-002 violation)"
        )


def test_region_is_us_central1_default() -> None:
    """Always Free 枠死守のため、デフォルトリージョンが us-central1 であること。"""
    variables_tf = (TERRAFORM_DIR / "variables.tf").read_text(encoding="utf-8")
    assert 'default     = "us-central1"' in variables_tf
    assert 'contains(["us-central1", "us-east1", "us-west1"], var.region)' in variables_tf


def test_storage_lifecycle_is_7_days() -> None:
    """NFR-001: GCS バケットのライフサイクルルールが 7 日で自動削除されること。"""
    storage_tf = (TERRAFORM_DIR / "storage.tf").read_text(encoding="utf-8")
    assert "uniform_bucket_level_access = true" in storage_tf
    assert "enabled = false" in storage_tf  # versioning disabled
    assert "type = \"Delete\"" in storage_tf or 'type      = "Delete"' in storage_tf
    assert "age        = 7" in storage_tf or "age = 7" in storage_tf


def test_bigquery_bronze_table_schema() -> None:
    """DS-001 2.3: stg_github_events のスキーマが仕様書と一致すること。"""
    bigquery_tf = (TERRAFORM_DIR / "bigquery.tf").read_text(encoding="utf-8")

    # タイムパーティショニングとクラスタリング
    assert 'field         = "event_date"' in bigquery_tf
    assert 'clustering = ["repo_name", "type"]' in bigquery_tf

    # 各フィールドブロックから name, type, mode を抽出
    fields = re.findall(
        r'name\s*=\s*"([^"]+)".*?type\s*=\s*"([^"]+)".*?mode\s*=\s*"([^"]+)"',
        bigquery_tf,
        re.DOTALL,
    )
    schema_dict = {name: (dtype, mode) for name, dtype, mode in fields}

    expected_fields = {
        "id": ("STRING", "REQUIRED"),
        "type": ("STRING", "REQUIRED"),
        "actor_login": ("STRING", "REQUIRED"),
        "repo_id": ("INT64", "NULLABLE"),
        "repo_name": ("STRING", "REQUIRED"),
        "payload": ("JSON", "REQUIRED"),
        "created_at": ("TIMESTAMP", "REQUIRED"),
        "event_date": ("DATE", "REQUIRED"),
    }

    assert schema_dict == expected_fields, f"Schema mismatch: {schema_dict}"


def test_workload_identity_federation_configured() -> None:
    """BD-001 3.2: GitHub Actions OIDC WIF が適切に定義されていること。"""
    iam_tf = (TERRAFORM_DIR / "iam.tf").read_text(encoding="utf-8")
    assert "google_iam_workload_identity_pool" in iam_tf
    assert "google_iam_workload_identity_pool_provider" in iam_tf
    assert "https://token.actions.githubusercontent.com" in iam_tf
    assert "roles/iam.workloadIdentityUser" in iam_tf
    assert "assertion.repository ==" in iam_tf
