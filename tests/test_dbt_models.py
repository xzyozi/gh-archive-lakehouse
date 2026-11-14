"""dbt メダリオンモデル構成および契約の単体テスト (Phase 3 / Issue #28).

BD-001, DS-001 Rev.1.6, TEST-002 Rev.1.1 の仕様書契約が
dbt モデル・設定・スキーマ定義に確実に実装されているかを検証する。
"""

from __future__ import annotations

from pathlib import Path
import re

DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"


def test_dbt_files_exist() -> None:
    """必要な dbt プロジェクトファイルがすべて存在することを確認。"""
    required_files = [
        "dbt_project.yml",
        "profiles.yml.example",
        "README.md",
        "macros/generate_schema_name.sql",
        "macros/incremental_window.sql",
        "models/staging/sources.yml",
        "models/staging/stg_github_events.sql",
        "models/silver/schema.yml",
        "models/silver/fct_github_events.sql",
        "models/silver/dim_repositories.sql",
        "models/gold/schema.yml",
        "models/gold/mart_oss_quadrant.sql",
    ]
    for rel_path in required_files:
        assert (DBT_DIR / rel_path).is_file(), f"Missing dbt file: {rel_path}"


def test_dbt_project_guardrails() -> None:
    """BD-001 3.2: 1GB クエリスキャン上限が profile の全 target に設定されていること。

    maximum_bytes_billed は接続設定であり、dbt_project.yml のモデル設定としては効かない。
    """
    profiles = (DBT_DIR / "profiles.yml.example").read_text(encoding="utf-8")
    limits = re.findall(r"^\s*maximum_bytes_billed:\s*(\d+)", profiles, re.MULTILINE)
    targets = re.findall(r"^    (?:dev|prod):\s*$", profiles, re.MULTILINE)
    assert len(targets) >= 1
    assert len(limits) == len(targets), "every target must set maximum_bytes_billed"
    assert all(int(v) == 1073741824 for v in limits), "limit must be 1GB (1,073,741,824 bytes)"

    project_yml = (DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8")
    assert "profile: 'octopulse'" in project_yml
    assert "models:" in project_yml


def test_validate_dbt_workflow_pins_match_dockerfile() -> None:
    """CI の dbt 検証が、実際に Airflow イメージへ入る dbt と同じ版で行われること。"""
    root = DBT_DIR.parent
    dockerfile = (root / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    workflow = (root / ".github" / "workflows" / "validate-dbt.yml").read_text(encoding="utf-8")
    for package in ("dbt-core", "dbt-bigquery"):
        docker_pin = re.search(rf'"{package}==([0-9.]+)"', dockerfile)
        workflow_pin = re.search(rf'"{package}==([0-9.]+)"', workflow)
        assert docker_pin and workflow_pin, package
        assert docker_pin.group(1) == workflow_pin.group(1), f"{package}: Dockerfile と validate-dbt.yml のピンが不一致"


def test_validate_dbt_workflow_does_not_need_warehouse_access() -> None:
    """CI の dbt 検証はウェアハウスへ接続せず、認証情報も使わないこと。"""
    workflow = (DBT_DIR.parent / ".github" / "workflows" / "validate-dbt.yml").read_text(encoding="utf-8")
    assert "dbt parse" in workflow and "dbt ls" in workflow
    # 接続を伴うコマンドは実行しない（実機確認は別 Issue）
    for command in ("dbt run", "dbt test", "dbt compile", "dbt build", "dbt debug"):
        assert not re.search(rf"^\s*(?:run:\s*)?{command}\b", workflow, re.MULTILINE), command
    # 認証系の設定を持ち込まない
    for forbidden in ("google-github-actions/auth", "GOOGLE_APPLICATION_CREDENTIALS", "secrets."):
        assert forbidden not in workflow, forbidden


def test_output_datasets_match_terraform() -> None:
    """出力先が Terraform の <prefix>_bronze/silver/gold と一致し、Bronze 実テーブルと衝突しないこと。"""
    macro = (DBT_DIR / "macros/generate_schema_name.sql").read_text(encoding="utf-8")
    assert "macro generate_schema_name" in macro
    assert "env_var('GCP_DATASET_PREFIX', 'octopulse')" in macro
    assert "target.schema" in macro  # カスタムスキーマ未指定時のフォールバック

    project_yml = (DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8")
    # Staging view は Bronze 実テーブル stg_github_events と同名のため bronze に置かない
    staging_block = project_yml.split("staging:")[1].split("silver:")[0]
    assert "+schema: bronze" not in staging_block
    assert "+schema: silver" in staging_block


def test_incremental_window_is_configurable_and_idempotent() -> None:
    """差分取り込み窓が lookback_days / run_date で制御でき、過去日の再実行が冪等であること (NFR-004)。"""
    project_yml = (DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8")
    assert re.search(r"lookback_days:\s*\d+", project_yml)

    macro = (DBT_DIR / "macros/incremental_window.sql").read_text(encoding="utf-8")
    assert "macro incremental_window" in macro
    assert "var('lookback_days')" in macro
    assert "var('run_date'" in macro
    # run_date は SQL に埋め込むため、日付形式の検証があること
    assert "raise_compiler_error" in macro
    # 下限だけでなく上限 (run_date) があること。上限が無いと過去日の再実行が冪等にならない
    assert re.search(r"between\s+date_sub\(.+\)\s+and\s+\{\{\s*run_date_expr\(\)\s*\}\}", macro, re.DOTALL)

    for rel in (
        "models/silver/fct_github_events.sql",
        "models/silver/dim_repositories.sql",
        "models/gold/mart_oss_quadrant.sql",
    ):
        sql = (DBT_DIR / rel).read_text(encoding="utf-8")
        assert "incremental_window('event_date')" in sql, rel
        # モデルに窓の条件を直書きしない (current_date 基準だと backfill で ds が反映されない)
        assert "current_date()" not in sql, rel
        assert "interval 3 day" not in sql, rel


def test_stg_github_events_model() -> None:
    """DS-001 2.3: Staging モデルが Bronze ソースを参照していること。"""
    sql = (DBT_DIR / "models/staging/stg_github_events.sql").read_text(encoding="utf-8")
    assert "source('bronze', 'stg_github_events')" in sql
    assert "event_id" in sql
    assert "event_type" in sql
    assert "event_date" in sql


def test_fct_github_events_model() -> None:
    """DS-001 2.4①: fct_github_events が90日保持と event_date パーティションを持つこと。"""
    sql = (DBT_DIR / "models/silver/fct_github_events.sql").read_text(encoding="utf-8")
    assert "partition_expiration_days=90" in sql or "partition_expiration_days: 90" in sql
    assert '"granularity": "day"' in sql
    assert "json_value(payload.action)" in sql.lower()


def test_dim_repositories_incremental_repo_age() -> None:
    """論点② & DD-002 7: dim_repositories が repo_metadata を結合し、CreateEvent の開設日を累積保持すること。"""
    sql = (DBT_DIR / "models/silver/dim_repositories.sql").read_text(encoding="utf-8")
    assert "CreateEvent" in sql
    assert "json_value(payload.ref_type) = 'repository'" in sql.lower()
    assert "repo_created_at" in sql
    assert "source('silver', 'repo_metadata')" in sql
    # API 由来の開設日を優先し、未補完時は CreateEvent 累積値を使用
    assert "coalesce(r.repo_created_at, e.repo_created_at, c.repo_created_at)" in sql

    sources_yml = (DBT_DIR / "models/staging/sources.yml").read_text(encoding="utf-8")
    assert "- name: silver" in sources_yml
    assert "- name: repo_metadata" in sources_yml

    schema_yml = (DBT_DIR / "models/silver/schema.yml").read_text(encoding="utf-8")
    # DS-001 Rev.1.7 / DD-002 7: metadata_updated_at は NULLABLE のため not_null テストを外す
    meta_section = schema_yml.split("- name: metadata_updated_at")[1]
    assert "not_null" not in meta_section


def test_mart_oss_quadrant_chaoss_calculations() -> None:
    """DS-001 2.5, 2.6: mart_oss_quadrant の CHAOSS 2軸計算式・足切り・フラグが確定仕様と一致すること。"""
    sql = (DBT_DIR / "models/gold/mart_oss_quadrant.sql").read_text(encoding="utf-8")

    # 1. 重複排除カウント (論点③)
    assert re.search(r"count\s*\(\s*distinct\s+if\s*\(\s*event_type\s*=\s*'WatchEvent'", sql, re.IGNORECASE)
    assert re.search(r"count\s*\(\s*distinct\s+if\s*\(\s*event_type\s*=\s*'ForkEvent'", sql, re.IGNORECASE)

    # 2. 母集団足切り条件 (DS-001 2.6①)
    assert "daily_stars >= 3" in sql
    assert "(daily_forks + daily_issues + daily_prs) >= 1" in sql

    # 3. Attention Velocity 計算式 (DS-001 2.6②)
    assert "percent_rank() over" in sql.lower()
    assert "order by daily_stars asc" in sql.lower()

    # 4. Engagement Depth 加重スコア (TEST-002確定: Fork3 + PR2 + Issue1)
    assert "(a.daily_forks * 3) + (a.daily_prs * 2) + (a.daily_issues * 1)" in sql or (
        "daily_forks * 3" in sql and "daily_prs * 2" in sql and "daily_issues * 1" in sql
    )

    # 5. 4象限分類と 0.80 境界 (DS-001 2.6④)
    assert "Hyper-Growth" in sql
    assert "Viral Spotlight" in sql
    assert "Silent Utility" in sql
    assert "Niche / Emerging" in sql
    assert "0.80" in sql

    # 6. 異常値検知フラグ
    assert "is_suspicious_star_surge" in sql
    assert "is_bot_generated_prs" in sql
    assert "is_newborn_buzz" in sql

    # 7. 論点④遵守: target_date パーティションと追加3列
    assert '"field": "target_date"' in sql
    assert "primary_language" in sql
    assert "topics_display" in sql
    assert "repo_age_days" in sql
