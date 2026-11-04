"""Airflow DAG 構成および契約の単体テスト (Phase 2 / Issue #26).

ローカルおよびCI環境に重量級の apache-airflow パッケージを要求せず、
AST (抽象構文木) 解析と文字列契約検証を用いて、ミリ秒単位で高速・確実に
タスク定義・パラメータ契約・依存関係を検証する。
"""

from __future__ import annotations

import ast
import datetime
from pathlib import Path

from octopulse.ingestion.gcs_uploader import build_gcs_blob_path

DAG_FILE = Path(__file__).resolve().parent.parent / "airflow" / "dags" / "gh_stream_ingest.py"


def test_dag_file_syntax_valid() -> None:
    """DAG Python ファイルが構文エラーなく解析できることを確認。"""
    assert DAG_FILE.is_file(), f"DAG file not found: {DAG_FILE}"
    source = DAG_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(DAG_FILE))
    assert isinstance(tree, ast.Module)


def test_dag_imports_gcs_to_bigquery_operator() -> None:
    """GCSToBigQueryOperator がインポートされていることを確認。"""
    source = DAG_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported_names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imported_names.append(alias.name)

    assert "GCSToBigQueryOperator" in imported_names
    assert "PythonOperator" in imported_names
    assert "DAG" in imported_names


def test_dag_task_parameters_and_contracts() -> None:
    """BigQuery Load タスクのパラメータが設計書 (BD-001 / DS-001) と一致することを確認。"""
    source = DAG_FILE.read_text(encoding="utf-8")

    # 1. タスク ID
    assert 'task_id="load_gcs_to_bigquery_bronze"' in source

    # 2. フォーマット契約
    assert 'source_format="NEWLINE_DELIMITED_JSON"' in source

    # 3. 冪等性契約: WRITE_TRUNCATE
    assert 'write_disposition="WRITE_TRUNCATE"' in source

    # 4. スキーマ契約: 既存テーブル定義を利用 (autodetect=False)
    assert "autodetect=False" in source

    # 5. 論点①遵守: 宛先テーブルにパーティションデコレータ ($YYYYMMDD) が付与されていること
    assert "stg_github_events" in source
    assert "${{{{ ds_nodash }}}}" in source or "${{ ds_nodash }}" in source
    assert "_bronze.stg_github_events" in source

    # 6. タスク依存関係
    assert "ingest_task >> load_bronze_task" in source


def test_source_objects_path_matches_uploader_blob_path() -> None:
    """Airflow の source_objects パスが gcs_uploader.build_gcs_blob_path と完全一致すること。"""
    source = DAG_FILE.read_text(encoding="utf-8")
    assert "raw/{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:8] }}/events_{{ ds_nodash }}.jsonl" in source

    # 任意の日付で Jinja 式をシミュレーション展開し、build_gcs_blob_path と突合
    sample_date = datetime.date(2024, 1, 3)
    ds_nodash = sample_date.strftime("%Y%m%d")  # "20240103"

    simulated_path = (
        f"raw/{ds_nodash[:4]}/{ds_nodash[4:6]}/{ds_nodash[6:8]}/events_{ds_nodash}.jsonl"
    )
    expected_path = build_gcs_blob_path(sample_date)

    assert simulated_path == expected_path, (
        f"Path mismatch: simulated={simulated_path}, expected={expected_path}"
    )


# ---------------------------------------------------------------------------
# Phase 5: Cosmos による dbt 統合 (Issue #30)
# ---------------------------------------------------------------------------

COMPOSE_FILE = Path(__file__).resolve().parent.parent / "airflow" / "docker-compose.yaml"
DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"


def test_dag_imports_cosmos() -> None:
    """Cosmos の主要クラスと定数がインポートされていること。"""
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    imported = {alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) for alias in node.names}
    for name in (
        "DbtTaskGroup",
        "ProjectConfig",
        "ProfileConfig",
        "ExecutionConfig",
        "RenderConfig",
        "ExecutionMode",
        "LoadMode",
        "TestBehavior",
    ):
        assert name in imported, name


def test_dbt_task_group_contract() -> None:
    """DbtTaskGroup の構成が設計 (BD-001 C-ORCHESTRATE / Issue #30 の決定事項) と一致すること。"""
    source = DAG_FILE.read_text(encoding="utf-8")

    assert "DbtTaskGroup(" in source
    assert 'group_id="dbt_transform"' in source
    # Dockerfile の専用 venv で実行する (Airflow と dbt の依存を分離)
    assert "ExecutionMode.LOCAL" in source
    assert "/opt/airflow/dbt_venv/bin/dbt" in source
    # Q4: まず DBT_LS で描画 / Q7: 各モデル直後にテスト
    assert "LoadMode.DBT_LS" in source
    assert "TestBehavior.AFTER_EACH" in source
    # 論理日付を dbt に渡す (NFR-004: 過去日の再実行が冪等になる)
    assert '"vars": {"run_date": "{{ ds }}"}' in source
    # ネットワークアクセスを伴う dbt deps は実行しない (packages.yml なし)
    assert "dbt_deps=False" in source
    assert '"install_deps": False' in source


def test_dbt_runs_after_bronze_load() -> None:
    """ingest → Bronze Load → dbt の順で依存していること。"""
    source = DAG_FILE.read_text(encoding="utf-8")
    assert "ingest_task >> load_bronze_task >> dbt_transform" in source


def test_dbt_profile_is_single_source_with_guardrail() -> None:
    """dbt の profile は profiles.yml.example を profiles.yml としてマウントし、1GB ガードレールを一元化すること。"""
    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    assert "../dbt:/opt/airflow/dbt:ro" in compose
    # Cosmos は profiles_yml_filepath の親ディレクトリを --profiles-dir に使うため、ファイル名は profiles.yml 固定
    assert "../dbt/profiles.yml.example:/opt/airflow/dbt_profiles/profiles.yml:ro" in compose

    dag = DAG_FILE.read_text(encoding="utf-8")
    assert "/opt/airflow/dbt_profiles/profiles.yml" in dag
    assert (DBT_DIR / "profiles.yml.example").is_file()


def test_compose_passes_dataset_prefix_and_writable_dbt_paths() -> None:
    """dbt が参照する GCP_DATASET_PREFIX を渡し、読み取り専用マウントでも書き込める出力先を指定すること。"""
    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    assert "GCP_DATASET_PREFIX:" in compose
    assert "DBT_TARGET_PATH: /tmp/dbt_target" in compose
    assert "DBT_LOG_PATH: /tmp/dbt_logs" in compose
