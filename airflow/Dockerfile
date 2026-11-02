# OctoPulse 用 Airflow イメージ (Issue #16)
#
# 構成:
#   - Airflow 本体の環境: astronomer-cosmos (dbt を Airflow タスクとして描画・実行)
#   - 専用 venv /opt/airflow/dbt_venv: dbt-core + dbt-bigquery
#     (Airflow と dbt の依存衝突を避ける Cosmos 公式推奨パターン。
#      Cosmos 側は ExecutionMode.LOCAL + dbt_executable_path で下記 dbt を指す)
#
# バージョンピン (サプライチェーン対策: PyPI 公開から3日以上経過した版のみ。確認日 2026-11-02)
#   | パッケージ          | 版       | PyPI 公開日 (UTC)   | 選定根拠                                                    |
#   | ------------------- | -------- | ------------------- | ----------------------------------------------------------- |
#   | astronomer-cosmos   | 1.15.1   | 2026-08-04          | requires-python>=3.10, apache-airflow>=2.9.0 (2.9.1/3.11 可) |
#   | dbt-bigquery        | 1.11.3   | 2026-06-24          | dbt-core<2.0,>=1.11.6 (1.x 系)。requires-python>=3.10        |
#   | dbt-core            | 1.11.16  | 2026-10-14          | dbt-bigquery 1.11.3 の要求 (>=1.11.6,<2.0) を満たす 1.11 系 |
#   詳細と更新手順: docs/setup/airflow_image.md
FROM apache/airflow:2.9.1-python3.11

ARG AIRFLOW_VERSION=2.9.1
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

# 公式イメージの非root ユーザー (airflow) のまま実行する。USER root は使わない。
USER airflow

# 1) dbt 専用 venv (Airflow の依存と分離)
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core==1.11.16" \
        "dbt-bigquery==1.11.3"

# 2) Airflow 本体の環境に Cosmos を導入 (Airflow 公式 constraints で Airflow の依存を固定)
RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" \
        "astronomer-cosmos==1.15.1"
