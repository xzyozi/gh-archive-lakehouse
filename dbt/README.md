# OctoPulse dbt Transformation Pipeline (Phase 3)

BigQuery 上のメダリオンアーキテクチャ（Bronze → Silver → Gold）を変換・管理する dbt プロジェクトです。
Linux Foundation 傘下の「CHAOSS」標準に準拠した 2軸パーセンタイル評価モデル（Attention Velocity × Engagement Depth）を日次で計算し、Looker Studio 向けの集計マートを提供します。

---

## 1. メダリオンモデル構成

| レイヤー    | モデル名            | マテリアライズ | 説明                                                            | 正本仕様                                                       |
| :---------- | :------------------ | :------------: | :-------------------------------------------------------------- | :------------------------------------------------------------- |
| **Staging** | `stg_github_events` |     `view`     | Bronze 生ログテーブルの参照・型定義                             | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.3      |
| **Silver**  | `fct_github_events` | `incremental`  | クレンジング済み日別イベントファクト（90日保持）                | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.4①     |
| **Silver**  | `dim_repositories`  | `incremental`  | リポジトリマスタ。**CreateEvent から開設日を累積保持（論点②）** | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.4②     |
| **Gold**    | `mart_oss_quadrant` | `incremental`  | **CHAOSS 2軸評価 4象限マート（恒久保持）**                      | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.5, 2.6 |

---

## 2. Always Free 枠のコストガードレール

* **クエリスキャン上限強制**:
  * `profiles.yml`（`profiles.yml.example` 参照）の各 target に `maximum_bytes_billed: 1073741824` (1GB) を設定。誤った巨大クエリによるスキャン課金を BigQuery 側で物理的に強制停止します（[`BD-001`](../docs/design/BD-001_基本設計書.md) 3.2）。これは dbt のモデル設定ではなく接続設定のため、`dbt_project.yml` ではなく profile に置きます。
* **インクリメンタル保持によるスキャン削減 (論点②)**:
  * `dim_repositories` が `CreateEvent` 由来の `repo_created_at` を累積保持するため、Gold マート生成時は過去全パーティションを再スキャンせず、当日1日分のイベントファクトとの結合だけで `repo_age_days` を算出できます。

---

## 2.1 出力先データセットと再処理範囲

* **出力先データセット**: `macros/generate_schema_name.sql` が、カスタムスキーマを `<GCP_DATASET_PREFIX>_<schema>` としてそのまま出力先にします。Terraform が作成する `<prefix>_bronze` / `<prefix>_silver` / `<prefix>_gold` と一致します。
* **Staging の配置**: `stg_github_events`（view）は Bronze の実テーブルと同名のため、Bronze ではなく Silver データセット（`<prefix>_silver`）に作成します。
* **再処理範囲**: インクリメンタル実行は直近 `lookback_days`（既定 3 日）を再処理します。backfill は `dbt run --vars '{lookback_days: 30}'` のように上書きします。
* **未充填の列**: `dim_repositories` の `primary_language` / `topics` / `description` / `total_stars` / `total_forks` は、本 Issue のスコープでは Enrichment 未実装のため NULL（`topics` は空配列）です。

* **Airflow からの実行**: `airflow/dags/gh_stream_ingest.py` の `DbtTaskGroup`（Astronomer Cosmos）が、Bronze ロードの後に本プロジェクトを実行します。論理日付 `ds` が `run_date` として渡されます。制御仕様は [`DD-004`](../docs/design/DD-004_オーケストレーション詳細設計書.md) を参照してください。

---

## 3. 実行手順

### 1. プロファイル設定
```bash
cp profiles.yml.example ~/.dbt/profiles.yml
# または環境変数を設定して実行
export GCP_PROJECT_ID="your-project-id"
export GCP_DATASET_PREFIX="octopulse"
```

### 2. モデル実行・テスト

インクリメンタルモデルの再処理窓は `[run_date - lookback_days, run_date]` です（`macros/incremental_window.sql`）。手動実行で `run_date` を省略すると `current_date()` になります。過去日を再実行する場合は日付を明示してください。

```bash
dbt run --vars '{run_date: 2026-11-03}'
```

```bash
cd dbt

# パッケージインストール (必要に応じて)
dbt deps

# モデル変換実行 (全レイヤー)
dbt run

# データ品質・制約テスト
dbt test
```
