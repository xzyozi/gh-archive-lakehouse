# OctoPulse dbt Transformation Pipeline (Phase 3)

BigQuery 上のメダリオンアーキテクチャ（Bronze → Silver → Gold）を変換・管理する dbt プロジェクトです。
Linux Foundation 傘下の「CHAOSS」標準に準拠した 2軸パーセンタイル評価モデル（Attention Velocity × Engagement Depth）を日次で計算し、Looker Studio 向けの集計マートを提供します。

---

## 1. メダリオンモデル構成

| レイヤー | モデル名 | マテリアライズ | 説明 | 正本仕様 |
| :--- | :--- | :---: | :--- | :--- |
| **Staging** | `stg_github_events` | `view` | Bronze 生ログテーブルの参照・型定義 | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.3 |
| **Silver** | `fct_github_events` | `incremental` | クレンジング済み日別イベントファクト（90日保持） | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.4① |
| **Silver** | `dim_repositories` | `incremental` | リポジトリマスタ。**CreateEvent から開設日を累積保持（論点②）** | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.4② |
| **Gold** | `mart_oss_quadrant` | `incremental` | **CHAOSS 2軸評価 4象限マート（恒久保持）** | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.5, 2.6 |

---

## 2. Always Free 枠のコストガードレール

* **クエリスキャン上限強制**:
  * `dbt_project.yml` に `maximum_bytes_billed: 1073741824` (1GB) を設定。誤った巨大クエリによるスキャン課金を BigQuery 側で物理的に強制停止します（[`BD-001`](../docs/design/BD-001_基本設計書.md) 3.2）。
* **インクリメンタル保持によるスキャン削減 (論点②)**:
  * `dim_repositories` が `CreateEvent` 由来の `repo_created_at` を累積保持するため、Gold マート生成時は過去全パーティションを再スキャンせず、当日1日分のイベントファクトとの結合だけで `repo_age_days` を算出できます。

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
```bash
cd dbt

# パッケージインストール (必要に応じて)
dbt deps

# モデル変換実行 (全レイヤー)
dbt run

# データ品質・制約テスト
dbt test
```
