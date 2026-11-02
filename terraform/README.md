# OctoPulse Terraform Infrastructure (IaC)

Google Cloud Platform (GCP) 上に、完全0円運用（Always Free 枠死守）に準拠したレイクハウス基盤をプロビジョニングする Terraform 構成です。

## 1. 構築されるリソース

| リソース | 役割 | Always Free 対策 / 非機能要件 | 正本仕様 |
| :--- | :--- | :--- | :--- |
| **GCS Bucket** | Filtered JSONL 一時蓄積（Data Lake） | `us-central1`、**配置後7日で自動破棄**（Lifecycle Rule）、バージョニング無効 | [`BD-001`](../docs/design/BD-001_基本設計書.md) NFR-001 |
| **BigQuery `octopulse_bronze`** | 生ログステージング層 | `us-central1`、**日別パーティション有効期限 30日** | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.3 |
| **BigQuery `octopulse_silver`** | クレンジング済み層 (fct/dim) | `us-central1`、**日別パーティション有効期限 90日** | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.4 |
| **BigQuery `octopulse_gold`** | 4象限スコアマート層 | `us-central1`、恒久保持（Looker Studio 抽出ソース） | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.5 |
| **BigQuery `stg_github_events`** | Bronze 生ログテーブル | フラット8列定義、`event_date` DAY パーティション、クラスタリング (`repo_name`, `type`) | [`DS-001`](../docs/design/DS-001_データ構造仕様書.md) 2.3 |
| **IAM Service Account** | パイプライン実行用 SA | 最小権限付与（GCS objectAdmin, BQ jobUser, BQ dataEditor） | [`BD-001`](../docs/design/BD-001_基本設計書.md) NFR-002 |
| **Workload Identity Federation** | GitHub Actions キーレス連携 | OIDC による短命トークン認証。**固定JSONキーの発行数 0件** | [`BD-001`](../docs/design/BD-001_基本設計書.md) 3.2 |

---

## 2. 前提条件

- Terraform: `>= 1.5.0`
- Google Cloud SDK (`gcloud`): インストールおよび認証済み
- 対象 GCP プロジェクトで以下の API が有効化されていること:
  - `storage.googleapis.com` (Cloud Storage)
  - `bigquery.googleapis.com` (BigQuery)
  - `iam.googleapis.com` (Identity and Access Management)
  - `iamcredentials.googleapis.com` (IAM Service Account Credentials API)
  - `sts.googleapis.com` (Security Token Service)

```bash
gcloud services enable \
  storage.googleapis.com \
  bigquery.googleapis.com \
  iam.googleapis.com \
  iamcredentials.googleapis.com \
  sts.googleapis.com
```

---

## 3. プロビジョニング手順

### 1. 変数ファイル作成
```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# terraform.tfvars 内の project_id をご自身の GCP プロジェクト ID に変更
```

### 2. 初期化と構文検証
```bash
terraform init
terraform fmt -check
terraform validate
```

### 3. 実行計画の確認
```bash
terraform plan
```

### 4. リソース作成
```bash
terraform apply
```

---

## 4. GitHub Actions CI/CD 連携

Terraform apply 完了後、出力された値を GitHub リポジトリの Variables / Secrets に登録します。

1. **`GCP_WORKLOAD_IDENTITY_PROVIDER`**:
   - `terraform output -raw workload_identity_provider` の値
2. **`GCP_SERVICE_ACCOUNT`**:
   - `terraform output -raw service_account_email` の値
