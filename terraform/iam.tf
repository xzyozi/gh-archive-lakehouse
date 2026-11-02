# サービスアカウントおよび IAM / WIF 設計 (BD-001 3.2, NFR-002)
# サービスアカウントの固定JSONキーは一切発行せず、WIFおよびADCマウントで完結させる。

# パイプライン実行用サービスアカウント
resource "google_service_account" "runner" {
  account_id   = "octopulse-runner"
  display_name = "OctoPulse Runner (Airflow Ingestion, dbt & CI/CD)"
  description  = "Service Account used by Airflow pipelines and GitHub Actions via WIF"
}

# GCS バケットに対する Object 管理権限 (最小権限)
resource "google_storage_bucket_iam_member" "gcs_runner" {
  bucket = google_storage_bucket.lakehouse_raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.runner.email}"
}

# BigQuery クエリジョブ実行権限 (プロジェクトレベル)
resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.runner.email}"
}

# BigQuery 各データセットに対する Data Editor 権限
resource "google_bigquery_dataset_iam_member" "bronze_editor" {
  dataset_id = google_bigquery_dataset.bronze.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.runner.email}"
}

resource "google_bigquery_dataset_iam_member" "silver_editor" {
  dataset_id = google_bigquery_dataset.silver.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.runner.email}"
}

resource "google_bigquery_dataset_iam_member" "gold_editor" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.runner.email}"
}

# Workload Identity Federation (GitHub Actions キーレス連携)
resource "google_iam_workload_identity_pool" "github_pool" {
  workload_identity_pool_id = "octopulse-github-pool"
  display_name              = "OctoPulse GitHub Actions Pool"
  description               = "Identity Pool for keyless GitHub Actions OIDC federation"
}

resource "google_iam_workload_identity_pool_provider" "github_provider" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-actions-provider"
  display_name                       = "GitHub Actions Provider"
  description                        = "OIDC Provider for GitHub Actions"

  attribute_mapping = {
    "google.subject"             = "assertion.sub"
    "attribute.actor"            = "assertion.actor"
    "attribute.repository"       = "assertion.repository"
    "attribute.repository_owner" = "assertion.repository_owner"
  }

  attribute_condition = "assertion.repository == '${var.github_repository}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# サービスアカウントへの権限借用 (Workload Identity User) 付与
resource "google_service_account_iam_member" "github_wif_user" {
  service_account_id = google_service_account.runner.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_pool.name}/attribute.repository/${var.github_repository}"
}
