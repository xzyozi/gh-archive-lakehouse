output "project_id" {
  description = "GCP プロジェクト ID"
  value       = var.project_id
}

output "region" {
  description = "リソースがプロビジョニングされたリージョン"
  value       = var.region
}

output "gcs_bucket_name" {
  description = "Filtered JSONL 一時蓄積用 GCS バケット名"
  value       = google_storage_bucket.lakehouse_raw.name
}

output "bigquery_dataset_bronze" {
  description = "BigQuery Bronze データセット ID"
  value       = google_bigquery_dataset.bronze.dataset_id
}

output "bigquery_dataset_silver" {
  description = "BigQuery Silver データセット ID"
  value       = google_bigquery_dataset.silver.dataset_id
}

output "bigquery_dataset_gold" {
  description = "BigQuery Gold データセット ID"
  value       = google_bigquery_dataset.gold.dataset_id
}

output "bigquery_table_bronze_events" {
  description = "BigQuery Bronze 生ログテーブル ID"
  value       = google_bigquery_table.stg_github_events.table_id
}

output "service_account_email" {
  description = "パイプライン実行用サービスアカウントのメールアドレス"
  value       = google_service_account.runner.email
}

output "workload_identity_provider" {
  description = "GitHub Actions 連携用 Workload Identity Provider リソース名"
  value       = google_iam_workload_identity_pool_provider.github_provider.name
}
