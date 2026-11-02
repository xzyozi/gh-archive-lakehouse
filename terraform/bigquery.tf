# BigQuery メダリオン構成データセット (BD-001, DS-001)

# Bronze データセット (Rawステージング層)
resource "google_bigquery_dataset" "bronze" {
  dataset_id  = "${var.dataset_prefix}_bronze"
  location    = var.region
  description = "OctoPulse Bronze Layer (Raw staging data - 30 days retention)"

  default_partition_expiration_ms = 2592000000 # 30日 (DS-001 2.3)

  labels = locals.common_labels
}

# Silver データセット (クレンジング済みスター・スキーマ層)
resource "google_bigquery_dataset" "silver" {
  dataset_id  = "${var.dataset_prefix}_silver"
  location    = var.region
  description = "OctoPulse Silver Layer (Cleansed star schema - 90 days retention)"

  default_partition_expiration_ms = 7776000000 # 90日 (DS-001 2.4)

  labels = locals.common_labels
}

# Gold データセット (集約マート層 - Looker Studio 接続用)
resource "google_bigquery_dataset" "gold" {
  dataset_id  = "${var.dataset_prefix}_gold"
  location    = var.region
  description = "OctoPulse Gold Layer (Aggregated marts - permanent retention)"

  # Gold層は恒久保持のため default_partition_expiration_ms は設定しない (DS-001 1.2)

  labels = locals.common_labels
}

# Bronze 生ログテーブル: stg_github_events (DS-001 2.3)
resource "google_bigquery_table" "stg_github_events" {
  dataset_id  = google_bigquery_dataset.bronze.dataset_id
  table_id    = "stg_github_events"
  description = "Raw GitHub events flat schema loaded from GCS JSONL (DS-001 Rev.1.5)"

  deletion_protection = false

  time_partitioning {
    type          = "DAY"
    field         = "event_date"
    expiration_ms = 2592000000 # 30日
  }

  clustering = ["repo_name", "type"]

  schema = jsonencode([
    {
      name        = "id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "GH Archiveイベントの一意識別子"
    },
    {
      name        = "type"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "イベント種別 (WatchEvent, ForkEvent, IssuesEvent, PullRequestEvent, CreateEvent)"
    },
    {
      name        = "actor_login"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "イベント実行ユーザーのアカウント名"
    },
    {
      name        = "repo_id"
      type        = "INT64"
      mode        = "NULLABLE"
      description = "GitHub リポジトリID"
    },
    {
      name        = "repo_name"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "リポジトリフルネーム (owner/repo)"
    },
    {
      name        = "payload"
      type        = "JSON"
      mode        = "REQUIRED"
      description = "射影済みペイロード (action, merged, ref_type)"
    },
    {
      name        = "created_at"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "イベント発生時刻 (UTC)"
    },
    {
      name        = "event_date"
      type        = "DATE"
      mode        = "REQUIRED"
      description = "パーティションキー (created_at の日付部分)"
    }
  ])

  labels = locals.common_labels
}
