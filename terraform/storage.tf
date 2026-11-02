# GCS Filtered JSONL 一時蓄積用バケット (BD-001 C-LAKE, NFR-001)
# 配置後7日で自動削除するライフサイクルルールによりストレージ無料枠 (5GB) を永続維持。
resource "google_storage_bucket" "lakehouse_raw" {
  name          = locals.bucket_name
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = false
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age        = 7
      with_state = "ANY"
    }
  }

  labels = locals.common_labels
}
