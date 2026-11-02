variable "project_id" {
  type        = string
  description = "Google Cloud のプロジェクト ID"
}

variable "region" {
  type        = string
  description = "リソースをプロビジョニングするリージョン (Always Free 対象: us-central1 推奨)"
  default     = "us-central1"

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1"], var.region)
    error_message = "Always Free 枠を死守するため、region は us-central1, us-east1, us-west1 のいずれかを指定してください (BD-001)。"
  }
}

variable "environment" {
  type        = string
  description = "環境識別子 (例: prod, dev)"
  default     = "prod"
}

variable "bucket_name" {
  type        = string
  description = "GCS バケット名 (空文字の場合はプロジェクトIDプレフィックス付きで自動決定)"
  default     = ""
}

variable "dataset_prefix" {
  type        = string
  description = "BigQuery データセットのプレフィックス (例: octopulse -> octopulse_bronze)"
  default     = "octopulse"
}

variable "github_repository" {
  type        = string
  description = "Workload Identity Federation で連携する GitHub リポジトリ名 (owner/repo)"
  default     = "xzyozi/gh-archive-lakehouse"
}
