terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0.0, < 7.0.0"
    }
  }

  # ステートは GCS バックエンドで管理（必要に応じて terraform init -backend-config で指定）
  # backend "gcs" {
  #   bucket = "<project_id>-tfstate"
  #   prefix = "octopulse/state"
  # }
}
