provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.project_id}-octopulse-lakehouse"

  common_labels = {
    managed_by  = "terraform"
    project     = "octopulse"
    environment = var.environment
  }
}
