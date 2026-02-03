# =============================================================================
# NYC Taxi Pipeline - Terraform Configuration (Simplified)
# =============================================================================

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# -----------------------------------------------------------------------------
# Provider Configuration
# -----------------------------------------------------------------------------
provider "google" {
  project = var.project_id
  region  = var.region
}

# -----------------------------------------------------------------------------
# Local Variables
# -----------------------------------------------------------------------------
locals {
  common_labels = {
    project     = "nyc-taxi-pipeline"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# =============================================================================
# GCS BUCKETS
# =============================================================================

# Bronze Bucket - Raw Data Layer
resource "google_storage_bucket" "bronze" {
  name          = "${var.project_id}-nyc-taxi-bronze"
  project       = var.project_id
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  labels = local.common_labels
}

# Gold Bucket - Curated Data Layer
resource "google_storage_bucket" "gold" {
  name          = "${var.project_id}-nyc-taxi-gold"
  project       = var.project_id
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = local.common_labels
}

# =============================================================================
# SERVICE ACCOUNT
# =============================================================================

resource "google_service_account" "pipeline" {
  project      = var.project_id
  account_id   = "nyc-taxi-pipeline-${var.environment}"
  display_name = "NYC Taxi Pipeline Service Account"
  description  = "Service account for the NYC Taxi data pipeline"
}

# =============================================================================
# IAM PERMISSIONS
# =============================================================================

# Bronze bucket permissions
resource "google_storage_bucket_iam_member" "bronze_object_admin" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# Gold bucket permissions
resource "google_storage_bucket_iam_member" "gold_object_admin" {
  bucket = google_storage_bucket.gold.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}
