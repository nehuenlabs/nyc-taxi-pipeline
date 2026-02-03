# =============================================================================
# Outputs
# =============================================================================

output "bronze_bucket_name" {
  description = "Name of the bronze GCS bucket"
  value       = google_storage_bucket.bronze.name
}

output "bronze_bucket_url" {
  description = "URL of the bronze bucket"
  value       = google_storage_bucket.bronze.url
}

output "gold_bucket_name" {
  description = "Name of the gold GCS bucket"
  value       = google_storage_bucket.gold.name
}

output "gold_bucket_url" {
  description = "URL of the gold bucket"
  value       = google_storage_bucket.gold.url
}

output "service_account_email" {
  description = "Email of the pipeline service account"
  value       = google_service_account.pipeline.email
}

output "deployment_summary" {
  description = "Summary of deployed resources"
  value = <<-EOT
    
    ============================================
    NYC Taxi Pipeline - Deployment Complete!
    ============================================
    
    GCS Buckets:
      - Bronze: gs://${google_storage_bucket.bronze.name}
      - Gold:   gs://${google_storage_bucket.gold.name}
    
    Service Account:
      - ${google_service_account.pipeline.email}
    
    ============================================
  EOT
}
