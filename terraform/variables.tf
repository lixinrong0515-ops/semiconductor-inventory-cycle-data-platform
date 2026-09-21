variable "credentials" {
  description = "My Credentials"
  default     = "/Users/lixinrong/semic/terraform/keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "semic-inventory-platform"
}


variable "region" {
  description = "Region"
  default     = "us-central1"
}


variable "location" {
  description = "Project Location"
  default     = "US"
}


variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "semic-inventory-platform-data"
}


variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}


variable "bq_raw_dataset_name" {
  description = "Raw BigQuery Dataset Name"
  default     = "semiconductor_raw"
}


variable "bq_staging_dataset_name" {
  description = "Staging BigQuery Dataset Name"
  default     = "semiconductor_staging"
}


variable "bq_marts_dataset_name" {
  description = "Marts BigQuery Dataset Name"
  default     = "semiconductor_marts"
}