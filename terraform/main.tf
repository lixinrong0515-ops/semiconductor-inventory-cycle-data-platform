terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


# GCS DATA LAKE

resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = var.gcs_storage_class
  force_destroy = true

  uniform_bucket_level_access = true
}


# BIGQUERY DATASETS

resource "google_bigquery_dataset" "raw" {
  dataset_id = var.bq_raw_dataset_name
  location   = var.location
}


