# Connect to gcp using ADC (identity verification)
provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

/* add these data blocks */

# This data source gets a temporary token for the service account
data "google_service_account_access_token" "default" {
  provider               = google
  target_service_account = "terraform-runner@terraform-485414.iam.gserviceaccount.com"
  scopes                 = ["https://www.googleapis.com/auth/cloud-platform"]
  lifetime               = "3600s"
}

# This second provider block uses that temporary token and does the real work
provider "google" {
  alias        = "impersonated"
  access_token = data.google_service_account_access_token.default.access_token
  project      = var.project
  region       = var.region
  zone         = var.zone
}

#BUCKET
resource "google_storage_bucket" "demo-bucket" {
  provider      = google.impersonated
  name          = "${var.project}-terra-bucket"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1 #day
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

#BIGQUERY DATASET
resource "google_bigquery_dataset" "demo_dataset" {
  provider   = google.impersonated
  dataset_id = "zoomcamp_dataset"
  location   = var.region

  friendly_name = "Zoomcamp Data Engineering Dataset"
}