locals {
  #data_lake_bucket = "data_lake"
  data_lake_bucket = "data_lake_de-project-711"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "gh-archive-355200"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-west1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  #default = "gh_archive_all"
  default = "de_project_711"
}
variable "credentials" {
  type = string
  default= "~/.google/credentials/google_credentials.json"
}

/* variable "composer_name" {
  description = "Name for the Cloud Composer / Airflow service in GCP"
  type = string
  default = "gh-airflow"
}

variable "composer_image" {
  description = "Image to be used for Composer/Airflow"
  type = string
  default = "composer-2.0.7-airflow-2.2.3"
}

variable "service_account" {
  description = "Service account to use for setting up GCP"
  type = string
  default = "gh-archive-user@gh-archive-345218.iam.gserviceaccount.com"
} */
# Transfer service
# variable "access_key_id" {
#   description = "AWS access key"
#   type = string
# }

# variable "aws_secret_key" {
#   description = "AWS secret key"
#   type = string
# }