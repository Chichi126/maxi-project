variable "bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
}

variable "bucket_location" {
  description = "The location of the GCS bucket"
  type        = string
}

variable "force_destroy" {
  description = "Whether to force destroy the bucket"
  type        = bool
  default     = false
}

variable "lifecycle_rule_age" {
  description = "The age in days to apply lifecycle rules"
  type        = number
}

variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
}

variable "credentials" {
  description = "Path to the GCP credentials JSON file"
  type        = string
}

variable "public_access_prevention" {
  description = "Whether public access to bucket is prevented"
  type        = string
  default     = "enforced"
}
