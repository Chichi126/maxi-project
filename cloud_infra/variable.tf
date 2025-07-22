variable "bucket_name" {
  description = "Name of the Google Cloud Storage bucket"
  type        = string
}

variable "bucket_location" {
  type = string
}

variable "force_destroy" {
  type    = bool
  default = true
}

variable "public_access_prevention" {
  type    = string
  default = "enforced"
}

variable "lifecycle_rule_age" {
  type = number
}


variable "project_id" {
  type        = string
  description = "The ID of the Google Cloud project"
}

variable "region" {
  type = string
}

variable "credentials" {
  type        = string
  description = "Path to the Google Cloud service account credentials JSON file"
}