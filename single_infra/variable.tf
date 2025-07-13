variable "bucket_name" {
  type        = string
  description = "The name of the Google Cloud Storage bucket."
}

variable "bucket_location" {
  type        = string
  description = "The location of the Google Cloud Storage bucket."
}

variable "force_destroy" {
  type    = bool
  default = true
}

variable "public_access_prevention" {
  type        = string
  default     = "enforced"
  description = "Prevent public access to the bucket"
}

variable "lifecycle_rule_age" {
  type        = number
  default     = 3
  description = "The age in days after which objects in the  bucket"
}

variable "project_id" {
  type        = string
  description = "value"
}

variable "region" {
  type    = string
  default = "us-central1"

}

variable "credentials" {
  type = string
}