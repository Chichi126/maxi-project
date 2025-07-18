variable "bucket_name" {
  type        = string
  description = "cloud storage for sales data"
}
variable "bucket_location" {
  type    = string
  default = false
}
variable "force_destroy" {
  type    = bool
  default = true
}
variable "lifecyle_rule_age" {
  type        = number
  default     = 3
  description = "the age in days after which"

}

variable "public_access_prevention" {
  type        = string
  description = "prevent public access to the bucket"
  default     = "enforced"
}
variable "project_id" {
  type        = string
  description = "value"

}
variable "region" {
  type = string
}
variable "credentials" {
  type = string
}