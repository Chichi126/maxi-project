variable "bucket_name" {
    type= string
    description = "The name of google cloud storage"
  
}

variable "bucket_location" {
    type = string
    description = "Location of cloud storage"
  
}

variable "force_destroy" {
  type = bool
  default = true
}

variable "public_access_prevention" {
  type = string
  default = "enforced"
}

variable "lifecycle_rule_age" {
  type = number
  default = 3

}

variable "project_id" {
  type = string
  description = "The ID of the google cloud project"
}

variable "region" {
  type = string
}

variable "credentials" {
  type = string
  description = "Path to the google cloud service key"
}

