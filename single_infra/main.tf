# resource "google_storage_bucket" "bucket" {
#   name                        = var.bucket_name
#   location                    = var.bucket_location
#   force_destroy              = var.force_destroy
#   public_access_prevention   = var.public_access_prevention

#   lifecycle_rule {
#     condition {
#       age = var.lifecycle_rule_age
#     }
#     action {
#       type = "Delete"
#     }
#   }
# }


resource "google_storage_bucket" "sales_bucket" {
  name                     = var.bucket_name
  location                 = var.bucket_location
  force_destroy            = var.force_destroy
  public_access_prevention = var.public_access_prevention # Ensure this variable is defined

  lifecycle_rule {
    condition {
      age = var.lifecycle_rule_age
    }
    action {
      type = "Delete"
    }
  }
}
