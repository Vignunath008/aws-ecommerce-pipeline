variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type    = string
  default = "ecom-pipeline"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "create_redshift" {
  description = "Create a Redshift Serverless namespace + workgroup (costs money while running)"
  type        = bool
  default     = true
}

variable "redshift_db_name" {
  type    = string
  default = "ecommerce"
}

variable "redshift_admin_user" {
  type    = string
  default = "admin_user"
}

variable "redshift_admin_password" {
  description = "Set in terraform.tfvars (never commit it). 8-64 chars, upper, lower and a digit."
  type        = string
  sensitive   = true
  default     = null
}
