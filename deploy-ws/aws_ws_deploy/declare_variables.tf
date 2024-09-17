# variable declaration
variable "databricks_account_console_host" {
  description = "Databricks Account Console URL"
  type = string
}

variable "databricks_account_id" {
  type = string
}

variable "databricks_account_client_id" {
  type = string
}

variable "databricks_acccount_client_secret" {
  type = string
}


variable "aws_account_id" {
  type = string
}

variable "aws_access_key_id" {
  type = string
}

variable "aws_secret_access_key" {
  type = string
}

variable "aws_session_token" {
  type = string
}

variable "tags" {
  default = {}
  type = map(string)
}

variable "cidr_block" {
  default = "10.4.0.0/16"
}

variable "deployment_region" {
  description = "Deployment Rehion"  
  default = "ap-south-1"
}

variable "deployment_keyword" {
    type = string
    default = "dbdemos_ucx"
}

variable "workspace_admin" {
    type = string
}

resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
}

locals {
  prefix = "${var.deployment_keyword}-${random_string.naming.result}"
}