terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.15.0"
    }
  }
}

// aws provider auth
provider "aws" {
  region     = var.deployment_region
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key
  token      = var.aws_session_token 
}


// initialize provider in "MWS" mode to provision new workspace
provider "databricks" {
  alias         = "mws"
  host          = var.databricks_account_console_host
  account_id    = var.databricks_account_id
  client_id     = var.databricks_account_client_id
  client_secret = var.databricks_acccount_client_secret
}


provider "databricks" {
  alias         = "accounts"
  host          = var.databricks_account_console_host
  account_id    = var.databricks_account_id
  client_id     = var.databricks_account_client_id
  client_secret = var.databricks_acccount_client_secret
}
