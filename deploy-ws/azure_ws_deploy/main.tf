terraform {
  required_providers {
    azurerm = "~> 2.33"
  }
}

provider "azurerm" {
  features {}
}

# # Get the current timestamp
# locals {
#   current_date = timestamp()
  
#   # Add 30 days to the current date
#   future_date = timeadd(timestamp(), "720h")  # 30 days = 720 hours
# }

# # Output to verify the results
# output "current_date" {
#   value = local.current_date
# }

# output "future_date" {
#   value = local.future_date
# }

resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
}

data "azurerm_client_config" "current" {
}

data "external" "me" {
  program = ["az", "account", "show", "--query", "user"]
}

locals {
  prefix = "${var.deployment_keyword}-${random_string.naming.result}"
  tags = {
    Environment = "Demo"
    Owner       = lookup(data.external.me.result, "name")
    RemoveAfter = substr(timeadd(timestamp(), "720h"), 0, 10)
  }
}

// creates resource group
resource "azurerm_resource_group" "this" {
  name     = "${local.prefix}-rg"
  location = var.deployment_region
  tags     = local.tags
}

// deploys databricks workspace
resource "azurerm_databricks_workspace" "this" {
  name                        = "${local.prefix}-workspace"
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = "premium"
  managed_resource_group_name = "${local.prefix}-workspace-rg"
  tags                        = local.tags
}

output "databricks_host" {
  value = "https://${azurerm_databricks_workspace.this.workspace_url}/"
}