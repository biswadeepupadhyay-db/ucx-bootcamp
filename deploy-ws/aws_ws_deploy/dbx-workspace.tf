resource "databricks_mws_workspaces" "this" {
  provider       = databricks.mws
  account_id     = var.databricks_account_id
  aws_region     = var.deployment_region
  workspace_name = local.prefix

  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
  network_id               = databricks_mws_networks.this.network_id

  token {
    comment = "Terraform"
  }
}


# # Add a user to the Databricks Workspace
# resource "databricks_user" "admin_user" {
#   user_name = var.workspace_admin  # Replace with the user's email address
#   provider = databricks.accounts
# }

# data "databricks_user" "existing_user" {
#   user_name = var.workspace_admin
#   provider = databricks.accounts
# }


# resource "databricks_mws_permission_assignment" "add_user_to_workspace" {
#   workspace_id = azurerm_databricks_workspace.databricks.workspace_id
#   principal_id = data.databricks_user.existing_user.id
#   permissions  = ["USER"]
# }

# Assign workspace admin role to the user
# resource "databricks_mws_permission_assignment" "add_user_to_workspace" {
#   workspace_id = databricks_mws_workspaces.this.workspace_id
#   principal_id = data.databricks_user.existing_user.id
#   permissions  = ["USER"]
#   provider = databricks.mws
# }


output "databricks_host" {
  value = databricks_mws_workspaces.this.workspace_url
}

output "databricks_token" {
  value     = databricks_mws_workspaces.this.token[0].token_value
  sensitive = true
}