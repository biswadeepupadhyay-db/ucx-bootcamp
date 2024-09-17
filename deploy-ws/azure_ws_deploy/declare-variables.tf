// declaring variable definitions
variable "databricks_account_console_host" {
  description = "Databricks Account Console URL"
  type = string
}

variable "databricks_account_id" {
  type = string
}

variable "deployment_region" {
  type    = string
  default = "centralus"
}

variable "deployment_keyword" {
  type    = string
  default = "databricksdemo"
}