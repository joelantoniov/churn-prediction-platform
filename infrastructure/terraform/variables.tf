variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "rg-churn-prediction"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US"
}

variable "storage_account_name" {
  description = "Name of the storage account"
  type        = string
  default     = "sachurnprediction"
}

variable "sql_server_name" {
  description = "Name of the SQL server"
  type        = string
  default     = "sql-churn-prediction"
}

variable "sql_admin_username" {
  description = "SQL Server admin username"
  type        = string
  default     = "sqladmin"
}

variable "sql_admin_password" {
  description = "SQL Server admin password"
  type        = string
  sensitive   = true
}

variable "aks_cluster_name" {
  description = "Name of the AKS cluster"
  type        = string
  default     = "aks-churn-prediction"
}

variable "eventhub_namespace_name" {
  description = "Name of the EventHub namespace"
  type        = string
  default     = "ehns-churn-prediction"
}
