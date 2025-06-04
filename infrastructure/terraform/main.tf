terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "churn_prediction" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_storage_account" "data_lake" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.churn_prediction.name
  location                 = azurerm_resource_group.churn_prediction.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled          = true
}

resource "azurerm_storage_container" "data_container" {
  name                  = "churn-data"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_mssql_server" "sql_server" {
  name                         = var.sql_server_name
  resource_group_name          = azurerm_resource_group.churn_prediction.name
  location                     = azurerm_resource_group.churn_prediction.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = var.sql_admin_password
}

resource "azurerm_mssql_database" "churn_db" {
  name           = "churn_predictions"
  server_id      = azurerm_mssql_server.sql_server.id
  collation      = "SQL_Latin1_General_CP1_CI_AS"
  license_type   = "LicenseIncluded"
  max_size_gb    = 100
  sku_name       = "S2"
  zone_redundant = false
}

resource "azurerm_kubernetes_cluster" "aks" {
  name                = var.aks_cluster_name
  location            = azurerm_resource_group.churn_prediction.location
  resource_group_name = azurerm_resource_group.churn_prediction.name
  dns_prefix          = "churnprediction"

  default_node_pool {
    name       = "default"
    node_count = 3
    vm_size    = "Standard_D4s_v3"
  }

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Environment = "Production"
    Project     = "ChurnPrediction"
  }
}

resource "azurerm_eventhub_namespace" "kafka_namespace" {
  name                = var.eventhub_namespace_name
  location            = azurerm_resource_group.churn_prediction.location
  resource_group_name = azurerm_resource_group.churn_prediction.name
  sku                 = "Standard"
  capacity            = 2
}

resource "azurerm_eventhub" "customer_events" {
  name                = "customer-events"
  namespace_name      = azurerm_eventhub_namespace.kafka_namespace.name
  resource_group_name = azurerm_resource_group.churn_prediction.name
  partition_count     = 8
  message_retention   = 7
}
