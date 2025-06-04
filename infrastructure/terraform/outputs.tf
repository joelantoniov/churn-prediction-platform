output "resource_group_name" {
  value = azurerm_resource_group.churn_prediction.name
}

output "storage_account_name" {
  value = azurerm_storage_account.data_lake.name
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.sql_server.fully_qualified_domain_name
}

output "aks_cluster_name" {
  value = azurerm_kubernetes_cluster.aks.name
}

output "eventhub_namespace_name" {
  value = azurerm_eventhub_namespace.kafka_namespace.name
}
