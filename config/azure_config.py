import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

class AzureConfig:
    STORAGE_ACCOUNT_NAME = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
    CONTAINER_NAME = os.getenv('AZURE_CONTAINER_NAME', 'churn-data')
    BLOB_ENDPOINT = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    
    @staticmethod
    def get_blob_client():
        credential = DefaultAzureCredential()
        return BlobServiceClient(
            account_url=AzureConfig.BLOB_ENDPOINT,
            credential=credential
        )
