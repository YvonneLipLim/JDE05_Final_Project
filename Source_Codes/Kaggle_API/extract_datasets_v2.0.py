import os
from kaggle.api.kaggle_api_extended import KaggleApi
from azure.storage.blob import BlobServiceClient
from pathlib import Path

def download_and_upload_dataset():
    # Kaggle Authentication
    api = KaggleApi()
    api.authenticate()

    # Azure Storage Account credentials
    connection_string = <YOUR_AZURE_STORAGE_CONNECTION_STRING"
    container_name = <YOUR_STORAGE_CONTAINER_NAME"
    blob_prefix = "raw-data/" # Specific folder
    
    # Set up local directory structure
    base_dir = './Olist'
    raw_data_dir = os.path.join(base_dir, 'raw-data')

    # Create output directory if it doesn't exist
    os.makedirs(raw_data_dir, exist_ok=True)
    print(f"Downloading dataset to {raw_data_dir}...")
    api.dataset_download_files(dataset='olistbr/brazilian-ecommerce', path=raw_data_dir, unzip=True)

    # Initialize Azure Blob Service Client
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        # Upload each file to Azure Storage
        for file_path in Path(raw_data_dir).glob('*.csv'):
            print(f"Uploading {file_path.name} to Azure Storage...")
            
            # Create blob client for each file
            blob_name = f"{blob_prefix}{file_path.name}"
            blob_client = container_client.get_blob_client(blob_name)
            
            # Upload file
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            
            print(f"Successfully uploaded {file_path.name}")

        print("All files uploaded successfully!")

    except Exception as e:
        print(f"Error uploading to Azure Storage: {str(e)}")

if __name__ == "__main__":
    # First, install required packages if not already installed
    # pip install kaggle azure-storage-blob

    download_and_upload_dataset()
