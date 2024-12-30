import azure.functions as func
import logging
import os
from kaggle.api.kaggle_api_extended import KaggleApi
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()  # Create the FunctionApp instance

# Function to download and extract Kaggle dataset on a scheduled interval
@app.function_name(name="ScheduledKaggleDataExtractor")
@app.schedule(schedule="0 0 0 * * *",  # Daily at midnight UTC
              arg_name="mytimer",
              run_on_startup=False)
def scheduled_kaggle_data_extractor(mytimer: func.TimerRequest) -> None:
    logging.info('Scheduled KaggleDataExtractor function triggered.')
    kaggle_data_extractor()

# Function to download and extract Kaggle dataset on manual basis
@app.function_name(name="ManualKaggleDataExtractor")
@app.route(route="manually-trigger-kaggle-extractor")  # One-off trigger
def manual_kaggle_data_extractor(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Manual KaggleDataExtractor function triggered.')
    kaggle_data_extractor()
    return func.HttpResponse("Data extraction and upload completed successfully.", status_code=200)

def kaggle_data_extractor():
    try:
        # Kaggle API setup
        os.environ['KAGGLE_CONFIG_DIR'] = '/Users/yvonnelip/.kaggle'  # Update if needed
        kaggle_config_path = os.path.join(os.environ['KAGGLE_CONFIG_DIR'], 'kaggle.json')
        if not os.path.exists(kaggle_config_path):
            raise FileNotFoundError(f"kaggle.json not found in {os.environ['KAGGLE_CONFIG_DIR']}")

        api = KaggleApi()
        api.authenticate()

        # Download dataset
        dataset = 'olistbr/brazilian-ecommerce'  # Replace with Kaggle dataset path
        local_path = '/Users/yvonnelip/Olist'    # Replace with your local path
        os.makedirs(local_path, exist_ok=True)
        logging.info(f"Downloading dataset {dataset} to {local_path}")
        api.dataset_download_files(dataset, path=local_path, unzip=True)

        # Upload to Azure Blob Storage
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable not set")

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_name = 'olist-store-data'

        for file_name in os.listdir(local_path):
            file_path = os.path.join(local_path, file_name)
            if os.path.isfile(file_path):  # Ensure it's a file
                # Upload to raw-data folder within the container
                blob_path = f"raw-data/{file_name}"  # Define folder structure in blob path
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
                with open(file_path, 'rb') as data:
                    logging.info(f'Starting upload of {file_name} to Azure Blob Storage.')
                    blob_client.upload_blob(data, overwrite=True)
                    logging.info(f'Successfully uploaded {file_name} to Azure Blob Storage in folder raw-data.')
               
                    logging.info(f'Uploaded {file_name} to Azure Blob Storage in folder raw-data.')
                    
                    # Remove local file(s) after successful upload
                    os.remove(file_path)
                    logging.info(f'Removed local file: {file_name}')
                    
        logging.info('Data extraction and upload completed.')
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise # Re-raise the exception to be caught by the calling function
