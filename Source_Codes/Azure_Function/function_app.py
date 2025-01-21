import azure.functions as func
import logging
import sys
import os
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi
from azure.storage.blob import BlobServiceClient

# Add virtual environment's site-packages directory to sys.path
sys.path.append('/Users/yvonnelip/myenv/lib/python3.12/site-packages')

app = func.FunctionApp()

# Function to download and extract Kaggle dataset on a scheduled interval
@app.function_name(name="ScheduledKaggleDataExtractor")
@app.schedule(schedule="0 30 0 * * *", arg_name="mytimer", run_on_startup=False)
def scheduled_kaggle_data_extractor(mytimer: func.TimerRequest) -> None:
    logging.info('Scheduled KaggleDataExtractor function triggered at 13:30.')
    kaggle_data_extractor()

# Manual trigger function for downloading the dataset
@app.function_name(name="ManualKaggleDataExtractor")
@app.route(route="manually-trigger-kaggle-extractor")
def manual_kaggle_data_extractor(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Manual KaggleDataExtractor function triggered.')
    kaggle_data_extractor()
    return func.HttpResponse("Data extraction and upload completed successfully.", status_code=200)

# Function to download and extract Kaggle dataset
def kaggle_data_extractor():
    temp_dir = None
    try:
        # Kaggle API setup
        kaggle_config_dir = os.environ.get('KAGGLE_CONFIG_DIR', '/Users/yvonnelip/.kaggle') # Change the directory accounrding to your system
        kaggle_config_path = os.path.join(kaggle_config_dir, 'kaggle.json')
        if not os.path.exists(kaggle_config_path):
            raise FileNotFoundError(f"kaggle.json not found in {kaggle_config_dir}")

        api = KaggleApi()
        api.authenticate()

        # Download dataset
        dataset = os.environ.get('KAGGLE_DATASET', 'olistbr/brazilian-ecommerce')
        temp_dir = os.path.join(os.getcwd(), 'temp_download')
        os.makedirs(temp_dir, exist_ok=True)
        logging.info(f"Downloading dataset {dataset} to {temp_dir}")
        api.dataset_download_files(dataset, path=temp_dir, unzip=True)

        # Upload to Azure Blob Storage
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable not set")

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_name = os.environ.get('AZURE_STORAGE_CONTAINER', 'olist-store-data')

        for file_name in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, file_name)
            if os.path.isfile(file_path):
                blob_path = f"raw-data/{file_name}"
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
                with open(file_path, 'rb') as data:
                    logging.info(f'Uploading {file_name} to Azure Blob Storage.')
                    blob_client.upload_blob(data, overwrite=True)
                    logging.info(f'Successfully uploaded {file_name} to Azure Blob Storage in folder raw-data.')

        logging.info('Data extraction and upload completed successfully.')

    except FileNotFoundError as e:
        logging.error(f"File not found error: {e}")
    except ValueError as e:
        logging.error(f"Value error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logging.info(f'Removed temporary directory: {temp_dir}')
