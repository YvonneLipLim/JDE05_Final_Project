import os
from kaggle.api.kaggle_api_extended import KaggleApi

def download_kaggle_dataset():
    # Authenticate Kaggle API
    os.environ['KAGGLE_CONFIG_DIR'] = '/path/to/kaggle'
    api = KaggleApi()
    api.authenticate()

    # Download and extract dataset
    dataset = 'olistbr/brazilian-ecommerce'  # Actual dataset <owner>/<dataset-name>
    output_dir = '/Users/yvonnelip/Olist'    # Desired local directory
    api.dataset_download_files(dataset, path=output_dir, unzip=True)

if __name__ == "__main__":
    download_kaggle_dataset()