# Databricks notebook source
# MAGIC %md
# MAGIC ### Mounting the container into Databricks

# COMMAND ----------

# Install required packages
%pip install azure-identity azure-keyvault-secrets

# COMMAND ----------

# Restart Python interpreter to ensure new packages are loaded
%restart_python

# COMMAND ----------

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Key Vault configuration
key_vault_url = "https://Olist-Key.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=key_vault_url, credential=credential)

# Retrieve secrets from Key Vault
client_id = client.get_secret("olist-client-id").value
client_secret = client.get_secret("olist-client-secret").value
tenant_id = client.get_secret("olist-tenant-id").value

# Unmount the existing mount point if it exists
dbutils.fs.unmount("/mnt/olist-store-data")

# Create the configurations
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Mount the storage
dbutils.fs.mount(
    source="abfss://olist-store-data@olistbrdata.dfs.core.windows.net",
    mount_point="/mnt/olist-store-data",
    extra_configs=configs
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check mounting of the storage-account container

# COMMAND ----------

# Check if the mounting is successful or not
dbutils.fs.ls("/mnt/olist-store-data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read order_items dataset from raw-data folder

# COMMAND ----------

order_items = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/olist_order_items_dataset.csv")

# COMMAND ----------

order_items.show(10)
order_items.printSchema()
order_items.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data cleaning for order_items dataset
# MAGIC - **Step 1: Apply skimming_data(df) function**:<br>
# MAGIC It provides a comprehensive overview of the DataFrame. It:
# MAGIC   - Prints the schema of the DataFrame
# MAGIC   - Counts the number of rows and columns
# MAGIC   - Calculates and displays the count and percentage of missing values for each column
# MAGIC   - Lists the data types of each column
# MAGIC   - Provides summary statistics for numeric columns
# MAGIC
# MAGIC - **Step 2: Apply calculate_percentage(df, column_name) function:**<br>
# MAGIC It calculates the percentage distribution of values in a specified column. It:
# MAGIC   - Counts the total number of rows in the DataFrame
# MAGIC   - Groups the data by the specified column and counts occurrences
# MAGIC   - Calculates the percentage for each group
# MAGIC
# MAGIC - **Step 3: Data Cleaning Steps**:
# MAGIC   - dropDuplicates(): Removes duplicate rows from the DataFrame
# MAGIC   - filter(): Used to remove invalid data based on conditions:
# MAGIC       - (col("price") > 0) & (col("freight_value") >= 0): Removes rows with non-positive prices or negative freight values
# MAGIC       - col("shipping_limit_date") >= threshold_date: Removes rows with shipping dates older than the threshold
# MAGIC
# MAGIC - **Step 4: Data Transformation**:
# MAGIC   - withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date"))): Converts the shipping_limit_date column to a timestamp format
# MAGIC
# MAGIC - **Step 5: Analysis Functions**:
# MAGIC   - count(): Used to count the number of rows at various stages of cleaning
# MAGIC   - exceptAll(): Used to find rows that were removed during deduplication
# MAGIC   - display(): Used to show samples of the data at different stages

# COMMAND ----------

# This script performs data cleaning and analysis on the order_items dataset.
# It removes duplicates, filters out invalid entries, and standardizes date formats.
# The script also provides detailed analysis of the cleaning process and its results.

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, current_timestamp, when, count, round, to_timestamp, isnan, isnull
from pyspark.sql.types import NumericType
from datetime import datetime, timedelta

# Create SparkSession (if not already created)
spark = SparkSession.builder.appName("OrderItemsCleaning").getOrCreate()

# Step 1: Define a function to provide an overview of the DataFrame
def skimming_data(df):
    """
    This function analyzes and displays key information about the DataFrame:
    - Schema
    - Row and column counts
    - Missing value analysis
    - Data types
    - Summary statistics for numeric columns
    """
    print("DataFrame Info:")
    df.printSchema()
    print(f"Number of rows: {df.count()}")
    print(f"Number of columns: {len(df.columns)}")
    
    print("\nMissing Values:")
    for column in df.columns:
        if df.schema[column].dataType in [NumericType()]:
            missing_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        else:
            missing_count = df.filter(isnull(col(column))).count()
        missing_percentage = (missing_count / df.count()) * 100
        print(f"{column}: {missing_count} ({missing_percentage:.2f}%)")
    
    print("\nData Types:")
    for column, data_type in df.dtypes:
        print(f"{column}: {data_type}")
    
    print("\nSummary Statistics:")
    numeric_columns = [c for c, d in df.dtypes if d in ['int', 'double', 'float']]
    if numeric_columns:
        df.select(numeric_columns).describe().show()
    else:
        print("No numeric columns to summarize.")

# Load the order_items dataset
order_items = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/olist_order_items_dataset.csv")

# Display an overview of the original DataFrame
print("Original Data Overview:")
skimming_data(order_items)

# Step 2: Define a function to calculate percentage distribution of values in a column
def calculate_percentage(df, column_name):
    """
    This function calculates and returns the percentage distribution of values in a specified column.
    """
    total = df.count()
    count_df = df.groupBy(column_name).agg(count("*").alias("count"))
    return count_df.withColumn("percentage", round(col("count") / total * 100, 2))

# Record the original number of rows
total_rows = order_items.count()
print(f"\nOriginal rows: {total_rows}")

# Step 3: Start the cleaning process
# Remove duplicate rows
deduped_df = order_items.dropDuplicates()
deduped_rows = deduped_df.count()
print(f"Rows after deduplication: {deduped_rows}")

# Remove rows with invalid prices (<=0) or freight values (<0)
valid_price_freight_df = deduped_df.filter((col("price") > 0) & (col("freight_value") >= 0))
valid_price_freight_rows = valid_price_freight_df.count()
print(f"Rows after removing invalid prices and freight values: {valid_price_freight_rows}")

# Convert shipping_limit_date to timestamp format
valid_price_freight_df = valid_price_freight_df.withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date")))

# Set a reasonable threshold, e.g., dates from 2016 to 2018
start_date = datetime(2016, 1, 1)
end_date = datetime(2018, 12, 31)

# Remove rows with invalid shipping dates
valid_dates_df = valid_price_freight_df.filter(
    (col("shipping_limit_date") >= start_date) & 
    (col("shipping_limit_date") <= end_date)
)
valid_dates_rows = valid_dates_df.count()
print(f"Rows after removing invalid shipping dates: {valid_dates_rows}")

# Final cleaned data
cleaned_order_items = valid_dates_df
cleaned_rows = cleaned_order_items.count()
print(f"Final cleaned rows: {cleaned_rows}")

# Step 4: Calculate and display cleaning statistics
unclean_rows = total_rows - cleaned_rows
print("\nCleaning Summary:")
print(f"Total rows: {total_rows}")
print(f"Cleaned rows: {cleaned_rows}")
print(f"Unclean rows: {unclean_rows}")
print(f"Percentage of clean data: {cleaned_rows/total_rows*100:.2f}%")
print(f"Percentage of unclean data: {unclean_rows/total_rows*100:.2f}%")

# Detailed analysis of removed rows
print("\nAnalysis of removed rows:")
print(f"Rows removed by deduplication: {total_rows - deduped_rows}")
print(f"Rows removed due to invalid prices or freight values: {deduped_rows - valid_price_freight_rows}")
print(f"Rows removed due to invalid shipping dates: {valid_price_freight_rows - valid_dates_rows}")

# Step 5: Perform detailed analysis of removed rows
print("\nSample of duplicate rows:")
display(order_items.exceptAll(deduped_df).limit(5))

print("\nSample of rows with invalid prices or freight values:")
display(deduped_df.filter((col("price") <= 0) | (col("freight_value") < 0)).limit(5))

print("\nSample of rows with invalid shipping dates:")
display(valid_price_freight_df.filter((col("shipping_limit_date") < start_date) | (col("shipping_limit_date") > end_date)).limit(5))

# Step 6: Display samples of removed and cleaned data
print("\nSample of cleaned data:")
display(cleaned_order_items.limit(5))

# Step 7: Display an overview of the cleaned DataFrame
print("\nCleaned Data Overview:")
skimming_data(cleaned_order_items)

# Display final result
display(cleaned_order_items.limit(25))

print("\nCleaning process completed.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the cleaned_order_items dataset

# COMMAND ----------

# Verify final schema
print("\nFinal Schema:")
cleaned_order_items.printSchema()

# Show sample of final dataset
print("\nSample of final cleaned dataset:")
cleaned_order_items.show(5, truncate=False)

# Print final record count
print(f"\nTotal records in cleaned dataset: {cleaned_order_items.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the cleaned_order_items to a parquet file

# COMMAND ----------

# Define the output path
output_path = "/mnt/olist-store-data/transformed-data/olist_order_items_cleaned_dataset_v2.0.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_parquet_output"

try:
    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (cleaned_order_items
     .repartition(1)  # Force to a single partition
     .write
     .mode("overwrite")
     .parquet(temp_path))

    # Find the Parquet file in temp directory
    temp_files = dbutils.fs.ls(temp_path)
    parquet_file = [f.path for f in temp_files if f.path.endswith(".parquet")][0]
    
    # Move to final location with correct filename
    dbutils.fs.mv(parquet_file, output_path)
    
    # Clean up temp directory
    dbutils.fs.rm(temp_path, recurse=True)

    # Verify the saved Parquet file
    verified_df = spark.read.parquet(output_path)
    print("\nVerification of saved Parquet file:")
    print(f"Number of rows in saved Parquet file: {verified_df.count():,}")
    print("\nSample of saved data:")
    display(verified_df.limit(5))

    # Verify it's a single file
    if len(dbutils.fs.ls(output_path)) == 1:
        print("\nSuccessfully saved as a single Parquet file.")
    else:
        print("\nWarning: Multiple files were created.")

except Exception as e:
    print(f"Error saving dataset: {str(e)}")
    # Clean up temp directory in case of failure
    dbutils.fs.rm(temp_path, recurse=True)
    raise
finally:
    # Unpersist cached DataFrame
    cleaned_order_items.unpersist()
