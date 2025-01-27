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
# MAGIC ### Read order_payments dataset from raw-data folder

# COMMAND ----------

order_payments = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/olist_order_payments_dataset.csv")


# COMMAND ----------

order_payments.show(10)
order_payments.printSchema()
order_payments.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data cleaning for order_payments dataset
# MAGIC - **Step 1: Import Libraries and Load Data**:
# MAGIC   - Necessary PySpark functions are imported.
# MAGIC   - The order_payments dataset is loaded from a CSV file.
# MAGIC
# MAGIC - **Step 2: Define Utility Function**:
# MAGIC   - print_section() is defined to print formatted section headers.
# MAGIC
# MAGIC - **Step 3: Initial Data Quality Check:**
# MAGIC   - Counts total rows in the original dataset.
# MAGIC   - Checks for missing values in each column.
# MAGIC   - Identify rows with zero values in key columns (payment_installments and payment_value).
# MAGIC
# MAGIC - **Step 4: Data Cleaning and Flagging**:
# MAGIC   - Creates a new DataFrame cleaned_order_payments.
# MAGIC   - Adds an is_problematic column to flag rows with zero installments or zero payment value.
# MAGIC   - Standardizes the payment_type column by capitalizing each word and replacing underscores with spaces.
# MAGIC   - Checks for duplicate rows.
# MAGIC   - Rounds the payment_value to 2 decimal places.
# MAGIC   - Casts columns to appropriate data types (IntegerType for payment_sequential and payment_installments, DoubleType for payment_value).
# MAGIC
# MAGIC - **Step 5: Cleaned Dataset Summary**:
# MAGIC   - Calculates the number of problematic and clean rows.
# MAGIC   - Computes percentages of clean vs problematic data.
# MAGIC   - Displays summary statistics of the cleaned dataset.
# MAGIC   - Shows unique payment types and their counts.
# MAGIC   - Displays the distribution of payment installments.
# MAGIC
# MAGIC - **Step 6: Sample Data Display**:
# MAGIC   - Shows a sample of the cleaned data, including problematic rows.
# MAGIC   - Displays a sample of problematic rows.
# MAGIC
# MAGIC - **Step 7: Final Result**:
# MAGIC   - Displays the first 25 rows of the cleaned dataset.

# COMMAND ----------

# This script performs data cleaning and analysis on the order_payments dataset.
# It identifies and flags problematic entries, standardizes data formats,
# and provides detailed insights into the dataset's quality and distribution.

# Import libraries
from pyspark.sql.functions import col, sum, regexp_replace, initcap, round, when, lit
from pyspark.sql.types import IntegerType, DoubleType

# Load the order_payments dataset from CSV file
order_payments = spark.read.csv("dbfs:/mnt/olist-store-data/raw-data/olist_order_payments_dataset.csv", header=True, inferSchema=True)

# Define a utility function to print formatted section headers
def print_section(title):
    print(f"\n{'='*50}\n{title}\n{'='*50}")

# Count total rows in the original dataset
total_rows = order_payments.count()

# Step 1: Perform initial data quality check
print_section("Data Quality Check")

# Check for missing values in each column
missing_values = order_payments.select(
    [sum(col(c).isNull().cast("int")).alias(c) for c in order_payments.columns]
)
display(missing_values)

# Identify rows with zero values in key columns
zero_installments = order_payments.filter(col("payment_installments") == 0).count()
zero_payment_value = order_payments.filter(col("payment_value") == 0).count()
print(f"Zero installments: {zero_installments}")
print(f"Zero payment value: {zero_payment_value}")

# Step 2: Clean and flag problematic data
print_section("Data Cleaning and Flagging")

# Create cleaned_order_payments DataFrame and flag problematic rows
cleaned_order_payments = order_payments.withColumn(
    "is_problematic",
    when((col("payment_installments") == 0) | (col("payment_value") == 0), True).otherwise(False)
)

# Standardize payment_type by capitalizing each word and replacing underscores with spaces
cleaned_order_payments = cleaned_order_payments.withColumn(
    "payment_type",
    initcap(regexp_replace(col("payment_type"), "_", " "))
)

# Check for duplicates rows
duplicates = cleaned_order_payments.groupBy(cleaned_order_payments.columns).count().filter(col("count") > 1)
print(f"Number of duplicate rows: {duplicates.count()}")

# Round payment_value to 2 decimal places for consistency
cleaned_order_payments = cleaned_order_payments.withColumn("payment_value", round(col("payment_value"), 2))

# Cast columns to appropriate data types 
cleaned_order_payments = cleaned_order_payments.withColumn("payment_sequential", col("payment_sequential").cast(IntegerType()))
cleaned_order_payments = cleaned_order_payments.withColumn("payment_installments", col("payment_installments").cast(IntegerType()))
cleaned_order_payments = cleaned_order_payments.withColumn("payment_value", col("payment_value").cast(DoubleType()))

# Step 3: Cleaned Dataset Summary
print_section("Cleaned Dataset Summary")

# Calculate the number of problematic and clean rows
problematic_rows = cleaned_order_payments.filter(col("is_problematic") == True).count()
clean_rows = total_rows - problematic_rows

# Calculate the percentage of clean vs problematic data
problematic_percentage = (problematic_rows / total_rows) * 100
clean_percentage = (clean_rows / total_rows) * 100

print(f"Total rows: {total_rows}")
print(f"Problematic rows flagged: {problematic_rows}")
print(f"Clean rows: {clean_rows}")
print(f"Percentage of problematic data: {problematic_percentage:.2f}%")
print(f"Percentage of clean data: {clean_percentage:.2f}%")

# Show summary statistics
display(cleaned_order_payments.describe())

# Show distribution of payment types
print("\nUnique payment types:")
display(cleaned_order_payments.groupBy("payment_type").count().orderBy("count", ascending=False))

# Show distribution of payment installments
print("\nDistribution of payment_installments:")
display(cleaned_order_payments.groupBy("payment_installments").count().orderBy("payment_installments"))

# Step 4: Sample Data Display
print("\nSample of cleaned data (including problematic rows):")
display(cleaned_order_payments.limit(5))

print("\nSample of problematic rows:")
display(cleaned_order_payments.filter(col("is_problematic") == True).limit(5))

# Step 5: Display final result
display(cleaned_order_payments.limit(25))

print("\nCleaning process completed.")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the cleaned_order_payments dataset

# COMMAND ----------

# Select only the specified columns
cleaned_order_payments = cleaned_order_payments.select(
    "order_id", 
    "payment_sequential", 
    "payment_type", 
    "payment_installments", 
    "payment_value"
)

# Verify final schema
print("\nFinal Schema:")
cleaned_order_payments.printSchema()

# Show sample of final dataset
print("\nSample of final cleaned dataset:")
cleaned_order_payments.show(5, truncate=False)

# Print final record count
print(f"\nTotal records in cleaned dataset: {cleaned_order_payments.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the cleaned_order_payments to a parquet file

# COMMAND ----------

# Define the output path
output_path = "/mnt/olist-store-data/transformed-data/olist_order_payments_cleaned_dataset_v2.0.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_parquet_output"

try:
    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (cleaned_order_payments
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
    cleaned_order_payments.unpersist()
