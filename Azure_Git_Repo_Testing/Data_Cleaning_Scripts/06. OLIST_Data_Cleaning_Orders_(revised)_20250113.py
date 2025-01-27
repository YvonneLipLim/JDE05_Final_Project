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
# MAGIC ### Read orders dataset from raw-data folder

# COMMAND ----------

orders = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/olist_orders_dataset.csv")


# COMMAND ----------

orders.show(10)
orders.printSchema()
display(orders.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data cleaning for orders dataset
# MAGIC - **Step 1: Data Loading and Initial Analysis**:  
# MAGIC   - Load the orders dataset from CSV file
# MAGIC   - Count initial number of records and columns
# MAGIC   - Analyze missing values for each column
# MAGIC   - Check for duplicate order IDs
# MAGIC
# MAGIC - **Step 2: Basic Cleaning Steps**:<br>
# MAGIC Remove rows with null values in critical columns:
# MAGIC   - order_id
# MAGIC   - customer_id
# MAGIC   - order_status
# MAGIC   - order_purchase_timestamp
# MAGIC
# MAGIC - **Step 3: Data Standardization**:<br>
# MAGIC Standardize order status values using initcap()
# MAGIC
# MAGIC - **Step 4: Data Type Conversion**:<br>
# MAGIC Convert timestamp columns to proper TimestampType:
# MAGIC   - order_purchase_timestamp
# MAGIC   - order_approved_at
# MAGIC   - order_delivered_carrier_date
# MAGIC   - order_delivered_customer_date
# MAGIC   - order_estimated_delivery_date
# MAGIC
# MAGIC - **Step 5: Feature Engineering**:<br>
# MAGIC Add derived columns:
# MAGIC   - processing_time: difference between order_approved_at and order_purchase_timestamp
# MAGIC   - delivery_time: difference between order_delivered_customer_date and order_approved_at
# MAGIC   - estimated_delivery_time: difference between order_estimated_delivery_date and order_purchase_timestamp
# MAGIC   - purchase_year: extracted from order_purchase_timestamp
# MAGIC   - purchase_month: extracted from order_purchase_timestamp
# MAGIC   - purchase_dayofweek: extracted from order_purchase_timestamp
# MAGIC
# MAGIC - **Step 6: Handling Inconsistencies**:<br>
# MAGIC Correct inconsistent order statuses:
# MAGIC   - Change "Delivered" to "Shipped" if order_delivered_customer_date is null
# MAGIC   - Change "Canceled" to "Delivered" if order_delivered_customer_date is not null
# MAGIC
# MAGIC - **Step 7: Performance Metrics**:<br>
# MAGIC Add delivery_performance column:
# MAGIC   - "On Time" if delivered on or before estimated date
# MAGIC   - "Delayed" if delivered after estimated date
# MAGIC   - "Canceled" if order status is canceled
# MAGIC   - "In Progress" for other cases
# MAGIC
# MAGIC - **Step 8: Outlier Detection**:
# MAGIC   - Calculate average and standard deviation for processing_time and delivery_time
# MAGIC   - Flag outliers (> 3 standard deviations from mean) for processing and delivery times
# MAGIC
# MAGIC - **Step 9: Data Validation and Summary**:
# MAGIC   - Analyze time ranges for processing and delivery
# MAGIC   - Show distribution of delivery performance and outliers
# MAGIC   - Calculate and display cleaning metrics (records removed, retention rate)
# MAGIC   - Show distribution of order statuses with percentages
# MAGIC
# MAGIC - **Step 10: Final Output**:
# MAGIC   - Display schema of cleaned dataset
# MAGIC   - Show sample of cleaned data

# COMMAND ----------

# This script performs data cleaning and analysis on the orders dataset.
# It identifies and flags problematic entries, standardizes data formats,
# adds derived features, and provides detailed insights into the dataset's quality and distribution.

# Import libraries
from pyspark.sql.functions import (
    col, sum, count, when, isnan, 
    to_timestamp, initcap, datediff, 
    year, month, dayofweek, expr,
    avg, stddev, min, max
)
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F

def clean_orders_dataset(spark):
    # Step 1: Data Loading and Initial Analysis
    print("Loading orders dataset...")
    orders = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/olist_orders_dataset.csv")
    
    # Store initial count
    initial_count = orders.count()
    
    # Print initial dataset info
    print("\nInitial dataset information:")
    print(f"Number of records: {initial_count:,}")
    print(f"Number of columns: {len(orders.columns)}")
    
    # Analyze missing values
    print("\nMissing values analysis:")
    missing_values = orders.select([
        sum(col(c).isNull().cast("int")).alias(c) for c in orders.columns
    ])
    
    # Calculate and display missing value percentages
    for column in orders.columns:
        missing_count = missing_values.collect()[0][column]
        missing_percentage = (missing_count / initial_count) * 100
        print(f"{column}: {missing_count:,} missing values ({missing_percentage:.2f}%)")
    
    # Display missing values table
    print("\nMissing values count:")
    missing_values.show()
    
    # Check for duplicate orders
    print("\nChecking for duplicate orders...")
    duplicates = orders.groupBy("order_id").count().filter(col("count") > 1)
    duplicate_count = duplicates.count()
    duplicate_percentage = (duplicate_count / initial_count) * 100
    print(f"Number of duplicate order_ids: {duplicate_count:,} ({duplicate_percentage:.2f}%)")
    
    print("\nStarting data cleaning process...")
    
    # Step 2: Basic Cleaning Steps
    cleaned_orders = orders.filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("order_status").isNotNull() &
        col("order_purchase_timestamp").isNotNull()
    )
    
    # Step 3: Data Standardization
    cleaned_orders = cleaned_orders.withColumn(
        "order_status",
        initcap(col("order_status"))
    )
    
    # Step 4: Data Type Conversion
    timestamp_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    
    for col_name in timestamp_columns:
        cleaned_orders = cleaned_orders.withColumn(
            col_name,
            to_timestamp(col(col_name))
        )
    
    # Step 5: Feature Engineering
    cleaned_orders = cleaned_orders.withColumn(
        "processing_time",
        when(
            col("order_approved_at").isNotNull(),
            datediff(col("order_approved_at"), col("order_purchase_timestamp"))
        ).otherwise(None)
    ).withColumn(
        "delivery_time",
        when(
            col("order_delivered_customer_date").isNotNull(),
            datediff(col("order_delivered_customer_date"), col("order_approved_at"))
        ).otherwise(None)
    ).withColumn(
        "estimated_delivery_time",
        datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp"))
    ).withColumn(
        "purchase_year",
        year(col("order_purchase_timestamp"))
    ).withColumn(
        "purchase_month",
        month(col("order_purchase_timestamp"))
    ).withColumn(
        "purchase_dayofweek",
        dayofweek(col("order_purchase_timestamp"))
    )
    
    # Calculate time statistics for outlier detection
    time_stats = cleaned_orders.select(
        avg("processing_time").alias("avg_processing_time"),
        stddev("processing_time").alias("stddev_processing_time"),
        avg("delivery_time").alias("avg_delivery_time"),
        stddev("delivery_time").alias("stddev_delivery_time"),
        min("purchase_year").alias("min_year"),
        max("purchase_year").alias("max_year")
    ).collect()[0]
    
    # Step 6: Handling Inconsistencies
    cleaned_orders = cleaned_orders.withColumn(
        "order_status",
        when(
            (col("order_status") == "Delivered") & col("order_delivered_customer_date").isNull(),
            "Shipped"
        ).when(
            (col("order_status") == "Canceled") & col("order_delivered_customer_date").isNotNull(),
            "Delivered"
        ).otherwise(col("order_status"))
    )
    
    # Step 7: Performance Metrics
    cleaned_orders = cleaned_orders.withColumn(
        "delivery_performance",
        when(
            col("order_delivered_customer_date").isNotNull(),
            when(
                col("order_delivered_customer_date") <= col("order_estimated_delivery_date"),
                "On Time"
            ).otherwise("Delayed")
        ).when(
            col("order_status") == "Canceled",
            "Canceled"
        ).otherwise("In Progress")
    )
    
    # Step 8: Outlier Detection
    cleaned_orders = cleaned_orders.withColumn(
        "is_processing_outlier",
        when(
            col("processing_time").isNotNull(),
            col("processing_time") > (time_stats.avg_processing_time + 3 * time_stats.stddev_processing_time)
        ).otherwise(False)
    ).withColumn(
        "is_delivery_outlier",
        when(
            col("delivery_time").isNotNull(),
            col("delivery_time") > (time_stats.avg_delivery_time + 3 * time_stats.stddev_delivery_time)
        ).otherwise(False)
    )
    
    # Step 9: Data Validation and Summary
    print("\nTime Range Analysis:")
    print(f"Processing Time (days) - Average: {time_stats.avg_processing_time:.2f}, StdDev: {time_stats.stddev_processing_time:.2f}")
    print(f"Delivery Time (days) - Average: {time_stats.avg_delivery_time:.2f}, StdDev: {time_stats.stddev_delivery_time:.2f}")
    print(f"Purchase Year Range: {time_stats.min_year} to {time_stats.max_year}")

    # Print Status and Performance Distribution
    print("\nDelivery Performance Distribution:")
    cleaned_orders.groupBy("delivery_performance").count().orderBy("count", ascending=False).show()
    
    print("\nOutlier Distribution:")
    print("Processing Time Outliers:")
    cleaned_orders.groupBy("is_processing_outlier").count().orderBy("is_processing_outlier").show()
    print("Delivery Time Outliers:")
    cleaned_orders.groupBy("is_delivery_outlier").count().orderBy("is_delivery_outlier").show()
    
    # Calculate and display cleaning metrics
    final_count = cleaned_orders.count()
    removed_count = initial_count - final_count
    retention_rate = (final_count / initial_count) * 100
    removal_rate = (removed_count / initial_count) * 100
    
    # Print cleaning summary with percentages
    print("\nCleaning Summary:")
    print(f"Original record count: {initial_count:,}")
    print(f"Cleaned record count: {final_count:,}")
    print(f"Records removed: {removed_count:,}")
    print(f"Data retention rate: {retention_rate:.2f}%")
    print(f"Data removal rate: {removal_rate:.2f}%")
    
    # Show distribution of order statuses with percentages
    status_counts = cleaned_orders.groupBy("order_status").count().orderBy("count", ascending=False)
    total_records = cleaned_orders.count()
    
    print("\nOrder Status Distribution:")
    status_counts_with_pct = status_counts.withColumn(
        "percentage",
        F.round((F.col("count") / total_records) * 100, 2)
    )
    status_counts_with_pct.show()
    
    # Step 10: Final Output
    print("\nCleaned Dataset Schema:")
    cleaned_orders.printSchema()
    
    # Show sample of cleaned data
    print("\nSample of Cleaned Data:")
    cleaned_orders.show(5, truncate=True)
    
    # Display final result
    display(cleaned_orders.limit(10))

    print("\nCleaning process completed.")
    return cleaned_orders

# Execute the cleaning process
if __name__ == "__main__":
    cleaned_orders = clean_orders_dataset(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the cleaned_orders_withCalculations dataset

# COMMAND ----------

# Verify final schema
print("\nFinal Schema:")
cleaned_orders.printSchema()

# Show sample of final dataset
print("\nSample of final cleaned dataset:")
cleaned_orders.display(5, truncate=False)

# Print final record count
print(f"\nTotal records in cleaned dataset: {cleaned_orders.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the cleaned_orders_withCalculations dataset in parquet file

# COMMAND ----------

# Define the output path for final cleaned and translated dataset
output_path = "/mnt/olist-store-data/transformed-data/olist_orders_cleaned_dataset_withCalculations.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_parquet_output"

try:
    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (cleaned_orders
     .repartition(1)  # Force to a single partition
     .write
     .mode("overwrite")
     .parquet(temp_path))

    # Find the Parquet file in temp directory
    temp_files = dbutils.fs.ls(temp_path)
    parquet_file = [f.path for f in temp_files if f.path.endswith(".parquet")][0]
    
    # Move to final location
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
    cleaned_orders.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the cleaned_orders dataset with selected columns

# COMMAND ----------

# Select only the specified columns
cleaned_orders = cleaned_orders.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
)

# Verify final schema
print("\nFinal Schema:")
cleaned_orders.printSchema()

# Show sample of final dataset
print("\nSample of final cleaned dataset:")
display(cleaned_orders.limit(10), truncate=False)

# Print final record count
print(f"\nTotal records in cleaned dataset: {cleaned_orders.count():,}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the cleaned_orders to a parquet file

# COMMAND ----------

# Define the output path
output_path = "/mnt/olist-store-data/transformed-data/olist_orders_cleaned_dataset_v2.0.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_parquet_output"

try:
    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (cleaned_orders
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
    cleaned_orders.unpersist()
