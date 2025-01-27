# Databricks notebook source
# MAGIC %md
# MAGIC ### Mounting the container into Databricks

# COMMAND ----------

# Install required packages
%pip install azure-identity azure-keyvault-secrets
%pip install google-cloud-translate==2.0.1
%pip install --upgrade google-cloud-translate
%pip list | grep google-cloud-translate

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
# MAGIC ### Read order_reviews dataset from raw-data folder

# COMMAND ----------

order_reviews = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/olist_order_reviews_dataset.csv")

# COMMAND ----------

order_reviews.show(10)
order_reviews.printSchema()
display(order_reviews.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the file location of the Google Credentials JSON file

# COMMAND ----------

import os

# Check Google Cloud credentials file
paths_to_check = [
    '/dbfs/FileStore/Google_Cloud-JSON',
    '/Workspace/Google_Cloud-JSON',
    '/dbfs/Workspace/Google_Cloud-JSON'
]

for path in paths_to_check:
    exists = os.path.exists(path)
    print(f"Path {path}: {'Exists' if exists else 'Does not exist'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Google Credentials JSON file

# COMMAND ----------

import json

# Read the file contents
with open('/Workspace/Google_Cloud-JSON', 'r') as file:
    credentials = json.load(file)

# Mask sensitive information
masked_credentials = credentials.copy()
masked_credentials['private_key_id'] = '****MASKED****'
masked_credentials['private_key'] = '****MASKED****'
masked_credentials['client_email'] = '****MASKED****'
masked_credentials['client_id'] = '****MASKED****'

# Print the masked credentials
print(json.dumps(masked_credentials, indent=2))


# COMMAND ----------

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import json

key_vault_url = "https://Olist-Key.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=key_vault_url, credential=credential)

# Retrieve secrets
private_key_id = client.get_secret("gcp-private-key-id").value
private_key = client.get_secret("gcp-private-key").value
client_id = client.get_secret("gcp-client-id").value
client_email = client.get_secret("gcp-client-email").value

# Reconstruct credentials
credentials = {
  "type": "service_account",
  "project_id": "arctic-idiom-447208-f1",
  "private_key_id": private_key_id,
  "private_key": private_key,
  "client_email": client_email,
  "client_id": client_id,
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/databricks-translation%40arctic-idiom-447208-f1.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

# Create directory if it doesn't exist
dbutils.fs.mkdirs("dbfs:/FileStore/google_credentials/")

# Write the credentials to a file in DBFS
with open('/dbfs/FileStore/google_credentials/google_cloud_credentials.json', 'w') as f:
    json.dump(credentials, f)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set the Google_Application_Credentials environment variable

# COMMAND ----------

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Workspace/Google_Cloud-JSON'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Google Translation API

# COMMAND ----------

from google.cloud import translate_v2

try:
    translate_client = translate_v2.Client()
    # Test translation
    result = translate_client.translate('Hello', target_language='pt')
    print("Translation successful:", result)
except Exception as e:
    print("Error:", str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data cleaning for order_reviews dataset
# MAGIC - **Step 1: Initial Data Quality Assessment**
# MAGIC   - Loads the CSV file and performs an initial row count
# MAGIC   - Checks for missing values in all columns using isNull()
# MAGIC   - Uses a custom skimming_data function to:
# MAGIC     - Display the schema/data types
# MAGIC     - Show sample data
# MAGIC     - Calculate null value percentages for each column
# MAGIC
# MAGIC - **Step 2: Basic Data Type Conversions**
# MAGIC   - Converts review_score to integer type using cast(IntegerType())
# MAGIC   - Converts date columns (review_creation_date and review_answer_timestamp) to timestamp format
# MAGIC   - Trims whitespace from string columns (review_id, order_id, review_comment_title, review_comment_message)
# MAGIC
# MAGIC - **Step 3: Text Field Normalization**<br>
# MAGIC For review comments and titles:
# MAGIC   - Converts text to lowercase
# MAGIC   - Removes special characters using regex
# MAGIC   - Keeps only alphanumeric characters and spaces
# MAGIC
# MAGIC - **Step 4: Null and Empty Value Handling**
# MAGIC   - Removes rows with null review_id or order_id
# MAGIC   - Creates a boolean flag 'has_comment' to identify reviews with non-empty comments
# MAGIC   - Checks and logs remaining null values after cleaning
# MAGIC
# MAGIC - **Step 5: Duplicate Management**
# MAGIC   - Identifies duplicate rows across all columns
# MAGIC   - Logs duplicate entries to a CSV file for reference
# MAGIC   - Removes duplicates using dropDuplicates()
# MAGIC
# MAGIC - **Step 6: Data Validation and Constraints**
# MAGIC   - Ensures review_score is between 1 and 5
# MAGIC   - Performs outlier detection on review_score using z-score method
# MAGIC   - Validates dates:
# MAGIC     - Ensures review_creation_date is not in the future
# MAGIC     - Confirms review_answer_timestamp is after review_creation_date
# MAGIC
# MAGIC - **Step 7: Quality Metrics Calculation**
# MAGIC   - Tracks problematic rows based on criteria like:
# MAGIC     - Null values in critical fields
# MAGIC     - Invalid date relationships
# MAGIC     - Incorrect ID lengths (expecting 32 characters)
# MAGIC   - Calculates and reports:
# MAGIC     - Total rows processed
# MAGIC     - Number of problematic rows
# MAGIC     - Percentage of clean vs. problematic data
# MAGIC
# MAGIC - **Step 8: Final Data Export**
# MAGIC   - Saves the cleaned dataset as a single Parquet file
# MAGIC   - Implements error handling and cleanup of temporary files
# MAGIC   - Verifies the saved data by:
# MAGIC     - Checking row count
# MAGIC     - Sampling the saved data
# MAGIC     - Confirming single file output
# MAGIC
# MAGIC - **Step 9: Performance Optimization**
# MAGIC   - Uses caching for the DataFrame after major transformations
# MAGIC   - Properly unpersists cached data when finished
# MAGIC   - Implements logging throughout the process for monitoring

# COMMAND ----------

# Import libraries
from pyspark.sql.functions import col, sum, to_timestamp, current_date, when, trim, length, lit, count, round, mean, stddev, lower, regexp_replace, abs
from pyspark.sql.types import IntegerType
import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Function to print section headers
def print_section(title):
    logger.info(f"\n{'='*50}\n{title}\n{'='*50}")

# Function to skim data 
def skimming_data(df):
    logger.info("Data Overview:")
    df.printSchema()
    logger.info("\nSample Data:")
    df.show(5, truncate=False)
    logger.info("\nColumn Statistics:")
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        total_count = df.count()
        null_percentage = (null_count / total_count) * 100
        logger.info(f"{col_name}: {null_percentage:.2f}% null values")

try:
    # Load the dataset
    order_reviews = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/olist_order_reviews_dataset.csv")
    initial_count = order_reviews.count()
    logger.info(f"Initial row count: {initial_count}")

    print_section("Initial Data Quality Check")

    # Check for missing values in each column
    missing_values = order_reviews.select([sum(col(c).isNull().cast("int")).alias(c) for c in order_reviews.columns])
    missing_values.show()

    # Call the skimming_data function
    skimming_data(order_reviews)

    print_section("Data Cleaning")

    # Convert review_score to integer
    order_reviews = order_reviews.withColumn("review_score", col("review_score").cast(IntegerType()))

    # Convert date columns to timestamp
    order_reviews = order_reviews.withColumn("review_creation_date", to_timestamp(col("review_creation_date")))
    order_reviews = order_reviews.withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp")))

    # Trim whitespace from string columns
    for column in ["review_id", "order_id", "review_comment_title", "review_comment_message"]:
        order_reviews = order_reviews.withColumn(column, trim(col(column)))

    # Normalize text fields
    for column in ["review_comment_title", "review_comment_message"]:
        order_reviews = order_reviews.withColumn(column, lower(regexp_replace(col(column), "[^a-zA-Z0-9\\s]", "")))

    # Remove rows with NULL review_id or order_id
    order_reviews = order_reviews.filter(col("review_id").isNotNull() & col("order_id").isNotNull())

    # Flag reviews with NULL or empty comment messages
    order_reviews = order_reviews.withColumn("has_comment", when((col("review_comment_message").isNotNull()) & (length(trim(col("review_comment_message"))) > 0), True).otherwise(False))

    # Check for duplicate rows
    duplicates = order_reviews.groupBy(order_reviews.columns).count().filter(col("count") > 1)
    logger.info(f"Number of duplicate rows: {duplicates.count()}")

    # Log duplicate rows
    duplicates.write.mode("overwrite").csv("/dbfs/mnt/olist-store-data/logs/duplicate_reviews.csv")
    logger.info("Duplicate rows have been logged.")

    # Remove duplicate rows
    order_reviews = order_reviews.dropDuplicates()
    after_dedup_count = order_reviews.count()
    logger.info(f"Rows removed after deduplication: {initial_count - after_dedup_count}")

    # Show summary statistics before final cleaning
    logger.info("\nSummary statistics before final cleaning:")
    order_reviews.describe().show()

    # Check unique values in review_score before cleaning
    logger.info("\nDistribution of review scores before cleaning:")
    order_reviews.groupBy("review_score").count().orderBy("review_score").show()

    # Ensure review_score is between 1 and 5
    order_reviews = order_reviews.filter((col("review_score") >= 1) & (col("review_score") <= 5))

    # Outlier detection for review_score
    review_score_stats = order_reviews.select(mean("review_score").alias("mean"), stddev("review_score").alias("stddev")).collect()[0]
    order_reviews = order_reviews.withColumn("review_score_zscore", abs((col("review_score") - review_score_stats["mean"]) / review_score_stats["stddev"]))
    order_reviews = order_reviews.withColumn("is_outlier", col("review_score_zscore") > 3)

    # Ensure review_creation_date is not in the future
    current_date = datetime.datetime.now().date()
    order_reviews = order_reviews.filter(col("review_creation_date").cast("date") <= lit(current_date))

    # Ensure consistency between review_creation_date and review_answer_timestamp
    order_reviews = order_reviews.filter(col("review_answer_timestamp") >= col("review_creation_date"))

    # Cache the DataFrame after major transformations
    order_reviews = order_reviews.cache()

    print_section("Cleaned Dataset Summary")

    # Calculate the number of problematic rows
    total_rows = order_reviews.count()
    problematic_rows = order_reviews.filter(
        (col("review_score").isNull()) |
        (col("review_creation_date").isNull()) |
        (col("review_answer_timestamp").isNull()) |
        (col("review_answer_timestamp") < col("review_creation_date")) |
        (length(col("review_id")) != 32) |  # Assuming review_id should be 32 characters
        (length(col("order_id")) != 32)  # Assuming order_id should be 32 characters
    ).count()

    clean_rows = total_rows - problematic_rows

    # Calculate the percentage of clean vs problematic data
    problematic_percentage = (problematic_rows / total_rows) * 100
    clean_percentage = (clean_rows / total_rows) * 100

    logger.info(f"Total rows: {total_rows}")
    logger.info(f"Problematic rows: {problematic_rows}")
    logger.info(f"Clean rows: {clean_rows}")
    logger.info(f"Percentage of problematic data: {problematic_percentage:.2f}%")
    logger.info(f"Percentage of clean data: {clean_percentage:.2f}%")

    # Show summary statistics of cleaned dataset
    logger.info("\nCleaned dataset summary:")
    order_reviews.describe().show()

    logger.info("\nData types:")
    order_reviews.printSchema()

    logger.info("\nSample of cleaned data:")
    order_reviews.show(5, truncate=False)

    # Check for any remaining NULL values
    logger.info("\nRemaining NULL values:")
    for column in order_reviews.columns:
        null_count = order_reviews.filter(col(column).isNull()).count()
        null_percentage = (null_count / total_rows) * 100
        logger.info(f"Null values in {column}: {null_count} ({null_percentage:.2f}%)")

    # Show distribution of has_comment
    logger.info("\nDistribution of has_comment:")
    has_comment_dist = order_reviews.groupBy("has_comment").agg(
        count("*").alias("count"),
        round((count("*") / total_rows * 100), 2).alias("percentage")
    )
    has_comment_dist.show()

    # Summary statistics for date columns
    logger.info("\nSummary statistics for date columns:")
    order_reviews.select("review_creation_date", "review_answer_timestamp").describe().show()

    logger.info("\nCleaning process completed.")

    # Define the output path for the Parquet file
    output_path = "/mnt/olist-store-data/transformed-data/olist_order_reviews_cleaned_dataset.parquet"
    temp_path = "/mnt/olist-store-data/transformed-data/temp_parquet_output"

    try:
        # Remove existing directories if they exist
        dbutils.fs.rm(output_path, recurse=True)
        dbutils.fs.rm(temp_path, recurse=True)

        # Save as a single Parquet file using temporary directory
        (order_reviews
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
        order_reviews.unpersist()

except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the order_reviews cleaned dataset for review

# COMMAND ----------

cleaned_df = spark.read.parquet("/mnt/olist-store-data/transformed-data/olist_order_reviews_cleaned_dataset.parquet")
display(cleaned_df.limit(25))
#display(cleaned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Translate review_comment_title

# COMMAND ----------

# Import libraries
from pyspark.sql.functions import col, length, trim
from google.cloud import translate_v2
import time

# Initialize Google Translate client
translate_client = translate_v2.Client()

# Filter non-null and non-empty titles to translate
titles_to_translate_df = cleaned_df.select("review_id", "review_comment_title").filter(
    (col("review_comment_title").isNotNull()) & (length(trim(col("review_comment_title"))) > 0)
)

# Collect titles to translate
titles_to_translate = titles_to_translate_df.collect()
translated_titles = []
batch_size = 100

# Translate in batches
for i in range(0, len(titles_to_translate), batch_size):
    batch = [row["review_comment_title"] for row in titles_to_translate[i:i + batch_size]]
    try:
        results = translate_client.translate(batch, target_language="en", source_language="pt")
        translated_titles.extend([r["translatedText"].capitalize() for r in results])
        time.sleep(0.5)  # Avoid overwhelming the API
    except Exception as e:
        print(f"Error translating batch {i // batch_size + 1}: {e}")
        translated_titles.extend([None] * len(batch))

# Create a DataFrame with translated titles
translated_titles_df = spark.createDataFrame(
    zip([row["review_id"] for row in titles_to_translate], translated_titles),
    schema=["review_id", "translated_title"]
)

# Join translated titles back to the original dataset
cleaned_df_with_titles = cleaned_df.join(translated_titles_df, on="review_id", how="left")

# Verify translation count matches original count
if len(titles_to_translate) == translated_titles_df.count():
    print("✓ Translation count matches original count for review_comment_title.")
else:
    print("✗ Discrepancy in translation count for review_comment_title.")

# Display 100 rows for verification
display(cleaned_df_with_titles.select("review_id", "review_comment_title", "translated_title").limit(100))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Translate review_comment_message

# COMMAND ----------

# Filter non-null and non-empty messages to translate
messages_to_translate_df = cleaned_df_with_titles.select("review_id", "review_comment_message").filter(
    (col("review_comment_message").isNotNull()) & (length(trim(col("review_comment_message"))) > 0)
)

# Collect messages to translate
messages_to_translate = messages_to_translate_df.collect()
translated_messages = []

# Translate in batches
for i in range(0, len(messages_to_translate), batch_size):
    batch = [row["review_comment_message"] for row in messages_to_translate[i:i + batch_size]]
    try:
        results = translate_client.translate(batch, target_language="en", source_language="pt")
        translated_messages.extend([r["translatedText"].capitalize() for r in results])
        time.sleep(0.5)  # Avoid overwhelming the API
    except Exception as e:
        print(f"Error translating batch {i // batch_size + 1}: {e}")
        translated_messages.extend([None] * len(batch))

# Create a DataFrame with translated messages
translated_messages_df = spark.createDataFrame(
    zip([row["review_id"] for row in messages_to_translate], translated_messages),
    schema=["review_id", "translated_message"]
)

# Join translated messages back to the dataset with titles
cleaned_df_with_translations = cleaned_df_with_titles.join(translated_messages_df, on="review_id", how="left")

# Verify translation count matches original count
if len(messages_to_translate) == translated_messages_df.count():
    print("✓ Translation count matches original count for review_comment_message.")
else:
    print("✗ Discrepancy in translation count for review_comment_message.")

# Display 100 rows for verification
display(cleaned_df_with_translations.select("review_id", "review_comment_message", "translated_message").limit(100))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data cleaning for review_comment_message

# COMMAND ----------

from pyspark.sql.functions import (
    udf, col, lower, regexp_replace, initcap, count, 
    when, length, coalesce
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import html
import re

def clean_text(text):
    if text is None:
        return None
    text = html.unescape(text)
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^a-z0-9\s\'.]', '', text)
    text = '. '.join(s.capitalize() for s in text.split('. '))
    return text[0].upper() + text[1:] if text else None

def correct_untranslated(text):
    if text is None:
        return None
    corrections = {
        "timo": "great",
        "rpido": "fast",
        "otima": "excellent",
        "j t": "already",
        "jt": "already"
    }
    for word, correction in corrections.items():
        text = re.sub(r'\b' + word + r'\b', correction, text, flags=re.IGNORECASE)
    return text

# Register UDFs
clean_text_udf = udf(clean_text, StringType())
correct_untranslated_udf = udf(correct_untranslated, StringType())

# Read the original dataset
base_df = spark.read.parquet("dbfs:/mnt/olist-store-data/transformed-data/olist_order_reviews_cleaned_dataset.parquet")

# First deduplicate the translations
translations_df = cleaned_df_with_translations.select(
    'review_id', 
    'translated_message', 
    'translated_title'
).dropDuplicates(['review_id'])

# Join with translations and apply cleaning
cleaned_df = base_df.join(
    translations_df,
    'review_id',
    'left'
).withColumn(
    "cleaned_message",
    correct_untranslated_udf(
        clean_text_udf(coalesce(col("translated_message"), col("review_comment_message")))
    )
)

# Count statistics
total_messages = cleaned_df.count()
total_cleaned = cleaned_df.filter(col("cleaned_message").isNotNull()).count()
total_changed = cleaned_df.filter(
    (col("cleaned_message").isNotNull()) & 
    (col("cleaned_message") != coalesce(col("translated_message"), col("review_comment_message")))
).count()

# Add row number and cleaning flags
cleaned_df_with_stats = cleaned_df.withColumn(
    "row_num", 
    count("*").over(Window.orderBy("review_id"))
).withColumn(
    "was_cleaned", 
    when(
        (col("cleaned_message").isNotNull()) & 
        (col("cleaned_message") != coalesce(col("translated_message"), col("review_comment_message"))), 
        True
    ).otherwise(False)
).withColumn(
    "chars_removed", 
    when(
        coalesce(col("translated_message"), col("review_comment_message")).isNotNull() & 
        col("cleaned_message").isNotNull(),
        length(coalesce(col("translated_message"), col("review_comment_message"))) - 
        length(col("cleaned_message"))
    ).otherwise(None)
)

# Print summary statistics
print(f"Total messages processed: {total_messages}")
print(f"Messages with non-null cleaned result: {total_cleaned}")
print(f"Messages changed during cleaning: {total_changed}")
print(f"Percentage of messages cleaned: {(total_changed / total_messages) * 100:.2f}%")

# Display sample of cleaned messages
print("\nSample of cleaned messages:")
display(cleaned_df_with_stats.select(
    "row_num", 
    "review_id", 
    "translated_message", 
    "cleaned_message", 
    "was_cleaned", 
    "chars_removed"
).orderBy("review_id").limit(25))

# Display distribution of character changes
print("\nDistribution of character changes:")
display(cleaned_df_with_stats.groupBy("chars_removed")
       .count()
       .orderBy("chars_removed"))

# Verification
print("\nVerification:")
print(f"Original row count: {base_df.count():,}")
print(f"Final row count: {cleaned_df_with_stats.count():,}")

# Additional verification
if base_df.count() != cleaned_df_with_stats.count():
    print("\nWARNING: Row count mismatch!")
    print("\nChecking for duplicates:")
    dupes = cleaned_df_with_stats.groupBy("review_id").count().filter("count > 1")
    print(f"Number of duplicate review_ids: {dupes.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the cleaned order_reviews_final version

# COMMAND ----------

# Import libraries
from pyspark.sql.functions import udf, col, count, when, regexp_replace
from pyspark.sql.window import Window
import html
import re

# Read the base dataset without deduplication
base_df = spark.read.parquet("dbfs:/mnt/olist-store-data/transformed-data/olist_order_reviews_cleaned_dataset.parquet")

print("\n=== Initial Data Analysis ===")
initial_count = base_df.count()
print(f"Initial row count: {initial_count:,}")

# Cleaning function
def clean_text(text):
    if text is None:
        return None
    
    # Decode HTML entities
    text = html.unescape(text)
    
    # Remove leading '>' characters and spaces
    text = re.sub(r'^[>\s]+', '', text)
    
    # Remove any remaining '>' characters
    text = text.replace('>', '')
    
    # Clean up HTML entities and special characters
    text = text.replace('&amp;', '&').replace('&#39;', "'")
    
    # Remove multiple spaces and trim
    text = ' '.join(text.split())
    
    # Ensure proper capitalization of sentences
    sentences = text.split('. ')
    cleaned_sentences = []
    for sentence in sentences:
        if sentence:
            sentence = sentence.strip()
            sentence = sentence[0].upper() + sentence[1:] if len(sentence) > 1 else sentence.upper()
            cleaned_sentences.append(sentence)
    
    return '. '.join(cleaned_sentences)

# Register UDF for cleaning text
clean_text_udf = udf(clean_text)

# First deduplicate the translations
translations_df = final_df.select(
    'review_id',
    'translated_title',
    'translated_message'
).dropDuplicates(['review_id'])

# Join and apply cleaning
final_cleaned_df = base_df.join(
    translations_df,
    'review_id',
    'left'
).withColumn(
    "final_translated_message",
    clean_text_udf(col("translated_message"))
).withColumn(
    "final_translated_title",
    clean_text_udf(col("translated_title"))
)

# Display results with all relevant columns
print("\nSample of final dataset (100 rows):")
display(final_cleaned_df.select(
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "final_translated_title",
    "review_comment_message",
    "final_translated_message",
    "review_creation_date",
    "review_answer_timestamp",
    "has_comment",
    "review_score_zscore",
    "is_outlier"
).orderBy("review_creation_date").limit(100))

# Calculate statistics
total_rows = final_cleaned_df.count()
translated_title_rows = final_cleaned_df.filter(col("final_translated_title").isNotNull()).count()
translated_message_rows = final_cleaned_df.filter(col("final_translated_message").isNotNull()).count()

print(f"\n=== Summary Statistics ===")
print(f"Total rows in dataset: {total_rows:,}")
print(f"Rows with translated titles: {translated_title_rows:,}")
print(f"Rows with translated messages: {translated_message_rows:,}")
print(f"Percentage of rows with translated titles: {(translated_title_rows/total_rows)*100:.2f}%")
print(f"Percentage of rows with translated messages: {(translated_message_rows/total_rows)*100:.2f}%")

# Quality checks
print("\n=== Quality Checks ===")
duplicate_count = final_cleaned_df.groupBy("review_id").count().filter("count > 1").count()
print(f"Number of duplicate review_ids: {duplicate_count:,}")

# Verification
print("\n=== Data Reconciliation ===")
print(f"Original row count: {initial_count:,}")
print(f"Final row count: {total_rows:,}")
if initial_count == total_rows:
    print("✓ Row count maintained successfully")
else:
    print(f"✗ Row count mismatch: Difference of {abs(total_rows - initial_count):,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the final version of the order_reviews file

# COMMAND ----------

# Define the output path for final cleaned and translated dataset
output_path = "/mnt/olist-store-data/transformed-data/olist_order_reviews_cleaned_dataset_v2.0.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_parquet_output"

try:
    # Create final DataFrame with selected columns
    final_df = final_cleaned_df.select(
        "review_id",
        "order_id",
        "review_score",
        "final_translated_title",
        "final_translated_message",
        "review_creation_date",
        "review_answer_timestamp"
    )

    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (final_df
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
    print("\nSample of saved data (showing selected columns):")
    display(verified_df.select(
        "review_id",
        "order_id",
        "review_score",
        "final_translated_title",
        "final_translated_message",
        "review_creation_date",
        "review_answer_timestamp"
    ).orderBy("review_creation_date").limit(5))

    # Print column names for verification
    print("\nColumns in saved file:")
    for col in verified_df.columns:
        print(f"- {col}")

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
    # Unpersist cached DataFrame if it was cached
    if final_cleaned_df.is_cached:
        final_cleaned_df.unpersist()
