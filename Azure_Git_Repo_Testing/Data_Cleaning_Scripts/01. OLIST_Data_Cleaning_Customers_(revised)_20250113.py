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
# MAGIC ### Read customers dataset from raw-data folder

# COMMAND ----------

customers = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/olist_customers_dataset.csv")

# COMMAND ----------

customers.show(10)
customers.printSchema()
customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cast the column to a string value as it has turned into an integer automatically when Databricks reads data from source.<br>
# MAGIC Adding a "0" in the customer_zip_code_prefix column

# COMMAND ----------

from pyspark.sql.functions import col, lpad

customers = customers.withColumn(
    "customer_zip_code_prefix", 
    lpad(col("customer_zip_code_prefix").cast("string"), 5, "0")
)

customers.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data cleaning for customers dataset<br>
# MAGIC - **Step 1: Standardization with Geolocation Data**<br>
# MAGIC The script starts by using the cleaned city names from the geolocation dataset as a reference. This helps ensure consistency between the two datasets.
# MAGIC
# MAGIC - **Step 2: Custom Cleaning Function**:<br>
# MAGIC A user-defined function (UDF) called clean_city_name is created to handle various cleaning tasks:
# MAGIC   - Converting city names to lowercase and stripping whitespace
# MAGIC   - Handling specific abbreviations (e.g., 'bh' to 'Belo Horizonte', 'rj' to 'Rio de Janeiro')
# MAGIC   - Addressing known problematic names (e.g., 'quilometro 14' or 'km 14' to 'Mutum')
# MAGIC   - Proper capitalization of city names, keeping certain words (like 'de', 'da', 'do') in lowercase when not at the start of the name
# MAGIC
# MAGIC - **Step 3: Merging with Geolocation Data**:<br>
# MAGIC The cleaned customer data is merged with the geolocation data using zip code prefixes. This step helps to further standardize city names by using the cleaned geolocation city names where available.
# MAGIC
# MAGIC - **Step 4: Prioritizing Geolocation City Names**:<br>
# MAGIC In the final dataset creation, the script prioritizes the geolocation city name over the customer city name when available. This helps to ensure consistency with the geolocation dataset.
# MAGIC
# MAGIC - **Step 5: Analysis of Cleaning Results**:<br>
# MAGIC The script includes several analysis steps to evaluate the effectiveness of the cleaning process:
# MAGIC   - Comparing the number of unique cities before and after cleaning
# MAGIC   - Identifying and analyzing problematic cities that couldn't be fully cleaned
# MAGIC   - Calculating the percentage of clean vs. problematic cities
# MAGIC   - Examining the distribution of customers across cities and states
# MAGIC
# MAGIC - **Step 6: Analyze the cleaning statistics**
# MAGIC
# MAGIC - **Step 7: Calculate and display city distributions**
# MAGIC
# MAGIC - **Step 8: Show summary statistics**
# MAGIC
# MAGIC - **Step 9: Show state distribution**

# COMMAND ----------

# This script cleans and standardizes city names in the customers dataset.
# It leverages the cleaned geolocation data and applies various cleaning techniques
# to ensure consistency and accuracy in city names.

# Import libraries
from pyspark.sql.functions import col, when, upper, initcap, length, count, desc, sum, format_number, lower, udf, regexp_replace, lit
from pyspark.sql.types import StringType

# Step 1: Prepare cleaned cities from geolocation data
# This step creates a reference dataset of cleaned city names from the geolocation data
print("Starting city cleaning process...")
geolocation_cities = geolocation.select(
    col("geolocation_zip_code_prefix"),
    col("geolocation_city_final").alias("geolocation_city")
).distinct()

# Step 2: Initialize cleaning process for customers dataset
# Create a copy of the customers dataset to apply cleaning operations
print("Applying specific fixes for problematic cities...")
cleaned_customers = customers

# Step 3: Define a function to clean city names
def clean_city_name(city):
    """
    Clean and standardize city names.
    This function performs the following operations:
    1. Converts to lowercase and strips whitespace
    2. Handles specific abbreviations (e.g., 'bh' to 'Belo Horizonte')
    3. Addresses known problematic names (e.g., 'quilometro 14' to 'Mutum')
    4. Applies proper capitalization, keeping certain words lowercase when appropriate
    """
    if not city:
        return None
    city = city.lower().strip()
    if city in ['bh', 'rj']:
        return 'Belo Horizonte' if city == 'bh' else 'Rio de Janeiro'
    if 'quilometro 14' in city or 'km 14' in city:
        return 'Mutum'
    words = city.split()
    cleaned_words = [word.capitalize() if word not in ['de', 'da', 'do', 'das', 'dos', 'e'] or i == 0 else word for i, word in enumerate(words)]
    return ' '.join(cleaned_words)

# Register the UDF (User Defined Function) for use in Spark SQL operations
clean_city_udf = udf(clean_city_name, StringType())

# Apply the cleaning function to the customer_city column
cleaned_customers = cleaned_customers.withColumn(
    "customer_city",
    clean_city_udf(col("customer_city"))
)

# Step 4: Merge with geolocation data to ensure consistency
# This step joins the cleaned customer data with the geolocation data to further standardize city names
print("Merging with geolocation data...")
merged_customers = cleaned_customers.join(
    geolocation_cities,
    cleaned_customers.customer_zip_code_prefix == geolocation_cities.geolocation_zip_code_prefix,
    "left"
)

# Step 5: Create final dataset
# Prioritize geolocation city names over customer city names when available
print("Creating final dataset...")
cleaned_customers = merged_customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    when(col("geolocation_city").isNotNull(), col("geolocation_city"))
    .otherwise(col("customer_city")).alias("customer_city"),
    "customer_state"
)

# Step 6: Analyze cleaning results
print("\nAnalyzing results...")

# Display a sample of the cleaned data for visual inspection
print("\nSample of cleaned customer data:")
cleaned_customers.select(
    "customer_id", 
    "customer_zip_code_prefix", 
    "customer_city", 
    "customer_state"
).show(10, truncate=False)

# Calculate and display cleaning statistics
print("\nCity Cleaning Statistics:")
cities_before = customers.select("customer_city").distinct().count()
cities_after = cleaned_customers.select("customer_city").distinct().count()
print(f"Unique cities before cleaning: {cities_before}")
print(f"Unique cities after cleaning: {cities_after}")

# Analyze problematic cities
problematic_cities_df = cleaned_customers.filter(
    (lower(col("customer_city")) == "bh") |
    (lower(col("customer_city")) == "rj") |
    (lower(col("customer_city")).like("%quilometro%14%"))
)

# Calculate cleaning metrics
total_unique_cities = cleaned_customers.select("customer_city").distinct().count()
problematic_cities_count = problematic_cities_df.select("customer_city").distinct().count()
clean_cities_count = total_unique_cities - problematic_cities_count

# Calculate percentages of clean and problematic cities
clean_percentage = (clean_cities_count / total_unique_cities) * 100
unclean_percentage = (problematic_cities_count / total_unique_cities) * 100

# Display detailed cleaning analysis
print("\nDetailed City Cleaning Analysis:")
print("-" * 40)
print(f"Total unique cities: {total_unique_cities:,}")
print(f"Clean cities: {clean_cities_count:,} ({clean_percentage:.2f}%)")
print(f"Problematic cities: {problematic_cities_count:,} ({unclean_percentage:.2f}%)")

# Show details of problematic cities
print("\nProblematic Cities Details:")
problematic_cities_df.groupBy("customer_city") \
    .agg(count("*").alias("count")) \
    .orderBy("customer_city") \
    .show(truncate=False)

# Step 7: Calculate and display city distributions
total_customers = cleaned_customers.count()
print(f"\nTotal number of customers: {total_customers:,}")

# Create a cached DataFrame of city counts for multiple uses
city_counts = cleaned_customers.groupBy("customer_city") \
    .agg(count("*").alias("count")) \
    .withColumn(
        "percentage",
        format_number((col("count") / total_customers) * 100, 2)
    ) \
    .cache()

# Display top 20 cities by customer count
print("\nTop 20 cities by customer count:")
city_counts.orderBy(desc("count")).show(20, truncate=False)

# Display bottom 20 cities by customer count
print("\nBottom 20 cities by customer count:")
city_counts.orderBy("count").show(20, truncate=False)

# Step 8: Show summary statistics
print("\nSummary Statistics:")
print(f"Total number of customers: {total_customers:,}")
print(f"Total number of unique cities: {total_unique_cities:,}")
print(f"Average customers per city: {total_customers/total_unique_cities:.2f}")

# Calculate and display state distribution
print("\nState distribution:")
state_counts = cleaned_customers.groupBy("customer_state") \
    .agg(count("*").alias("count")) \
    .withColumn(
        "percentage",
        format_number((col("count") / total_customers) * 100, 2)
    ) \
    .orderBy(desc("count"))

state_counts.show(truncate=False)

# Cleanup cached data to free up memory
city_counts.unpersist()

# Display a sample of the final cleaned dataset
display(cleaned_customers.limit(25))

print("\nCleaning process completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the cleaned_customers dataset

# COMMAND ----------

# Verify final schema
print("\nFinal Schema:")
cleaned_customers.printSchema()

# Show sample of final dataset
print("\nSample of final cleaned dataset:")
cleaned_customers.show(5, truncate=False)

# Print final record count
print(f"\nTotal records in cleaned dataset: {cleaned_customers.count():,}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the cleaned_customers to a parquet file

# COMMAND ----------

# Define the output path
output_path = "/mnt/olist-store-data/transformed-data/olist_customers_cleaned_dataset_v2.0.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_parquet_output"

try:
    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (cleaned_customers
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
    cleaned_customers.unpersist()
