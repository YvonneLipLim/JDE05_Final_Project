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
# MAGIC ### Read sellers dataset from raw-data folder

# COMMAND ----------

sellers = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/olist_sellers_dataset.csv")

# COMMAND ----------

sellers.show(10)
sellers.printSchema()
display(sellers.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cast the column to a string value as it has turned into an integer automatically when Databricks reads data from source.<br>
# MAGIC Adding a "0" in the seller_zip_code_prefix column

# COMMAND ----------

from pyspark.sql.functions import col, lpad

sellers = sellers.withColumn(
    "seller_zip_code_prefix", 
    lpad(col("seller_zip_code_prefix").cast("string"), 5, "0")
)

sellers.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data cleaning for sellers dataset
# MAGIC - **Step 1: Data Loading and Initial Analysis**<br>
# MAGIC The script begins by loading a CSV file containing seller data into a PySpark DataFrame. It then performs an initial analysis, providing information about:
# MAGIC   - Total number of records
# MAGIC   - Number of columns
# MAGIC   - Unique cities and states
# MAGIC   - Missing values analysis
# MAGIC
# MAGIC - **Step 2: Data Cleaning Process**<br>
# MAGIC The cleaning process involves several steps:
# MAGIC   - Basic String Cleaning: Trims whitespace from string columns.
# MAGIC   - Enhanced City Name Cleaning:
# MAGIC     - Removes numeric values and special characters
# MAGIC     - Standardizes city names by splitting on delimiters
# MAGIC     - Performs character standardization (e.g., replacing "são" with "sao")
# MAGIC   - State Name Standardization: Converts state names to uppercase.
# MAGIC   - Metrics Calculation:
# MAGIC     - Calculates sellers per state and city:<br>
# MAGIC       a. **Seller Density**<br>
# MAGIC       Categorizes cities based on the number of sellers:
# MAGIC       ```
# MAGIC       - High: >= 100 sellers
# MAGIC       - Medium: >= 20 sellers
# MAGIC       - Low: < 20 sellers<br>
# MAGIC       ```
# MAGIC       b. **City Size**<br>
# MAGIC       Classifies cities based on the number of sellers:
# MAGIC       ```
# MAGIC       - Major City: >= 500 sellers
# MAGIC       - Large City: >= 100 sellers
# MAGIC       - Medium City: >= 50 sellers
# MAGIC       - Small City: < 50 sellers
# MAGIC       ```
# MAGIC     - Computes state market share percentages:<br>
# MAGIC     a. **Sellers per State**<br>The "sellers_in_state" column shows the number of sellers in each state. Some key observations:<br>
# MAGIC       ```
# MAGIC       - The state with the highest number of sellers has 1,849 sellers.
# MAGIC       - Several states have very few sellers, with some having only 1 or 2.
# MAGIC       - There's a wide range in the number of sellers across states, indicating a highly concentrated market in certain areas.
# MAGIC       ```
# MAGIC     - **State Market Share**<br>The "state_market_share" column represents the percentage of total sellers in each state. Notable points:<br>
# MAGIC       ```
# MAGIC       - The highest market share is 59.74%, corresponding to the state with 1,849 sellers.
# MAGIC       - Many states have very small market shares, below 1%.
# MAGIC       - The market shares directly correlate with the number of sellers in each state.
# MAGIC       ```
# MAGIC   - Geographic Classification:<br>
# MAGIC     - **Metropolitan Area**<br>Classifies sellers based on their location:
# MAGIC       ```
# MAGIC       - Sao Paulo Metro
# MAGIC       - Rio Metro
# MAGIC       - BH Metro
# MAGIC       - Curitiba Metro
# MAGIC       - Porto Alegre Metro
# MAGIC       - Other
# MAGIC       ```
# MAGIC     - **Seller Region**<br>This classification categorizes sellers into broader geographical regions based on their state. Here's how the regions are defined:
# MAGIC       ```
# MAGIC       - Southeast: SP, RJ, MG, ES
# MAGIC       - South: PR, RS, SC
# MAGIC       - Central-West: MT, MS, GO, DF
# MAGIC       - Northeast: BA, PE, CE, PB, MA, RN, AL, PI, SE
# MAGIC       - North: PA, AM, RO, AP, AC, RR, TO
# MAGIC       - Unknown: Any other state
# MAGIC       ```
# MAGIC   - Market Analysis:
# MAGIC     - **Seller Density**<br>This classification categorizes cities based on the concentration of sellers, providing insights into the competitiveness and market saturation of different locations:
# MAGIC       ```
# MAGIC       - High: Cities with 100 or more sellers (>= 100 sellers)
# MAGIC       - Medium: Cities with 20 to 99 sellers (>= 20 sellers)
# MAGIC       - Low: Cities with fewer than 20 sellers (< 20 sellers)
# MAGIC       ```
# MAGIC     - **Market Proximity**<br>Categorizes sellers based on their 
# MAGIC     proximity to core markets:
# MAGIC       ```
# MAGIC       - Core Market: Located in major metropolitan areas
# MAGIC       - Near Market: In SP, RJ, or MG states but outside metropolitan areas
# MAGIC       - Remote Market: All other locations
# MAGIC       ```
# MAGIC     - **Business Potential**<br>Assesses areas based on their growth potential:
# MAGIC       ```
# MAGIC       - High Growth: Core Market with High seller density
# MAGIC       - Medium Growth: Near Market with High or Medium seller density
# MAGIC       - Stable: All other combinations
# MAGIC       ```
# MAGIC
# MAGIC - **Step 3: Data Analysis and Reporting**<br>
# MAGIC The script generates various analytical reports:
# MAGIC   - State-level analysis
# MAGIC   - City size distribution
# MAGIC   - Metropolitan area market share
# MAGIC   - Top 10 cities with market analysis
# MAGIC   - Market proximity distribution
# MAGIC   - Business potential distribution

# COMMAND ----------

# This script processes the sellers dataset to clean, standardize, and enrich the data
# with additional metrics and classifications for business intelligence purposes.

# Import libraries
from pyspark.sql.functions import (
    col, sum, count, when, upper, initcap, 
    length, regexp_replace, trim, round, avg,
    split, expr, desc, lower
)
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from functools import reduce

def clean_sellers_dataset(spark, geolocation=None):
    """
    Comprehensive data cleaning and enrichment function for the Olist sellers dataset.
    
    The function performs the following major operations:
    1. Initial data loading and analysis
    2. Data cleaning and standardization
    3. Geographic classification and market analysis
    4. Business metrics calculation
    5. Detailed reporting
    
    Args:
        spark: SparkSession object
        geolocation: Optional geolocation data for additional analysis
        
    Returns:
        DataFrame: Cleaned and enriched sellers dataset
    """
    try:
        # Step 1: Data Loading and Initial Analysis
        print("Loading sellers dataset...")
        sellers = spark.read.format("csv").option("header","true").option("inferSchema","true")\
            .load("/mnt/olist-store-data/raw-data/olist_sellers_dataset.csv")
        
        # Create a copy for cleaning operations
        cleaned_sellers = sellers
        
        # Calculate initial dataset statistics for later comparison
        initial_count = sellers.count()
        initial_cities = sellers.select("seller_city").distinct().count()
        initial_states = sellers.select("seller_state").distinct().count()
        
         # Display initial dataset metrics
        print("\nInitial dataset information:")
        print(f"Number of records: {initial_count:,}")
        print(f"Number of columns: {len(sellers.columns)}")
        print(f"Number of unique cities: {initial_cities:,}")
        print(f"Number of unique states: {initial_states:,}")
        
        # Analyze missing values across all columns
        print("\nMissing values analysis:")
        missing_values = sellers.select([
            sum(col(c).isNull().cast("int")).alias(c) for c in sellers.columns
        ])
        
        # Display missing value statistics for each column
        for column in sellers.columns:
            missing_count = missing_values.collect()[0][column]
            missing_percentage = (missing_count / initial_count) * 100
            print(f"{column}: {missing_count:,} missing values ({missing_percentage:.2f}%)")
        
        print("\nMissing values count:")
        missing_values.show()
        
        # Check for duplicate seller IDs
        print("\nChecking for duplicate records...")
        seller_id_duplicates = sellers.groupBy("seller_id").count().filter(col("count") > 1)
        seller_id_duplicate_count = seller_id_duplicates.count()
        seller_id_duplicate_percentage = (seller_id_duplicate_count / initial_count) * 100
        print(f"Number of duplicate seller_ids: {seller_id_duplicate_count:,} ({seller_id_duplicate_percentage:.2f}%)")
        
        # Step 2: Data Cleaning Process
        print("\nStarting data cleaning process...")
        
        # Basic string cleaning: Remove whitespace from all string columns
        string_columns = ["seller_id", "seller_city", "seller_state"]
        for column in string_columns:
            cleaned_sellers = cleaned_sellers.withColumn(
                column,
                trim(col(column))
            )
        
        # Enhanced city name cleaning
        # 1. Remove invalid characters and standardize formatting
        # 2. Handle common city name variations
        # 3. Standardize special characters and accents
        cleaned_sellers = cleaned_sellers.withColumn(
            "seller_city",
            when(col("seller_city").rlike("\\d+"), None)
            .when(col("seller_city").rlike("@"), None)
            .when(col("seller_city").rlike("/"), split(col("seller_city"), "/").getItem(0))
            .when(col("seller_city").rlike(","), split(col("seller_city"), ",").getItem(0))
            .when(col("seller_city").rlike("-"), split(col("seller_city"), "-").getItem(0))
            .otherwise(col("seller_city"))
        )
        
        # Standardize character encoding and formatting
        cleaned_sellers = cleaned_sellers.withColumn(
            "seller_city",
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                lower(trim(col("seller_city"))),
                                "são", "sao"
                            ),
                            "d['´`]", "d"
                        ),
                        "\\s+", " "
                    ),
                    "[^a-z ]", ""
                ),
                "^sp$", "sao paulo"
            )
        )
        
        # Convert city names to proper case
        cleaned_sellers = cleaned_sellers.withColumn(
            "seller_city",
            initcap(col("seller_city"))
        )
        
        # Standardize state names to uppercase
        cleaned_sellers = cleaned_sellers.withColumn(
            "seller_state",
            upper(trim(col("seller_state")))
        )
        
        # Convert zip_code into string
        cleaned_sellers = cleaned_sellers.withColumn(
            "seller_zip_code_prefix", 
            lpad(col("seller_zip_code_prefix").cast("string"), 5, "0")
        ) 

        # Metrics Calculation
        # Calculate sellers per state and city
        sellers_per_state = cleaned_sellers.groupBy("seller_state").count()
        cleaned_sellers = cleaned_sellers.join(
            sellers_per_state.withColumnRenamed("count", "sellers_in_state"), 
            "seller_state"
        )
        
        # Compute state market share percentages for each state
        cleaned_sellers = cleaned_sellers.withColumn(
            "state_market_share",
            round(col("sellers_in_state") / initial_count * 100, 2)
        )
        
        # Classify metropolitan areas based on major city clusters
        cleaned_sellers = cleaned_sellers.withColumn(
            "metropolitan_area",
            when(col("seller_city").isin("Sao Paulo", "Guarulhos", "Santo Andre", "Osasco", "Barueri"), "Sao Paulo Metro")
            .when(col("seller_city").isin("Rio De Janeiro", "Niteroi", "Nova Iguacu", "Duque De Caxias"), "Rio Metro")
            .when(col("seller_city").isin("Belo Horizonte", "Contagem", "Betim"), "BH Metro")
            .when(col("seller_city").isin("Curitiba", "Sao Jose Dos Pinhais", "Colombo"), "Curitiba Metro")
            .when(col("seller_city").isin("Porto Alegre", "Canoas", "Novo Hamburgo"), "Porto Alegre Metro")
            .otherwise("Other")
        )
        
        # Classify sellers by geographic region
        cleaned_sellers = cleaned_sellers.withColumn(
            "seller_region",
            when(col("seller_state").isin("SP", "RJ", "MG", "ES"), "Southeast")
            .when(col("seller_state").isin("PR", "RS", "SC"), "South")
            .when(col("seller_state").isin("MT", "MS", "GO", "DF"), "Central-West")
            .when(col("seller_state").isin("BA", "PE", "CE", "PB", "MA", "RN", "AL", "PI", "SE"), "Northeast")
            .when(col("seller_state").isin("PA", "AM", "RO", "AP", "AC", "RR", "TO"), "North")
            .otherwise("Unknown")
        )
        
        # Calculate city-level metrics
        sellers_per_city = cleaned_sellers.groupBy("seller_city").count()
        cleaned_sellers = cleaned_sellers.join(
            sellers_per_city.withColumnRenamed("count", "sellers_in_city"), 
            "seller_city"
        )
        
        # Classify cities by size based on seller count
        cleaned_sellers = cleaned_sellers.withColumn(
            "city_size",
            when(col("sellers_in_city") >= 500, "Major City")
            .when(col("sellers_in_city") >= 100, "Large City")
            .when(col("sellers_in_city") >= 50, "Medium City")
            .otherwise("Small City")
        )
        
        # Classify locations by market proximity
        cleaned_sellers = cleaned_sellers.withColumn(
            "seller_density",
            when(col("sellers_in_city") >= 100, "High")
            .when(col("sellers_in_city") >= 20, "Medium")
            .otherwise("Low")
        )
        
        # Classify areas by business growth potential
        cleaned_sellers = cleaned_sellers.withColumn(
            "market_proximity",
            when(col("metropolitan_area") != "Other", "Core Market")
            .when(
                (col("seller_state").isin("SP", "RJ", "MG")) & 
                (col("metropolitan_area") == "Other"), 
                "Near Market"
            )
            .otherwise("Remote Market")
        )

        # Add business potential classification
        cleaned_sellers = cleaned_sellers.withColumn(
            "business_potential",
            when(
                (col("market_proximity") == "Core Market") & 
                (col("seller_density") == "High"),
                "High Growth"
            )
            .when(
                (col("market_proximity") == "Near Market") & 
                (col("seller_density").isin("High", "Medium")),
                "Medium Growth"
            )
            .otherwise("Stable")
        )
        
         # Calculate final dataset metrics
        final_count = cleaned_sellers.count()
        final_cities = cleaned_sellers.select("seller_city").distinct().count()
        
        # Print enhanced analysis results
        print("\nState-level Analysis:")
        cleaned_sellers.groupBy("seller_state")\
            .agg(
                count("*").alias("sellers"),
                round(count("*") / initial_count * 100, 2).alias("market_share_pct")
            )\
            .orderBy(desc("sellers"))\
            .show()
        
        print("\nCity Size Distribution:")
        cleaned_sellers.groupBy("city_size").count()\
            .withColumn("percentage", round(col("count") / final_count * 100, 2))\
            .orderBy("count", ascending=False).show()
        
        print("\nMetropolitan Area Market Share:")
        cleaned_sellers.groupBy("metropolitan_area")\
            .agg(
                count("*").alias("sellers"),
                round(count("*") / final_count * 100, 2).alias("market_share_pct"),
                round(avg("sellers_in_city"), 2).alias("avg_sellers_per_city")
            )\
            .orderBy(desc("sellers"))\
            .show()
        
        print("\nTop 10 Cities with Market Analysis:")
        cleaned_sellers.groupBy("seller_city", "seller_state", "metropolitan_area", "city_size")\
            .agg(
                count("*").alias("sellers"),
                round(count("*") / final_count * 100, 2).alias("market_share_pct")
            )\
            .orderBy(desc("sellers"))\
            .show(10)
        
        print("\nFinal Cleaning Summary:")
        print(f"Original record count: {initial_count:,}")
        print(f"Cleaned record count: {final_count:,}")
        print(f"Records affected: {abs(initial_count - final_count):,}")
        print(f"Data retention rate: {(final_count / initial_count) * 100:.2f}%")
        
        print("\nCity Consolidation:")
        print(f"Original cities: {initial_cities:,}")
        print(f"Final unique cities: {final_cities:,}")
        print(f"Cities consolidated: {initial_cities - final_cities:,}")
        print(f"Consolidation rate: {((initial_cities - final_cities) / initial_cities) * 100:.2f}%")
        
        print("\nCleaned Dataset Schema:")
        cleaned_sellers.printSchema()
        
        # Print market proximity analysis
        print("\nMarket Proximity Distribution:")
        cleaned_sellers.groupBy("market_proximity")\
            .agg(
                count("*").alias("sellers"),
                round(count("*") / final_count * 100, 2).alias("percentage"),
                round(avg("sellers_in_city"), 2).alias("avg_sellers_per_city")
            )\
            .orderBy(desc("sellers"))\
            .show()
            
        print("\nMarket Proximity by Region:")
        cleaned_sellers.groupBy("market_proximity", "seller_region")\
            .count()\
            .withColumn("percentage", round(col("count") / final_count * 100, 2))\
            .orderBy(desc("count"))\
            .show()

        print("\nBusiness Potential Distribution:")
        cleaned_sellers.groupBy("business_potential")\
            .count()\
            .withColumn("percentage", round(col("count") / final_count * 100, 2))\
            .orderBy(desc("count"))\
            .show()

        print("\nBusiness Potential by Region:")
        cleaned_sellers.groupBy("business_potential", "seller_region")\
            .count()\
            .withColumn("percentage", round(col("count") / final_count * 100, 2))\
            .orderBy(desc("count"))\
            .show()

        print("\nSample of Cleaned Data with New Metrics:")
        cleaned_sellers.select(
            "seller_id", "seller_city", "seller_state", 
            "metropolitan_area", "market_proximity", "city_size", 
            "seller_density", "state_market_share", "business_potential"
        ).show(5)

        # Display final result
        display(cleaned_sellers.limit(10))

        print("\nCleaning process completed.")
        return cleaned_sellers
        
    except Exception as e:
        print(f"\nError in data cleaning process: {str(e)}")
        raise

# Execute the cleaning process
if __name__ == "__main__":
    try:
        cleaned_sellers = clean_sellers_dataset(spark)
    except Exception as e:
        print(f"Failed to clean sellers dataset: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the cleaned_sellers_withCalculations dataset

# COMMAND ----------

# Verify final schema
print("\nFinal Schema:")
cleaned_sellers.printSchema()

# Show sample of final dataset
print("\nSample of final cleaned dataset:")
cleaned_sellers.show(5, truncate=False)

# Print final record count
print(f"\nTotal records in cleaned dataset: {cleaned_sellers.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the cleaned_sellers_withCalculations dataset in parquet file

# COMMAND ----------

# Define the output path for final cleaned and translated dataset
output_path = "/mnt/olist-store-data/transformed-data/olist_sellers_cleaned_dataset_withCalculations.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_parquet_output"

try:
    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (cleaned_sellers
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
    cleaned_sellers.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the cleaned_sellers dataset with selected columns

# COMMAND ----------

from pyspark.sql.functions import col

def preview_sellers(spark):
    # Load the existing cleaned products dataset
    cleaned_sellers = spark.read.parquet("/mnt/olist-store-data/transformed-data/olist_sellers_cleaned_dataset_withCalculations.parquet")

    # Create DataFrame with selected columns
    cleaned_selected_sellers = cleaned_sellers.select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    )

    # Verify final schema
    print("\nFinal Schema:")
    cleaned_selected_sellers.printSchema()

    # Show sample of final dataset
    print("\nSample of final cleaned dataset:")
    display(cleaned_selected_sellers.limit(5))

    # Print final record count
    print(f"\nTotal records in cleaned dataset: {cleaned_selected_sellers.count():,}")
    
    return cleaned_selected_sellers

# Call the function with the spark session and store the result
selected_sellers = preview_sellers(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the cleaned_sellers dataset with select columns in parquet file

# COMMAND ----------

# Define the output path for final cleaned and translated dataset
output_path = "/mnt/olist-store-data/transformed-data/olist_sellers_cleaned_dataset_v2.0.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_sellers_parquet"

try:
    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (selected_sellers
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
    selected_sellers.unpersist()
