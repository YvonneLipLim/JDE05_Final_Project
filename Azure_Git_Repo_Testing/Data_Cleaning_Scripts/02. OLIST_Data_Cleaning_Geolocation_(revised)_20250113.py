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
# MAGIC ### Read geolocation dataset from raw-data folder

# COMMAND ----------


geolocation = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/olist_geolocation_dataset.csv")

# COMMAND ----------

geolocation.printSchema()
geolocation.show(10)
geolocation.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cast the column to a string value as it has turned into an integer automatically when Databricks reads data from source.<br>
# MAGIC Adding a "0" in the geolocation_zip_code_prefix column

# COMMAND ----------

from pyspark.sql.functions import col, lpad

geolocation = geolocation.withColumn(
    "geolocation_zip_code_prefix", 
    lpad(col("geolocation_zip_code_prefix").cast("string"), 5, "0")
)

geolocation.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data cleaning for geolocation dataset<br>
# MAGIC - **Step 1: Basic cleaning**<br>
# MAGIC A user-defined function (UDF) called replace_char is created to perform initial cleaning:
# MAGIC   - Converts city names to lowercase and strips spaces.
# MAGIC   - Removes state abbreviations (e.g., '-sp')
# MAGIC   - Handles the special case of 'sp' (São Paulo)
# MAGIC   - Replaces accented characters with their non-accented equivalents
# MAGIC   - Handles specific patterns like "d'oeste" and hyphenated names
# MAGIC
# MAGIC - **Step 2: City corrections**<br>
# MAGIC A dictionary city_corrections is defined with common city name corrections. This is used to standardize frequently occurring city names.
# MAGIC
# MAGIC - **Step 3: Applying Cleaning and Corrections**<br>
# MAGIC   - The replace_char UDF is applied to create a new column geolocation_city_cleaned
# MAGIC   - City corrections are applied using a series of when conditions
# MAGIC
# MAGIC - **Step 4: Standardization Using Mode**<br>
# MAGIC For remaining inconsistencies, the code uses the mode (most frequent city name) for each zip code prefix to further standardize city names.
# MAGIC
# MAGIC - **Step 5: Proper Title Case**<br>
# MAGIC A UDF proper_title_case is applied to convert city names to proper title case, keeping certain words (like 'de', 'da', 'do') in lowercase when not at the start of the name.
# MAGIC
# MAGIC - **Step 6: Data Quality Checks and Statistics**<br>
# MAGIC The code then performs various data quality checks and generates statistics:
# MAGIC   - Compares the number of unique cities before and after cleaning
# MAGIC   - Shows a sample of city name changes
# MAGIC   - Identifies problematic cities (those with non-alphabetic characters or very short names)
# MAGIC   - Calculates and displays data quality metrics
# MAGIC
# MAGIC - **Step 7: Display Results**<br>
# MAGIC Finally, the code displays:
# MAGIC   - Top 10 cities by frequency
# MAGIC   - State distribution
# MAGIC   - A sample of 25 rows from the final cleaned DataFrame 

# COMMAND ----------

# This script cleans and standardizes city names in a geolocation dataset.
# It performs several steps including basic cleaning, applying corrections,
# standardizing using mode, and proper case formatting.

# Import libraries
from pyspark.sql.functions import col, lower, regexp_replace, udf, mode, when, length
from pyspark.sql.types import StringType, DecimalType
from pyspark.sql import Window

# Step 1: Basic cleaning
# Drop duplicates to ensure data integrity
geolocation = geolocation.dropDuplicates()

# Clean city names
@udf(StringType())
def replace_char(city_name):
    """
    Cleans city names by converting to lowercase, removing accents,
    and handling special cases.
    """
    if city_name is None:
        return None
    
    # Convert to lowercase and strip spaces
    city_name = city_name.lower().strip()
    
    # Remove state abbreviations
    city_name = city_name.replace('-sp', '')
    
    # Handle special abbreviation
    if city_name == 'sp':
        return 'sao paulo'
    
    # Replace special characters
    replacements = {
        'ã': 'a', 'â': 'a', 'á': 'a', 'à': 'a', 'ä': 'a',
        'í': 'i', 'î': 'i', 'ì': 'i',
        'ú': 'u', 'û': 'u', 'ù': 'u', 'ü': 'u',
        'é': 'e', 'ê': 'e', 'è': 'e', 'ë': 'e',
        'ó': 'o', 'õ': 'o', 'ô': 'o', 'ò': 'o', 'ö': 'o',
        'ç': 'c'
    }
    
    for char, replacement in replacements.items():
        city_name = city_name.replace(char, replacement)
    
    # Handle specific patterns
    city_name = city_name.replace("d'", "d ")  # Handle d'oeste, d'alianca patterns
    city_name = city_name.replace("-", " ")    # Handle hyphenated names
    city_name = city_name.replace("'", "")     # Remove any remaining apostrophes
    
    return city_name

# Step 2: Define comprehensive city corrections
# This dictionary maps common misspellings or variations to the correct 
city_corrections = {
    'sao paulo': 'Sao Paulo',
    'rio de janeiro': 'Rio de Janeiro',
    'belo horizonte': 'Belo Horizonte',
    'brasilia': 'Brasilia',
    'curitiba': 'Curitiba',
    'fortaleza': 'Fortaleza',
    'salvador': 'Salvador',
    'porto alegre': 'Porto Alegre',
    'guarulhos': 'Guarulhos',
    'campinas': 'Campinas',
    'sao bernardo do campo': 'Sao Bernardo do Campo',
    'santo andre': 'Santo Andre',
    'osasco': 'Osasco',
    'jundiai': 'Jundiai',
    'sao caetano do sul': 'Sao Caetano do Sul',
    'mogi das cruzes': 'Mogi das Cruzes',
    'embu': 'Embu das Artes',
    'taboao da serra': 'Taboao da Serra',
    'itapecerica da serra': 'Itapecerica da Serra',
    'santana de parnaiba': 'Santana de Parnaiba',
    'goiania': 'Goiania',
    'nova iguacu': 'Nova Iguacu',
    'ribeirao preto': 'Ribeirao Preto',
    'ribeirao das neves': 'Ribeirao das Neves',
    'ribeirao pires': 'Ribeirao Pires',
    'niteroi': 'Niteroi',
    'sao joao de meriti': 'Sao Joao de Meriti',
    'sao jose dos campos': 'Sao Jose dos Campos',
    'sao jose do rio preto': 'Sao Jose do Rio Preto',
    'feira de santana': 'Feira de Santana',
    'varzea grande': 'Varzea Grande',
    'sao vicente': 'Sao Vicente',
    'jaboatao dos guararapes': 'Jaboatao dos Guararapes',
    'aparecida de goiania': 'Aparecida de Goiania',
    'vitoria da conquista': 'Vitoria da Conquista',
    'barueri': 'Barueri',
    'cotia': 'Cotia',
    'carapicuiba': 'Carapicuiba',
    'diadema': 'Diadema',
    'suzano': 'Suzano',
    'embu das artes': 'Embu das Artes'
}

# Step 3: Applying Cleaning and Corrections
# Apply initial cleaning
print("Starting city name cleaning...")
geolocation = geolocation.withColumn(
    "geolocation_city_cleaned", 
    replace_char(col("geolocation_city"))
)

# Apply city corrections
for original, corrected in city_corrections.items():
    geolocation = geolocation.withColumn(
        "geolocation_city_cleaned",
        when(lower(col("geolocation_city_cleaned")) == original, corrected)
        .otherwise(col("geolocation_city_cleaned"))
    )

# Step 4: Standardize Using Mode (most frequent city name)
# Create a window spec to partition by zip code prefix
window_spec = Window.partitionBy("geolocation_zip_code_prefix")
mode_city = mode("geolocation_city_cleaned").over(window_spec)

geolocation = geolocation.withColumn(
    "geolocation_city_final",
    when(
        (col("geolocation_city_cleaned") != mode_city) &
        (length(col("geolocation_city_cleaned")) > 2) &
        (~lower(col("geolocation_city_cleaned")).isin(list(city_corrections.keys()))),
        mode_city
    )
    .otherwise(col("geolocation_city_cleaned"))
)

# Step 5: Proper Title Case transformation
# Convert to proper title case
@udf(StringType())
def proper_title_case(x):
    if not x:
        return None
    
    # Words that should remain lowercase (except at start)
    lowercase_words = {'de', 'da', 'do', 'das', 'dos', 'e'}
    
    words = x.split()
    result = []
    
    for i, word in enumerate(words):
        if word.lower() in lowercase_words and i != 0:
            result.append(word.lower())
        else:
            result.append(word.capitalize())
    
    return ' '.join(result)

geolocation = geolocation.withColumn(
    "geolocation_city_final",
    proper_title_case(col("geolocation_city_final"))
)

# Step 6: Data Quality Checks and Statistics
# Display a sample of the cleaned data for visual inspection
print("\nSample of cleaned data:")
geolocation.select(
    "geolocation_zip_code_prefix",
    "geolocation_city",
    "geolocation_city_final"
).show(25, truncate=False)

# Calculate and display cleaning statistics
total_records = geolocation.count()
print("\nCleaning Statistics:")
print(f"Total records processed: {total_records:,}")
print("Unique cities before cleaning:", 
      geolocation.select("geolocation_city").distinct().count())
print("Unique cities after cleaning:", 
      geolocation.select("geolocation_city_final").distinct().count())

# Show city changes (25 rows)
print("\nSample of city name changes:")
comparison = geolocation.select("geolocation_city", "geolocation_city_final").distinct()
comparison.filter(
    col("geolocation_city") != col("geolocation_city_final")
).show(25, truncate=False)

# Identify problematic cities
problematic_cities = geolocation.filter(
    (col("geolocation_city_final").rlike("[^a-zA-Z ]")) |
    (length(col("geolocation_city_final")) < 3)
).select("geolocation_city_final").distinct()

problem_count = problematic_cities.count()
total_distinct = geolocation.select("geolocation_city_final").distinct().count()

print(f"\nData Quality Metrics:")
print(f"Problematic cities: {problem_count} out of {total_distinct}")
print(f"Percentage: {problem_count/total_distinct*100:.2f}%")

# Show city frequency distribution with percentages (10 rows)
print("\nTop 10 cities by frequency:")
city_freq = geolocation.groupBy("geolocation_city_final") \
    .count() \
    .withColumn("percentage", (col("count") / total_records * 100).cast(DecimalType(10,2))) \
    .orderBy(col("count").desc())

city_freq.show(10, truncate=False)

# Show state distribution
print("\nState distribution:")
geolocation.groupBy("geolocation_state") \
    .count() \
    .withColumn("percentage", (col("count") / total_records * 100).cast(DecimalType(10,2))) \
    .orderBy(col("count").desc()) \
    .show(truncate=False)

# Clean up temporary columns
geolocation = geolocation.drop("geolocation_city_cleaned")

# Display 25 rows of the final cleaned dataframe
display(geolocation.limit(25))

print("\nCleaning process completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the cleaned_geolocation dataset

# COMMAND ----------

# Select only the required columns and create final DataFrame
cleaned_geolocation = geolocation.select(
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_state",
    "geolocation_city_final"
)

# Verify the schema
print("Final Schema:")
cleaned_geolocation.printSchema()

# Show sample of final dataset
print("\nSample of final cleaned dataset:")
cleaned_geolocation.show(5, truncate=False)

# Count final records
print(f"\nTotal records in cleaned dataset: {cleaned_geolocation.count():,}")
cleaned_geolocation.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the cleaned_geolocation to a parquet file with selected columns

# COMMAND ----------

# Define the output path
output_path = "/mnt/olist-store-data/transformed-data/olist_geolocation_cleaned_dataset_v2.0.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_parquet_output"

try:
    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (cleaned_geolocation
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
    cleaned_geolocation.unpersist()
