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
# MAGIC ### Read product_category dataset from raw-data folder

# COMMAND ----------

product_category = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/product_category_name_translation.csv")

# COMMAND ----------

product_category.show(10)
product_category.printSchema()
display(product_category.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data cleaning for product_catergory dataset
# MAGIC - **Step 1**: Basic text cleaning (lowercase, trim, special character removal).
# MAGIC
# MAGIC - **Step 2**: Title case conversion for English category names.
# MAGIC
# MAGIC - **Step 3**: Plural form detection and consistency checking.
# MAGIC
# MAGIC - **Step 4**: Special case handling for specific categories.
# MAGIC
# MAGIC - **Step 5**: Similarity calculation between Portuguese and English names.

# COMMAND ----------

# This script performs data cleaning and analysis on the product_category dataset.
# It standardizes category names, checks for consistency between Portuguese and English versions,
# and identifies potential mismatches or inconsistencies.

# Import libraries
from pyspark.sql.functions import (
    col, lower, when, levenshtein, count as count_, 
    sum as sum_, greatest, length, regexp_replace, trim,
    initcap  # Add initcap for title case conversion
)

# Define helper function to check if a category name is likely to be plural
def is_likely_plural(column_name):
    return when(
        (lower(col(column_name)).endswith('s') & 
         ~lower(col(column_name)).endswith('ss') &
         ~lower(col(column_name)).isin('business', 'press', 'express')) |
        (lower(col(column_name)).endswith('is') |
         lower(col(column_name)).endswith('es') |
         lower(col(column_name)).endswith('ns')),
        True
    ).when(
        lower(col(column_name)).isin(
            'alimentos', 'bebes', 'artes',
            'housewares', 'supplies', 'clothes'
        ),
        True
    ).otherwise(False)

# Define function to apply specific mappings and rules for special cases in category names
def apply_category_mapping(df):
    return df.withColumn(
        "is_plural_consistent",
        when(
            # Handle Portuguese-English special cases
            (lower(col("product_category_name_clean")).isin(
                "alimentos_bebidas", "bebes", "artes", "alimentos"
            )) & 
            (lower(col("product_category_name_english_clean")).isin(
                "food_drink", "baby", "art", "food"
            )),
            True
        ).when(
            # Handle tools/ferramentas pattern
            (col("product_category_name_clean").like("%ferramentas%")) & 
            (col("product_category_name_english_clean").like("%tools%")),
            True
        ).when(
            # Handle supplies pattern
            col("product_category_name_english_clean").like("%supplies%"),
            True
        ).when(
            # Handle clothes pattern
            col("product_category_name_english_clean").like("%clothes%"),
            True
        ).when(
            # Handle books pattern
            (col("product_category_name_clean").like("%livros_%")) & 
            (col("product_category_name_english_clean").like("books_%")),
            True
        ).when(
            # Handle business/commerce terms
            (col("product_category_name_clean").like("%comercio%")) & 
            (col("product_category_name_english_clean").like("%commerce%")),
            True
        ).when(
            # Handle industry/business terms
            (col("product_category_name_clean").like("%negocios%")) & 
            (col("product_category_name_english_clean").like("%business%")),
            True
        ).otherwise(col("is_plural_consistent"))
    )

# Define function to calculate similarity between Portuguese and English category names
def calculate_similarity(df):
    # First normalize common terms
    normalized_df = df.withColumn(
        "normalized_pt",
        when(col("product_category_name_clean").like("%portateis_casa_forno_e_cafe%"), 
             "small_appliances_home_oven_and_coffee")
        .when(col("product_category_name_clean").like("%fashion_roupa_infanto_juvenil%"),
              "fashion_childrens_clothes")
        .when(col("product_category_name_clean").like("%utilidades_domesticas%"),
              "housewares")
        .when(col("product_category_name_clean").like("%instrumentos_musicais%"),
              "musical_instruments")
        .when(col("product_category_name_clean").like("%livros_interesse_geral%"),
              "books_general_interest")
        .when(col("product_category_name_clean").like("%moveis_cozinha_area_de_servico_jantar_e_jardim%"), 
             "kitchen_dining_laundry_garden_furniture")
        .when(col("product_category_name_clean").like("%moveis_colchao_e_estofado%"),
              "furniture_mattress_and_upholstery")
        .when(col("product_category_name_clean").like("%moveis%"), 
             regexp_replace(col("product_category_name_clean"), "moveis", "furniture"))
        .when(col("product_category_name_clean").like("%construcao_ferramentas_ferramentas%"),
              "construction_tools_tools")
        .when(col("product_category_name_clean").like("%construcao_ferramentas%"),
              regexp_replace(
                  regexp_replace(col("product_category_name_clean"), 
                               "construcao", "construction"),
                  "ferramentas", "tools"
              ))
        .otherwise(col("product_category_name_clean"))
    ).withColumn(
        "normalized_en",
        when(col("product_category_name_english_clean").like("%costruction%"),
             regexp_replace(col("product_category_name_english_clean"), "costruction", "construction"))
        .otherwise(col("product_category_name_english_clean"))
    )
    
    return normalized_df.withColumn(
        "name_similarity",
        when(
            (col("normalized_pt") == col("normalized_en")),
            0
        ).when(
            (col("product_category_name_clean").isin(
                "portateis_casa_forno_e_cafe",
                "fashion_roupa_infanto_juvenil",
                "utilidades_domesticas",
                "instrumentos_musicais",
                "livros_interesse_geral"
            )),
            5
        ).otherwise(
            levenshtein(
                col("normalized_pt"),
                col("normalized_en")
            )
        )
    )

# Main cleaning function
def clean_product_categories(df):
    # Basic cleaning
    cleaned_df = df.withColumn(
        "product_category_name_clean",
        regexp_replace(lower(trim(col("product_category_name"))), "[^a-z0-9_]", "_")
    ).withColumn(
        "product_category_name_english_clean",
        regexp_replace(lower(trim(col("product_category_name_english"))), "[^a-z0-9_]", "_")
    )
    
    # Convert English category names to title case
    cleaned_df = cleaned_df.withColumn(
        "product_category_name_english_title",
        initcap(regexp_replace(col("product_category_name_english_clean"), "_", " "))
    )
    
    # Check for plural consistency
    cleaned_df = cleaned_df.withColumn(
        "is_original_plural",
        is_likely_plural("product_category_name_clean")
    ).withColumn(
        "is_english_plural",
        is_likely_plural("product_category_name_english_clean")
    )
    
    # Create is_plural_consistent column
    cleaned_df = cleaned_df.withColumn(
        "is_plural_consistent",
        col("is_original_plural") == col("is_english_plural")
    )
    
    # Apply category mapping and calculate name similarity
    cleaned_df = (apply_category_mapping(cleaned_df)
                 .transform(calculate_similarity))
    
    return cleaned_df

# Process the data
cleaned_product_category = clean_product_categories(product_category)

# Display results and analysis
print("Singular/Plural Inconsistencies:")
cleaned_product_category.filter(~col("is_plural_consistent")).select(
    "product_category_name_clean", 
    "product_category_name_english_clean",
    "product_category_name_english_title",
    "is_original_plural", 
    "is_english_plural"
).show(truncate=False)

# Calculate and display metrics
total_rows = cleaned_product_category.count()
cleaned_product_category.agg(
    sum_(col("is_plural_consistent").cast("int")).alias("consistent_count"),
    sum_(when(col("name_similarity") == 0, 1).otherwise(0)).alias("exact_match_count"),
    (sum_(col("is_plural_consistent").cast("int")) / count_("*") * 100).alias("plural_consistency_percentage"),
    (sum_(when(col("name_similarity") == 0, 1).otherwise(0)) / count_("*") * 100).alias("exact_match_percentage")
).show()

print("Categories with high name dissimilarity:")
cleaned_product_category.filter(col("name_similarity") > 15).orderBy(
    col("name_similarity").desc()
).select(
    "product_category_name_clean",
    "product_category_name_english_clean",
    "product_category_name_english_title",
    "name_similarity"
).show(10, truncate=False)

# Display final result
display(cleaned_product_category.limit(10))

print("\nCleaning process completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the cleaned_product_category dataset

# COMMAND ----------

# Select only the required columns and create final DataFrame
cleaned_product_categories = cleaned_product_category.select(
    "product_category_name_clean",
    "product_category_name_english_title"
)

# Verify final schema
print("\nFinal Schema:")
cleaned_product_categories.printSchema()

# Show sample of final dataset
print("\nSample of final cleaned dataset:")
cleaned_product_categories.show(5, truncate=False)

# Print final record count
print(f"\nTotal records in cleaned dataset: {cleaned_product_categories.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the cleaned_product_category to a parquet file

# COMMAND ----------

# Define the output path for final cleaned and translated dataset
output_path = "/mnt/olist-store-data/transformed-data/olist_product_category_cleaned_dataset_final_v2.0.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_parquet_output"

try:
    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (cleaned_product_categories
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
    cleaned_product_categories.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read products dataset from raw-data folder

# COMMAND ----------

products = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olist-store-data/raw-data/olist_products_dataset.csv")

# COMMAND ----------

products.show(10)
products.printSchema()
display(products.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data cleaning for products dataset
# MAGIC - **Step 1: Data Loading and Initial Processing**<br>
# MAGIC The function clean_products_dataset starts by loading two datasets:
# MAGIC   - The main products dataset from a CSV file
# MAGIC   - A cleaned product categories dataset from a Parquet file
# MAGIC These datasets are joined to replace the original category names with English titles. The function also renames misspelled columns and performs initial data quality checks.
# MAGIC
# MAGIC - **Step 2: Data Quality Assessment**:<br>
# MAGIC The code performs several data quality checks:
# MAGIC   - Missing Values Analysis: It calculates and displays the count and percentage of missing values for each column.
# MAGIC   - Duplicate Check: It identifies and reports the number of duplicate product IDs.
# MAGIC
# MAGIC - **Step 3: Data Cleaning and Transformation**:<br>
# MAGIC The cleaning process involves several steps:
# MAGIC   - Basic Cleaning: Removes records with null values in critical fields.
# MAGIC   - Text Standardization: Converts category names to lowercase and trims whitespace.
# MAGIC   - Numeric Value Handling: Replaces zero or negative values with null in numeric columns.
# MAGIC   - Derived Metrics: Calculates product volume and density.
# MAGIC
# MAGIC - **Step 4: Feature Engineering**:<br>
# MAGIC The code creates several new features to enhance the dataset:
# MAGIC   - **Volume Tier**:<br>Categorizes products based on their volume (Extra Small to Extra Large). This categorization helps in understanding the size distribution of products and can be useful for inventory management and shipping considerations. It's calculated using quartiles of the product_volume_cm3 field:
# MAGIC     - Extra Small: <= q1_volume/2
# MAGIC     - Small: <= q1_volume
# MAGIC     - Medium Small: <= median_volume
# MAGIC     - Medium Large: <= q3_volume
# MAGIC     - Large: <= q3_volume*2
# MAGIC     - Extra Large: > q3_volume*2
# MAGIC
# MAGIC   - **Weight Category**:<br>Classifies products by weight (Ultra Light to Extra Heavy). This categorization is valuable for shipping calculations, warehouse organization, and logistics planning. The weight_category classifies products based on their weight:
# MAGIC     - Ultra Light: <= 250g
# MAGIC     - Light: <= q1_weight
# MAGIC     - Medium Light: <= median_weight
# MAGIC     - Medium Heavy: <= q3_weight
# MAGIC     - Heavy: <= q3_weight*2
# MAGIC     - Extra Heavy: > q3_weight*2  
# MAGIC
# MAGIC   - **Product Complexity**:<br>Assigns complexity levels based on description length, photo quantity, volume, and weight. This metric can be useful for customer support, product management, and marketing strategies. The product_complexity field assesses the overall complexity of a product based on multiple factors:
# MAGIC     - Very High: Long description, many photos, large volume, heavy weight
# MAGIC     - High: Above average description length, multiple photos, above average volume
# MAGIC     - Medium: Default category
# MAGIC     - Medium Low: Below average description length, few photos, below average volume
# MAGIC     - Low: Short description, single photo, small volume, light weight
# MAGIC
# MAGIC   - **Shipping Complexity**:<br>Categorizes products as Simple, Standard, or Complex based on weight and dimensions. This categorization is crucial for logistics planning, shipping cost estimation, and warehouse management. The shipping_complexity categorizes products based on their shipping requirements:
# MAGIC     - Complex: Heavy weight, large volume, or any dimension > 100cm
# MAGIC     - Simple: Light weight and small volume
# MAGIC     - Standard: Everything else
# MAGIC
# MAGIC   - **Category Group**:<br>Groups products into broader categories like "Home & Decor", "Health & Fashion", etc. This grouping allows for higher-level analysis of product trends and can be useful for marketing, inventory management, and strategic decision-making. The category_group field aggregates similar product categories into broader groups:
# MAGIC     - Home & Decor
# MAGIC     - Health & Fashion
# MAGIC     - Electronics & Tech
# MAGIC     - Kids & Baby
# MAGIC     - Gifts & Personal
# MAGIC     - Food & Beverage
# MAGIC     - Tools & Construction
# MAGIC     - Other
# MAGIC
# MAGIC - **Step 5: Statistical Analysis and Reporting**:<br>
# MAGIC The code calculates and reports various statistics:
# MAGIC   - Quartiles for volume, weight, and description length.
# MAGIC   - Distribution of products across the newly created categories.
# MAGIC
# MAGIC - **Step 6: Final Cleaning and Output**:
# MAGIC The function concludes by:
# MAGIC   - Reporting the final record count and data retention rate.
# MAGIC   - Displaying the cleaned dataset's schema.
# MAGIC   - Showing a sample of the cleaned data.

# COMMAND ----------

# This script performs data cleaning and analysis on the products dataset.
# It standardizes product categories, handles missing values, and adds derived features.

# Import libraries
from pyspark.sql.functions import (
    col, sum, count, when, isnan, 
    regexp_replace, lower, trim, upper, concat,
    avg, stddev, min, max, round,
    array, explode, concat_ws, percentile_approx, lit, udf
)
from pyspark.sql.types import IntegerType, StringType

def sentence_case(text):
    if text:
        words = text.split()
        return ' '.join(word.capitalize() for word in words)
    return None

sentence_case_udf = udf(sentence_case, StringType())

def clean_products_dataset(spark):
    """
    This function performs comprehensive data cleaning and analysis on the sellers dataset.
    It standardizes city and state names, adds derived features, and generates various analytical reports.
    """
    try:
        # Load the sellers dataset from CSV
        print("Loading products dataset...")
        products = spark.read.format("csv").option("header","true").option("inferSchema","true")\
            .load("/mnt/olist-store-data/raw-data/olist_products_dataset.csv")
        
        cleaned_product_categories = spark.read.parquet("/mnt/olist-store-data/transformed-data/olist_product_category_cleaned_dataset_final_v2.0.parquet")

        products = products.join(
            cleaned_product_categories.select("product_category_name_clean", "product_category_name_english_title"),
            products["product_category_name"] == cleaned_product_categories["product_category_name_clean"],
            "left"
        )
        products = products.drop("product_category_name", "product_category_name_clean") \
                   .withColumnRenamed("product_category_name_english_title", "product_category_name")
        
        products = products.withColumnRenamed("product_name_lenght", "product_name_length") \
                           .withColumnRenamed("product_description_lenght", "product_description_length")
        
        initial_count = products.count()
        
        print("\nInitial dataset information:")
        print(f"Number of records: {initial_count:,}")
        print(f"Number of columns: {len(products.columns)}")
        
        print("\nMissing values analysis:")
        missing_values = products.select([
            sum(col(c).isNull().cast("int")).alias(c) for c in products.columns
        ])
        
        for column in products.columns:
            missing_count = missing_values.collect()[0][column]
            missing_percentage = (missing_count / initial_count) * 100
            print(f"{column}: {missing_count:,} missing values ({missing_percentage:.2f}%)")
        
        print("\nMissing values count:")
        missing_values.show()
        
        print("\nChecking for duplicate products...")
        duplicates = products.groupBy("product_id").count().filter(col("count") > 1)
        duplicate_count = duplicates.count()
        duplicate_percentage = (duplicate_count / initial_count) * 100
        print(f"Number of duplicate product_ids: {duplicate_count:,} ({duplicate_percentage:.2f}%)")
        
        print("\nStarting data cleaning process...")
        
        cleaned_products = products.filter(
            col("product_id").isNotNull() &
            col("product_category_name").isNotNull()
        )
        
        # Convert product_category_name to sentence case
        cleaned_products = cleaned_products.withColumn("product_category_name", sentence_case_udf(trim(col("product_category_name")))
        )

        
        numeric_columns = [
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        ]
        
        for column in numeric_columns:
            cleaned_products = cleaned_products.withColumn(
                column,
                when(col(column) <= 0, None).otherwise(col(column))
            )
        
        cleaned_products = cleaned_products.withColumn(
            "product_volume_cm3",
            col("product_length_cm") * col("product_height_cm") * col("product_width_cm")
        ).withColumn(
            "product_density_g_cm3",
            round(col("product_weight_g") / col("product_volume_cm3"), 3)
        )
        
        print("\nCalculating data statistics...")
        stats = cleaned_products.select(
            percentile_approx("product_volume_cm3", array(lit(0.25), lit(0.5), lit(0.75))).alias("volume_percentiles"),
            percentile_approx("product_weight_g", array(lit(0.25), lit(0.5), lit(0.75))).alias("weight_percentiles"),
            percentile_approx("product_description_length", array(lit(0.25), lit(0.5), lit(0.75))).alias("description_percentiles")
        ).collect()[0]
        
        q1_volume, median_volume, q3_volume = stats["volume_percentiles"]
        q1_weight, median_weight, q3_weight = stats["weight_percentiles"]
        q1_desc, median_desc, q3_desc = stats["description_percentiles"]
        
        cleaned_products = cleaned_products.withColumn(
            "volume_tier",
            when(col("product_volume_cm3") <= q1_volume/2, "Extra Small")
            .when(col("product_volume_cm3") <= q1_volume, "Small")
            .when(col("product_volume_cm3") <= median_volume, "Medium Small")
            .when(col("product_volume_cm3") <= q3_volume, "Medium Large")
            .when(col("product_volume_cm3") <= q3_volume*2, "Large")
            .otherwise("Extra Large")
        )
        
        cleaned_products = cleaned_products.withColumn(
            "weight_category",
            when(col("product_weight_g") <= 250, "Ultra Light")
            .when(col("product_weight_g") <= q1_weight, "Light")
            .when(col("product_weight_g") <= median_weight, "Medium Light")
            .when(col("product_weight_g") <= q3_weight, "Medium Heavy")
            .when(col("product_weight_g") <= q3_weight*2, "Heavy")
            .otherwise("Extra Heavy")
        )
        
        cleaned_products = cleaned_products.withColumn(
            "product_complexity",
            when(
                (col("product_description_length") > q3_desc) & 
                (col("product_photos_qty") > 2) & 
                (col("product_volume_cm3") > q3_volume) &
                (col("product_weight_g") > q3_weight),
                "Very High"
            )
            .when(
                (col("product_description_length") > median_desc) & 
                (col("product_photos_qty") > 1) & 
                (col("product_volume_cm3") > median_volume),
                "High"
            )
            .when(
                (col("product_description_length") < q1_desc) & 
                (col("product_photos_qty") == 1) & 
                (col("product_volume_cm3") < q1_volume) &
                (col("product_weight_g") < q1_weight),
                "Low"
            )
            .when(
                (col("product_description_length") < median_desc) & 
                (col("product_photos_qty") <= 2) & 
                (col("product_volume_cm3") < median_volume),
                "Medium Low"
            )
            .otherwise("Medium")
        )
        
        cleaned_products = cleaned_products.withColumn(
            "shipping_complexity",
            when(
                (col("product_weight_g") > q3_weight) | 
                (col("product_volume_cm3") > q3_volume) |
                (col("product_length_cm") > 100) |
                (col("product_height_cm") > 100) |
                (col("product_width_cm") > 100),
                "Complex"
            )
            .when(
                (col("product_weight_g") <= q1_weight) & 
                (col("product_volume_cm3") <= q1_volume),
                "Simple"
            )
            .otherwise("Standard")
        )
        
        cleaned_products = cleaned_products.withColumn(
        "category_group",
            when(
                lower(col("product_category_name")).isin(
                    "bed bath table", "furniture decor", "home construction", 
                    "housewares", "office furniture", "furniture living room",
                    "kitchen dining laundry garden furniture", "home comfort"
                ), 
                "Home & Decor"
            )
            .when(
                lower(col("product_category_name")).isin(
                    "sports leisure", "health beauty", "fashion bags accessories", 
                    "fashion shoes", "fashion male clothing", "luggage accessories"
                ), 
                "Health & Fashion"
            )
            .when(
                lower(col("product_category_name")).isin(
                    "computers accessories", "telephony", "electronics", "pc gamer",
                    "tablets printing image", "fixed telephony", "home appliances",
                    "home appliances 2", "small appliances", "air conditioning"
                ), 
                "Electronics & Tech"
            )
            .when(
                lower(col("product_category_name")).isin(
                    "toys", "baby", "fashion children clothes"
                ), 
                "Kids & Baby"
            )
            .when(
                lower(col("product_category_name")).isin(
                    "watches gifts", "perfumery", "art", "christmas articles"
                ), 
                "Gifts & Personal"
            )
            .when(
                lower(col("product_category_name")).isin(
                    "food", "drinks", "food drink"
                ),
                "Food & Beverage"
            )
            .when(
                lower(col("product_category_name")).isin(
                    "garden tools", "construction tools construction",
                    "construction tools garden", "construction tools safety",
                    "construction tools lights"
                ),
                "Tools & Construction"
            )
            .otherwise("Other")
        )
        
        print("\nVolume Tier Distribution:")
        cleaned_products.groupBy("volume_tier").count()\
            .withColumn("percentage", round(col("count") / cleaned_products.count() * 100, 2))\
            .orderBy("count", ascending=False).show()
        
        print("\nWeight Category Distribution:")
        cleaned_products.groupBy("weight_category").count()\
            .withColumn("percentage", round(col("count") / cleaned_products.count() * 100, 2))\
            .orderBy("count", ascending=False).show()
        
        print("\nProduct Complexity Distribution:")
        cleaned_products.groupBy("product_complexity").count()\
            .withColumn("percentage", round(col("count") / cleaned_products.count() * 100, 2))\
            .orderBy("count", ascending=False).show()
        
        print("\nShipping Complexity Distribution:")
        cleaned_products.groupBy("shipping_complexity").count()\
            .withColumn("percentage", round(col("count") / cleaned_products.count() * 100, 2))\
            .orderBy("count", ascending=False).show()
        
        print("\nCategory Group Distribution:")
        cleaned_products.groupBy("category_group").count()\
            .withColumn("percentage", round(col("count") / cleaned_products.count() * 100, 2))\
            .orderBy("count", ascending=False).show()
        
        final_count = cleaned_products.count()
        removed_count = initial_count - final_count
        retention_rate = (final_count / initial_count) * 100
        removal_rate = (removed_count / initial_count) * 100
        
        print("\nCleaning Summary:")
        print(f"Original record count: {initial_count:,}")
        print(f"Cleaned record count: {final_count:,}")
        print(f"Records removed: {removed_count:,}")
        print(f"Data retention rate: {retention_rate:.2f}%")
        print(f"Data removal rate: {removal_rate:.2f}%")
        
        print("\nCleaned Dataset Schema:")
        cleaned_products.printSchema()
        
        print("\nSample of Cleaned Data:")
        cleaned_products.select(
            "product_id", "product_category_name", "category_group", "product_complexity", 
            "shipping_complexity", "volume_tier", "weight_category"
        ).show(5, truncate=True)
        
        display(cleaned_products.limit(10))
        print("\nCleaning process completed.")
        return cleaned_products
        
    except Exception as e:
        print(f"\nError in data cleaning process: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        cleaned_products = clean_products_dataset(spark)
    except Exception as e:
        print(f"Failed to clean products dataset: {str(e)}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the cleaned_products_withCalculations dataset

# COMMAND ----------

# Verify final schema
print("\nFinal Schema:")
cleaned_products.printSchema()

# Show sample of final dataset
print("\nSample of final cleaned dataset:")
cleaned_products.show(5, truncate=False)

# Print final record count
print(f"\nTotal records in cleaned dataset: {cleaned_products.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the clean_products_withCalculations dataset in parquet

# COMMAND ----------

# Define the output path for final cleaned and translated dataset
output_path = "/mnt/olist-store-data/transformed-data/olist_products_cleaned_dataset_withCalculations.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_parquet_output"

try:
    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (cleaned_products
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
    cleaned_products.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the cleaned_products dataset with selected columns

# COMMAND ----------

from pyspark.sql.functions import col

def preview_products(spark):
    # Load the existing cleaned products dataset
    cleaned_products = spark.read.parquet("/mnt/olist-store-data/transformed-data/olist_products_cleaned_dataset_withCalculations.parquet")

    # Create DataFrame with selected columns
    cleaned_selected_products = cleaned_products.select(
        "product_id",
        "product_category_name",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    )

    # Verify final schema
    print("\nFinal Schema:")
    cleaned_selected_products.printSchema()

    # Show sample of final dataset
    print("\nSample of final cleaned dataset:")
    display(cleaned_selected_products.limit(5))

    # Print final record count
    print(f"\nTotal records in cleaned dataset: {cleaned_selected_products.count():,}")

# Call the function with the spark session
preview_products(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the cleaned_products with selected columns in parquet file

# COMMAND ----------

# Define the output path for final cleaned and translated dataset
output_path = "/mnt/olist-store-data/transformed-data/olist_products_cleaned_dataset_v2.0.parquet"
temp_path = "/mnt/olist-store-data/transformed-data/temp_products_parquet"

try:
    # Remove existing directories if they exist
    dbutils.fs.rm(output_path, recurse=True)
    dbutils.fs.rm(temp_path, recurse=True)

    # Save as a single Parquet file using temporary directory
    (selected_products
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
    selected_products.unpersist()
