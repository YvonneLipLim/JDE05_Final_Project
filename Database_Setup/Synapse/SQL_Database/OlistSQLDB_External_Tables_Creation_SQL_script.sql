------------------------------------------------
-- Create credential
------------------------------------------------
CREATE DATABASE SCOPED CREDENTIAL chyef_gensg
WITH
    IDENTITY = 'Managed Identity'
GO

------------------------------------------------
-- Mount the transformed-data container
------------------------------------------------
CREATE EXTERNAL DATA SOURCE transformed_data
WITH
(
    LOCATION = 'https://olistbrdata.dfs.core.windows.net/olist-store-data/transformed-data',
    CREDENTIAL = chyef_gensg
)
GO

------------------------------------------------
-- Mount the ready-data container
------------------------------------------------
CREATE EXTERNAL DATA SOURCE ready_data
WITH
(
    LOCATION = 'https://olistbrdata.dfs.core.windows.net/olist-store-data/ready-data',
    CREDENTIAL = chyef_gensg
)
GO

------------------------------------------------
-- Set file format
------------------------------------------------
CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
GO

------------------------------------------------
-- Create external table external customers
------------------------------------------------
CREATE EXTERNAL TABLE extcustomers
WITH
(
    LOCATION = 'external_olist_customers',
    DATA_SOURCE = ready_data,
    FILE_FORMAT = format_parquet
) AS 
SELECT * FROM customers_view
GO

------------------------------------------------
-- Create external table geolocation
------------------------------------------------
CREATE EXTERNAL TABLE extgeolocation
WITH
(
    LOCATION = 'external_olist_geolocation',
    DATA_SOURCE = ready_data,
    FILE_FORMAT = format_parquet
) AS 
SELECT * FROM geolocation_view
GO

------------------------------------------------
-- Create external table order_items
------------------------------------------------
CREATE EXTERNAL TABLE extorder_items
WITH
(
    LOCATION = 'external_olist_order_items',
    DATA_SOURCE = ready_data,
    FILE_FORMAT = format_parquet
) AS 
SELECT * FROM order_items_view
GO

------------------------------------------------
-- Create external table order_payments
------------------------------------------------
CREATE EXTERNAL TABLE extorder_payments
WITH
(
    LOCATION = 'external_olist_order_payments',
    DATA_SOURCE = ready_data,
    FILE_FORMAT = format_parquet
) AS 
SELECT * FROM order_payments_view
GO

------------------------------------------------
-- Create external table product_category
------------------------------------------------
CREATE EXTERNAL TABLE extproduct_category
WITH
(
    LOCATION = 'external_olist_product_category',
    DATA_SOURCE = ready_data,
    FILE_FORMAT = format_parquet
) AS 
SELECT * FROM product_category_view
GO

------------------------------------------------
-- Create external table sellers
------------------------------------------------
CREATE EXTERNAL TABLE extsellers
WITH
(
    LOCATION = 'external_olist_sellers',
    DATA_SOURCE = ready_data,
    FILE_FORMAT = format_parquet
) AS 
SELECT * FROM sellers_view
GO

------------------------------------------------
-- Create external table orders
------------------------------------------------
CREATE EXTERNAL TABLE extorders
WITH
(
    LOCATION = 'external_olist_orders',
    DATA_SOURCE = ready_data,
    FILE_FORMAT = format_parquet
) AS 
SELECT * FROM orders_view
GO

------------------------------------------------
-- Create external table products
------------------------------------------------
CREATE EXTERNAL TABLE extproducts
WITH
(
    LOCATION = 'external_olist_products',
    DATA_SOURCE = ready_data,
    FILE_FORMAT = format_parquet
) AS 
SELECT * FROM products_view
GO

------------------------------------------------
-- Create external table order_reviews
------------------------------------------------
CREATE EXTERNAL TABLE extorder_reviews
WITH
(
    LOCATION = 'external_olist_order_reviews',
    DATA_SOURCE = ready_data,
    FILE_FORMAT = format_parquet
) AS 
SELECT * FROM order_reviews_view
GO
