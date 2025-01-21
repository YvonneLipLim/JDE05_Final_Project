---------------------------------------------------
-- View 1: Customers
---------------------------------------------------
CREATE VIEW customers_view
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://olistbrdata.dfs.core.windows.net/olist-store-data/transformed-data/olist_customers_cleaned_dataset_v2.0.parquet',
            FORMAT = 'PARQUET'
        ) as QUER1
GO

---------------------------------------------------
-- View 2: Geolocation
---------------------------------------------------
CREATE VIEW geolocation_view
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://olistbrdata.dfs.core.windows.net/olist-store-data/transformed-data/olist_geolocation_cleaned_dataset_v2.0.parquet',
            FORMAT = 'PARQUET'
        ) as QUER1
GO

---------------------------------------------------
-- View 3: Orders
---------------------------------------------------
CREATE VIEW orders_view
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://olistbrdata.dfs.core.windows.net/olist-store-data/transformed-data/olist_orders_cleaned_dataset_v2.0.parquet',
            FORMAT = 'PARQUET'
        ) as QUER1
GO

---------------------------------------------------
-- View 4: Order_Items
---------------------------------------------------
CREATE VIEW order_items_view
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://olistbrdata.dfs.core.windows.net/olist-store-data/transformed-data/olist_order_items_cleaned_dataset_v2.0.parquet',
            FORMAT = 'PARQUET'
        ) as QUER1
GO

---------------------------------------------------
-- View 5: Order_Payments
---------------------------------------------------
CREATE VIEW order_payments_view
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://olistbrdata.dfs.core.windows.net/olist-store-data/transformed-data/olist_order_payments_cleaned_dataset_v2.0.parquet',
            FORMAT = 'PARQUET'
        ) as QUER1
GO

---------------------------------------------------
-- View 6: Order_Reviews
---------------------------------------------------
CREATE VIEW order_reviews_view
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://olistbrdata.dfs.core.windows.net/olist-store-data/transformed-data/olist_order_reviews_cleaned_dataset_v2.0.parquet',
            FORMAT = 'PARQUET'
        ) as QUER1
GO

---------------------------------------------------
-- View 7: Products
---------------------------------------------------
CREATE VIEW products_view
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://olistbrdata.dfs.core.windows.net/olist-store-data/transformed-data/olist_products_cleaned_dataset_v2.0.parquet',
            FORMAT = 'PARQUET'
        ) as QUER1
GO

---------------------------------------------------
-- View 8: Product_Category
---------------------------------------------------
CREATE VIEW product_category_view
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://olistbrdata.dfs.core.windows.net/olist-store-data/transformed-data/olist_product_category_cleaned_dataset_final_v2.0.parquet',
            FORMAT = 'PARQUET'
        ) as QUER1
GO

---------------------------------------------------
-- View 9: Sellers
---------------------------------------------------
CREATE VIEW sellers_view
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://olistbrdata.dfs.core.windows.net/olist-store-data/transformed-data/olist_sellers_cleaned_dataset_v2.0.parquet',
            FORMAT = 'PARQUET'
        ) as QUER1
GO
