USE OlistDB;

SELECT 
    DB_NAME() as database_name,
    s.name as schema_name,
    t.name as table_name,
    c.name as column_name,
    ty.name as data_type,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    CASE WHEN pk.column_id IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_primary_key
FROM sys.tables t
INNER JOIN sys.schemas s 
    ON t.schema_id = s.schema_id
INNER JOIN sys.columns c
    ON t.object_id = c.object_id
LEFT JOIN sys.types ty
    ON c.user_type_id = ty.user_type_id
LEFT JOIN sys.index_columns pk 
    ON c.object_id = pk.object_id 
    AND c.column_id = pk.column_id 
    AND pk.index_id = 1
WHERE t.name IN ('customers', 'geolocation', 'order_items', 
    'order_payments', 'order_reviews', 'orders', 
    'product_category', 'products', 'sellers')
ORDER BY t.name, c.column_id;
