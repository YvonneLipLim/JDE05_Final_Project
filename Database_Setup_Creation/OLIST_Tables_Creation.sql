-- OLIST: Brazilian E-Commerce Data Structure and Relationships

-- -------------------------------------------------
-- Table 1: Structure for customers
-- -------------------------------------------------
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
	customer_id VARCHAR(32) NOT NULL,
	customer_unique_id VARCHAR(32) NOT NULL,
	customer_zip_code_prefix INT NOT NULL,
	customer_city VARCHAR(255) NOT NULL,
	customer_state CHAR(2) NOT NULL,
	PRIMARY KEY (customer_id)
);

-- -------------------------------------------------
-- Table 2: Structure for geolocation
-- -------------------------------------------------
DROP TABLE IF EXISTS geolocation;
CREATE TABLE geolocation (
	geolocation_zip_code_prefix INT NOT NULL,
	geolocation_lat DECIMAL(12,8) NOT NULL,
	geolocation_lng DECIMAL(12,8) NOT NULL,
	geolocation_city VARCHAR(255) NOT NULL,
	geolocation_state CHAR(2) NOT NULL
);

-- -------------------------------------------------
-- Table 3: Structure for products
-- -------------------------------------------------
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    product_id VARCHAR(32) NOT NULL,
    product_category_name TEXT,
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    PRIMARY KEY (product_id),
	FOREIGN KEY (product_category_name) REFERENCES product_category_name_translation(product_category_name),
    CONSTRAINT positive_measurements CHECK (
        (product_weight_g IS NULL OR product_weight_g >= 0) AND  
        (product_length_cm IS NULL OR product_length_cm > 0) AND
        (product_height_cm IS NULL OR product_height_cm > 0) AND
        (product_width_cm IS NULL OR product_width_cm > 0)
    )
);

-- -------------------------------------------------
-- Table 4: Structure for sellers
-- -------------------------------------------------
DROP TABLE IF EXISTS sellers;
CREATE TABLE sellers (
    seller_id VARCHAR(32) NOT NULL,
    seller_zip_code_prefix INT NOT NULL,
    seller_city VARCHAR(255) NOT NULL,
    seller_state CHAR(2) NOT NULL,
    PRIMARY KEY (seller_id)
);

-- -------------------------------------------------
-- Table 5: Structure for orders
-- -------------------------------------------------
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id VARCHAR(32) NOT NULL,
    customer_id VARCHAR(32) NOT NULL,
    order_status VARCHAR(12) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP NOT NULL,
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT valid_order_status CHECK (order_status IN ('approved', 'canceled', 'created', 'delivered', 'invoiced', 'processing', 'shipped', 'unavailable'))
);

-- -------------------------------------------------
-- Table 6: Structure for order items
-- -------------------------------------------------
DROP TABLE IF EXISTS order_items;
CREATE TABLE order_items (
    order_id VARCHAR(32) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(32) NOT NULL,
    seller_id VARCHAR(32) NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price DECIMAL(12, 2) NOT NULL CHECK (price > 0),
    freight_value DECIMAL(12, 2) NOT NULL CHECK (freight_value >= 0),
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- -------------------------------------------------
-- Table 7: Structure for order payments
-- -------------------------------------------------
DROP TABLE IF EXISTS order_payments;
CREATE TABLE order_payments (
    order_id VARCHAR(32) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(20) NOT NULL,
    payment_installments INT NOT NULL,
    payment_value DECIMAL(12, 2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT valid_payment_type CHECK (payment_type IN ('credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined'))
);

-- -------------------------------------------------
-- Table 8: Structure for order reviews
-- -------------------------------------------------
DROP TABLE IF EXISTS order_reviews;
CREATE TABLE order_reviews (
    review_id VARCHAR(32) NOT NULL,
    order_id VARCHAR(32) NOT NULL,
    review_score INT NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP,  
    PRIMARY KEY (review_id, order_id),  
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);


-- -------------------------------------------------
-- Table 9: Structure for product catergory name translation
-- -------------------------------------------------
DROP TABLE IF EXISTS product_category_name_translation;
CREATE TABLE product_category_name_translation (
    product_category_name TEXT NOT NULL,
    product_category_name_english TEXT NOT NULL,
    PRIMARY KEY (product_category_name)
);

-- -------------------------------------------------
-- Import CSV
-- -------------------------------------------------
COPY customers
FROM '/private/tmp/olist_customers_dataset.csv' -- Change to your local directory
DELIMITER ','
CSV HEADER;

COPY geolocation
FROM '/private/tmp/olist_geolocation_dataset.csv' -- Change to your local directory
DELIMITER ','
CSV HEADER;

COPY products
FROM '/private/tmp/olist_products_dataset.csv' -- Change to your local directory
DELIMITER ','
CSV HEADER;

COPY sellers
FROM '/private/tmp/olist_sellers_dataset.csv' -- Change to your local directory
DELIMITER ','
CSV HEADER;

COPY orders
FROM '/private/tmp/olist_orders_dataset.csv' -- Change to your local directory
DELIMITER ','
CSV HEADER;

COPY order_items
FROM '/private/tmp/olist_order_items_dataset.csv' -- Change to your local directory
DELIMITER ','
CSV HEADER;

COPY order_payments
FROM '/private/tmp/olist_order_payments_dataset.csv' -- Change to your local directory
DELIMITER ','
CSV HEADER;

COPY order_reviews
FROM '/private/tmp/olist_order_reviews_dataset.csv' -- Change to your local directory
DELIMITER ','
CSV HEADER;

COPY product_category_name_translation
FROM '/private/tmp/product_category_name_translation.csv' -- Change to your local directory
DELIMITER ','
CSV HEADER;
