-- ============================================================
-- STAGING LAYER
-- Purpose: Type casting, deduplication, basic cleaning
-- Source: raw schema (direct CSV imports)
-- Convention: stg_<entity>
-- ============================================================

DROP SCHEMA IF EXISTS staging CASCADE;
CREATE SCHEMA staging;

-- ============================================================
-- stg_customers
-- One row per customer (by customer_id)
-- ============================================================
CREATE TABLE staging.stg_customers AS
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    INITCAP(TRIM(customer_city))    AS customer_city,
    UPPER(TRIM(customer_state))     AS customer_state
FROM raw.customers;

ALTER TABLE staging.stg_customers
    ADD PRIMARY KEY (customer_id);


-- ============================================================
-- stg_orders
-- One row per order, with properly typed timestamps
-- ============================================================
CREATE TABLE staging.stg_orders AS
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::TIMESTAMP         AS order_purchase_timestamp,
    order_approved_at::TIMESTAMP                AS order_approved_at,
    order_delivered_carrier_date::TIMESTAMP      AS order_delivered_carrier_date,
    order_delivered_customer_date::TIMESTAMP     AS order_delivered_customer_date,
    order_estimated_delivery_date::TIMESTAMP     AS order_estimated_delivery_date
FROM raw.orders;

ALTER TABLE staging.stg_orders
    ADD PRIMARY KEY (order_id);


-- ============================================================
-- stg_order_items
-- One row per item in an order
-- Composite key: (order_id, order_item_id)
-- ============================================================
CREATE TABLE staging.stg_order_items AS
SELECT
    order_id,
    order_item_id::INTEGER          AS order_item_id,
    product_id,
    seller_id,
    shipping_limit_date::TIMESTAMP  AS shipping_limit_date,
    price::NUMERIC(10,2)            AS price,
    freight_value::NUMERIC(10,2)    AS freight_value
FROM raw.order_items;

ALTER TABLE staging.stg_order_items
    ADD PRIMARY KEY (order_id, order_item_id);


-- ============================================================
-- stg_order_payments
-- Multiple payment methods per order
-- Composite key: (order_id, payment_sequential)
-- ============================================================
CREATE TABLE staging.stg_order_payments AS
SELECT
    order_id,
    payment_sequential::INTEGER     AS payment_sequential,
    payment_type,
    payment_installments::INTEGER   AS payment_installments,
    payment_value::NUMERIC(10,2)    AS payment_value
FROM raw.order_payments;

ALTER TABLE staging.stg_order_payments
    ADD PRIMARY KEY (order_id, payment_sequential);


-- ============================================================
-- stg_order_reviews
-- One review per order (take the latest if duplicates exist)
-- ============================================================
CREATE TABLE staging.stg_order_reviews AS
SELECT DISTINCT ON (order_id)
    review_id,
    order_id,
    review_score::INTEGER           AS review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date::TIMESTAMP AS review_creation_date,
    review_answer_timestamp::TIMESTAMP AS review_answer_timestamp
FROM raw.order_reviews
ORDER BY order_id, review_creation_date::TIMESTAMP DESC;

ALTER TABLE staging.stg_order_reviews
    ADD PRIMARY KEY (order_id);


-- ============================================================
-- stg_products
-- One row per product
-- ============================================================
CREATE TABLE staging.stg_products AS
SELECT
    p.product_id,
    COALESCE(ct.product_category_name_english, p.product_category_name, 'unknown')
        AS product_category,
    NULLIF(p.product_name_length, '')::INTEGER          AS product_name_length,
    NULLIF(p.product_description_length, '')::INTEGER   AS product_description_length,
    NULLIF(p.product_photos_qty, '')::INTEGER           AS product_photos_qty,
    NULLIF(p.product_weight_g, '')::NUMERIC             AS product_weight_g,
    NULLIF(p.product_length_cm, '')::NUMERIC            AS product_length_cm,
    NULLIF(p.product_height_cm, '')::NUMERIC            AS product_height_cm,
    NULLIF(p.product_width_cm, '')::NUMERIC             AS product_width_cm
FROM raw.products p
LEFT JOIN raw.category_translation ct
    ON p.product_category_name = ct.product_category_name;

ALTER TABLE staging.stg_products
    ADD PRIMARY KEY (product_id);


-- ============================================================
-- stg_sellers
-- One row per seller
-- ============================================================
CREATE TABLE staging.stg_sellers AS
SELECT
    seller_id,
    seller_zip_code_prefix,
    INITCAP(TRIM(seller_city))  AS seller_city,
    UPPER(TRIM(seller_state))   AS seller_state
FROM raw.sellers;

ALTER TABLE staging.stg_sellers
    ADD PRIMARY KEY (seller_id);


-- ============================================================
-- stg_geolocation
-- Deduplicated: one row per zip code (average of coordinates)
-- The raw table has ~1M rows with many duplicates per zip code
-- ============================================================
CREATE TABLE staging.stg_geolocation AS
SELECT
    geolocation_zip_code_prefix     AS zip_code_prefix,
    ROUND(AVG(geolocation_lat::NUMERIC), 6)  AS latitude,
    ROUND(AVG(geolocation_lng::NUMERIC), 6)  AS longitude,
    MODE() WITHIN GROUP (ORDER BY INITCAP(TRIM(geolocation_city)))  AS city,
    MODE() WITHIN GROUP (ORDER BY UPPER(TRIM(geolocation_state)))   AS state
FROM raw.geolocation
GROUP BY geolocation_zip_code_prefix;

ALTER TABLE staging.stg_geolocation
    ADD PRIMARY KEY (zip_code_prefix);