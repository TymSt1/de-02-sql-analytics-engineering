-- ============================================================
-- INTERMEDIATE LAYER
-- Purpose: Business logic, joins, enrichment, SCD Type 2
-- Source: staging schema
-- Convention: int_<entity>
-- ============================================================

DROP SCHEMA IF EXISTS intermediate CASCADE;
CREATE SCHEMA intermediate;

-- ============================================================
-- int_orders_enriched
-- The core fact table: one row per order with all key metrics
-- Joins: orders + customers + payments + reviews
-- ============================================================
CREATE TABLE intermediate.int_orders_enriched AS
SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    o.order_status,

    -- Timestamps
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Date parts (useful for aggregations)
    DATE(o.order_purchase_timestamp)                     AS order_date,
    EXTRACT(YEAR FROM o.order_purchase_timestamp)::INT   AS order_year,
    EXTRACT(MONTH FROM o.order_purchase_timestamp)::INT  AS order_month,
    TO_CHAR(o.order_purchase_timestamp, 'Day')           AS order_day_of_week,

    -- Order financials (aggregated from items)
    oi.total_items,
    oi.total_products,
    oi.total_price,
    oi.total_freight,
    oi.total_price + oi.total_freight                    AS total_order_value,

    -- Payment info
    pay.total_payment_value,
    pay.payment_methods,
    pay.max_installments,

    -- Review
    rev.review_score,

    -- Delivery performance
    EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp)) / 86400.0
        AS delivery_days,
    EXTRACT(EPOCH FROM (o.order_estimated_delivery_date - o.order_delivered_customer_date)) / 86400.0
        AS delivery_vs_estimate_days,
    CASE
        WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 'on_time'
        WHEN o.order_delivered_customer_date >  o.order_estimated_delivery_date THEN 'late'
        ELSE 'not_delivered'
    END AS delivery_status

FROM staging.stg_orders o

-- Join customer info
LEFT JOIN staging.stg_customers c
    ON o.customer_id = c.customer_id

-- Aggregate order items per order
LEFT JOIN (
    SELECT
        order_id,
        COUNT(*)                        AS total_items,
        COUNT(DISTINCT product_id)      AS total_products,
        SUM(price)                      AS total_price,
        SUM(freight_value)              AS total_freight
    FROM staging.stg_order_items
    GROUP BY order_id
) oi ON o.order_id = oi.order_id

-- Aggregate payments per order
LEFT JOIN (
    SELECT
        order_id,
        SUM(payment_value)                                  AS total_payment_value,
        STRING_AGG(DISTINCT payment_type, ', ' ORDER BY payment_type) AS payment_methods,
        MAX(payment_installments)                           AS max_installments
    FROM staging.stg_order_payments
    GROUP BY order_id
) pay ON o.order_id = pay.order_id

-- Review score
LEFT JOIN staging.stg_order_reviews rev
    ON o.order_id = rev.order_id;

ALTER TABLE intermediate.int_orders_enriched
    ADD PRIMARY KEY (order_id);


-- ============================================================
-- int_seller_performance
-- One row per seller with aggregated performance metrics
-- ============================================================
CREATE TABLE intermediate.int_seller_performance AS
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,

    COUNT(DISTINCT oi.order_id)         AS total_orders,
    COUNT(*)                            AS total_items_sold,
    COUNT(DISTINCT oi.product_id)       AS unique_products_sold,
    SUM(oi.price)                       AS total_revenue,
    AVG(oi.price)                       AS avg_item_price,
    SUM(oi.freight_value)               AS total_freight_collected,

    -- Review performance
    ROUND(AVG(rev.review_score), 2)     AS avg_review_score,

    -- Delivery performance
    ROUND(AVG(
        EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp)) / 86400.0
    )::NUMERIC, 1)                      AS avg_delivery_days,

    -- Activity window
    MIN(o.order_purchase_timestamp)     AS first_order_date,
    MAX(o.order_purchase_timestamp)     AS last_order_date,

    -- Seller tenure in days
    EXTRACT(EPOCH FROM (
        MAX(o.order_purchase_timestamp) - MIN(o.order_purchase_timestamp)
    )) / 86400.0                        AS active_days

FROM staging.stg_sellers s
LEFT JOIN staging.stg_order_items oi ON s.seller_id = oi.seller_id
LEFT JOIN staging.stg_orders o       ON oi.order_id = o.order_id
LEFT JOIN staging.stg_order_reviews rev ON o.order_id = rev.order_id
GROUP BY s.seller_id, s.seller_city, s.seller_state;

ALTER TABLE intermediate.int_seller_performance
    ADD PRIMARY KEY (seller_id);


-- ============================================================
-- int_product_performance
-- One row per product with sales and review metrics
-- ============================================================
CREATE TABLE intermediate.int_product_performance AS
SELECT
    p.product_id,
    p.product_category,
    p.product_weight_g,

    COUNT(DISTINCT oi.order_id)         AS times_ordered,
    SUM(oi.price)                       AS total_revenue,
    AVG(oi.price)                       AS avg_selling_price,
    SUM(oi.freight_value)               AS total_freight,
    ROUND(AVG(rev.review_score), 2)     AS avg_review_score,

    MIN(o.order_purchase_timestamp)     AS first_sold_date,
    MAX(o.order_purchase_timestamp)     AS last_sold_date

FROM staging.stg_products p
LEFT JOIN staging.stg_order_items oi    ON p.product_id = oi.product_id
LEFT JOIN staging.stg_orders o          ON oi.order_id = o.order_id
LEFT JOIN staging.stg_order_reviews rev ON o.order_id = rev.order_id
GROUP BY p.product_id, p.product_category, p.product_weight_g;

ALTER TABLE intermediate.int_product_performance
    ADD PRIMARY KEY (product_id);


-- ============================================================
-- int_customer_history
-- One row per unique customer with lifetime metrics
-- Uses customer_unique_id (a customer can have multiple customer_ids)
-- ============================================================
CREATE TABLE intermediate.int_customer_history AS
SELECT
    c.customer_unique_id,

    COUNT(DISTINCT o.order_id)                          AS lifetime_orders,
    SUM(oe.total_order_value)                           AS lifetime_value,
    AVG(oe.total_order_value)                           AS avg_order_value,
    ROUND(AVG(oe.review_score), 2)                      AS avg_review_score,
    MIN(oe.order_purchase_timestamp)                    AS first_order_date,
    MAX(oe.order_purchase_timestamp)                    AS last_order_date,

    -- Is this a repeat customer?
    CASE
        WHEN COUNT(DISTINCT o.order_id) > 1 THEN TRUE
        ELSE FALSE
    END AS is_repeat_customer,

    -- Preferred payment method (most used)
    MODE() WITHIN GROUP (ORDER BY pay.payment_type)     AS preferred_payment_method,

    -- Most common state
    MODE() WITHIN GROUP (ORDER BY c.customer_state)     AS primary_state

FROM staging.stg_customers c
JOIN staging.stg_orders o ON c.customer_id = o.customer_id
JOIN intermediate.int_orders_enriched oe ON o.order_id = oe.order_id
LEFT JOIN staging.stg_order_payments pay ON o.order_id = pay.order_id
GROUP BY c.customer_unique_id;

ALTER TABLE intermediate.int_customer_history
    ADD PRIMARY KEY (customer_unique_id);


-- ============================================================
-- SCD TYPE 2: int_seller_status_history
--
-- Simulates Slowly Changing Dimension Type 2 on seller status.
-- Since the Olist dataset is static, we derive status changes
-- from monthly activity: a seller is 'active' in months they
-- have orders, 'inactive' otherwise. We track transitions.
-- ============================================================
CREATE TABLE intermediate.int_seller_status_history AS
WITH monthly_activity AS (
    -- For each seller, flag each month as active/inactive
    SELECT
        s.seller_id,
        m.month_start,
        CASE
            WHEN COUNT(o.order_id) > 0 THEN 'active'
            ELSE 'inactive'
        END AS seller_status,
        COUNT(o.order_id) AS orders_in_month
    FROM staging.stg_sellers s
    CROSS JOIN (
        SELECT DISTINCT DATE_TRUNC('month', order_purchase_timestamp)::DATE AS month_start
        FROM staging.stg_orders
    ) m
    LEFT JOIN staging.stg_order_items oi ON s.seller_id = oi.seller_id
    LEFT JOIN staging.stg_orders o
        ON oi.order_id = o.order_id
        AND DATE_TRUNC('month', o.order_purchase_timestamp)::DATE = m.month_start
    GROUP BY s.seller_id, m.month_start
),
status_changes AS (
    -- Detect when status changes using LAG
    SELECT
        seller_id,
        month_start,
        seller_status,
        orders_in_month,
        LAG(seller_status) OVER (PARTITION BY seller_id ORDER BY month_start) AS prev_status
    FROM monthly_activity
),
change_points AS (
    -- Keep only rows where status changed (or first row)
    SELECT
        seller_id,
        month_start     AS valid_from,
        seller_status,
        orders_in_month
    FROM status_changes
    WHERE prev_status IS NULL OR seller_status != prev_status
)
SELECT
    seller_id,
    seller_status,
    valid_from,
    -- valid_to = next change date - 1 day, or '9999-12-31' if current
    COALESCE(
        LEAD(valid_from) OVER (PARTITION BY seller_id ORDER BY valid_from) - INTERVAL '1 day',
        '9999-12-31'::DATE
    )::DATE AS valid_to,
    -- is_current flag
    CASE
        WHEN LEAD(valid_from) OVER (PARTITION BY seller_id ORDER BY valid_from) IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current,
    orders_in_month AS orders_at_change
FROM change_points
ORDER BY seller_id, valid_from;

-- Composite key for SCD
ALTER TABLE intermediate.int_seller_status_history
    ADD COLUMN scd_id SERIAL PRIMARY KEY;
CREATE INDEX idx_seller_scd_lookup
    ON intermediate.int_seller_status_history (seller_id, is_current);