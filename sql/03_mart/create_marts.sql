-- ============================================================
-- MART LAYER
-- Purpose: Business-ready analytical models
-- Materialized views for fast queries, refreshable on demand
-- Source: intermediate schema
-- Convention: mart_<business_area>
-- ============================================================

DROP SCHEMA IF EXISTS mart CASCADE;
CREATE SCHEMA mart;

-- ============================================================
-- mart_monthly_revenue
-- Monthly revenue dashboard: GMV, orders, AOV, trends
-- Business question: "How is the business performing over time?"
-- ============================================================
CREATE MATERIALIZED VIEW mart.mart_monthly_revenue AS
SELECT
    order_year,
    order_month,
    DATE(DATE_TRUNC('month', order_purchase_timestamp))  AS month_start,
    COUNT(*)                                              AS total_orders,
    COUNT(DISTINCT customer_unique_id)                    AS unique_customers,
    ROUND(SUM(total_order_value)::NUMERIC, 2)             AS gmv,
    ROUND(AVG(total_order_value)::NUMERIC, 2)             AS avg_order_value,
    ROUND(SUM(total_freight)::NUMERIC, 2)                 AS total_freight_revenue,
    ROUND(AVG(review_score)::NUMERIC, 2)                  AS avg_review_score,
    ROUND(AVG(delivery_days)::NUMERIC, 1)                 AS avg_delivery_days,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE delivery_status = 'late') / NULLIF(COUNT(*), 0)
    , 1)                                                  AS late_delivery_pct
FROM intermediate.int_orders_enriched
WHERE order_status = 'delivered'
GROUP BY order_year, order_month, DATE_TRUNC('month', order_purchase_timestamp)
ORDER BY month_start;

CREATE UNIQUE INDEX idx_mart_monthly_rev
    ON mart.mart_monthly_revenue (month_start);


-- ============================================================
-- mart_state_performance
-- Geographic performance breakdown by Brazilian state
-- Business question: "Which states drive revenue and where
--   are quality issues?"
-- ============================================================
CREATE MATERIALIZED VIEW mart.mart_state_performance AS
SELECT
    customer_state,
    COUNT(*)                                              AS total_orders,
    COUNT(DISTINCT customer_unique_id)                    AS unique_customers,
    ROUND(SUM(total_order_value)::NUMERIC, 2)             AS total_revenue,
    ROUND(AVG(total_order_value)::NUMERIC, 2)             AS avg_order_value,
    ROUND(AVG(review_score)::NUMERIC, 2)                  AS avg_review_score,
    ROUND(AVG(delivery_days)::NUMERIC, 1)                 AS avg_delivery_days,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE delivery_status = 'late') / NULLIF(COUNT(*), 0)
    , 1)                                                  AS late_delivery_pct,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE review_score <= 2) / NULLIF(COUNT(*), 0)
    , 1)                                                  AS low_review_pct
FROM intermediate.int_orders_enriched
WHERE order_status = 'delivered'
GROUP BY customer_state
ORDER BY total_revenue DESC;

CREATE UNIQUE INDEX idx_mart_state_perf
    ON mart.mart_state_performance (customer_state);


-- ============================================================
-- mart_product_category_analysis
-- Category-level performance
-- Business question: "Which categories should we invest in
--   or phase out?"
-- ============================================================
CREATE MATERIALIZED VIEW mart.mart_product_category_analysis AS
SELECT
    p.product_category,
    COUNT(DISTINCT oi.order_id)                           AS total_orders,
    SUM(oi.price)::NUMERIC(12,2)                          AS total_revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2)                      AS avg_price,
    COUNT(DISTINCT oi.seller_id)                          AS seller_count,
    COUNT(DISTINCT oi.product_id)                         AS product_count,
    ROUND(AVG(rev.review_score)::NUMERIC, 2)              AS avg_review_score,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp)) / 86400.0
    )::NUMERIC, 1)                                        AS avg_delivery_days
FROM staging.stg_order_items oi
JOIN staging.stg_products p      ON oi.product_id = p.product_id
JOIN staging.stg_orders o        ON oi.order_id = o.order_id
LEFT JOIN staging.stg_order_reviews rev ON o.order_id = rev.order_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_category
HAVING COUNT(DISTINCT oi.order_id) >= 10
ORDER BY total_revenue DESC;

CREATE UNIQUE INDEX idx_mart_category
    ON mart.mart_product_category_analysis (product_category);


-- ============================================================
-- mart_seller_scorecard
-- Seller ranking and scoring for marketplace management
-- Business question: "Who are our best and worst sellers?"
-- ============================================================
CREATE MATERIALIZED VIEW mart.mart_seller_scorecard AS
SELECT
    seller_id,
    seller_city,
    seller_state,
    total_orders,
    total_revenue,
    avg_review_score,
    avg_delivery_days,

    -- Percentile ranks
    ROUND(PERCENT_RANK() OVER (ORDER BY total_revenue)::NUMERIC, 3)
        AS revenue_percentile,
    ROUND(PERCENT_RANK() OVER (ORDER BY avg_review_score)::NUMERIC, 3)
        AS review_percentile,
    ROUND(PERCENT_RANK() OVER (ORDER BY avg_delivery_days DESC)::NUMERIC, 3)
        AS delivery_speed_percentile,

    -- Tier classification
    CASE
        WHEN total_orders >= 50 AND avg_review_score >= 4.0 THEN 'gold'
        WHEN total_orders >= 20 AND avg_review_score >= 3.5 THEN 'silver'
        WHEN total_orders >= 5                              THEN 'bronze'
        ELSE 'new'
    END AS seller_tier

FROM intermediate.int_seller_performance
WHERE total_orders > 0
ORDER BY total_revenue DESC;


-- ============================================================
-- mart_customer_segments
-- RFM-style customer segmentation
-- Business question: "Who are our most valuable customers
--   and who is at risk of churning?"
-- ============================================================
CREATE MATERIALIZED VIEW mart.mart_customer_segments AS
WITH rfm AS (
    SELECT
        customer_unique_id,
        lifetime_orders,
        lifetime_value,
        avg_order_value,
        avg_review_score,
        first_order_date,
        last_order_date,
        is_repeat_customer,
        preferred_payment_method,
        primary_state,

        -- Recency: days since last order (relative to max date in dataset)
        EXTRACT(EPOCH FROM (
            (SELECT MAX(order_purchase_timestamp) FROM staging.stg_orders)
            - last_order_date
        )) / 86400.0 AS recency_days,

        -- Frequency quintile
        NTILE(5) OVER (ORDER BY lifetime_orders)      AS frequency_quintile,

        -- Monetary quintile
        NTILE(5) OVER (ORDER BY lifetime_value)        AS monetary_quintile
    FROM intermediate.int_customer_history
    WHERE lifetime_orders > 0
),
rfm_scored AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY recency_days DESC)    AS recency_quintile
    FROM rfm
)
SELECT
    customer_unique_id,
    lifetime_orders,
    ROUND(lifetime_value::NUMERIC, 2)                   AS lifetime_value,
    ROUND(avg_order_value::NUMERIC, 2)                  AS avg_order_value,
    avg_review_score,
    first_order_date,
    last_order_date,
    ROUND(recency_days::NUMERIC, 0)                     AS recency_days,
    is_repeat_customer,
    preferred_payment_method,
    primary_state,
    recency_quintile,
    frequency_quintile,
    monetary_quintile,

    -- Segment assignment
    CASE
        WHEN recency_quintile >= 4 AND frequency_quintile >= 4 AND monetary_quintile >= 4
            THEN 'champions'
        WHEN recency_quintile >= 4 AND frequency_quintile >= 3
            THEN 'loyal'
        WHEN recency_quintile >= 4 AND monetary_quintile >= 4
            THEN 'big_spenders'
        WHEN recency_quintile >= 4
            THEN 'recent'
        WHEN frequency_quintile >= 4
            THEN 'frequent'
        WHEN recency_quintile <= 2 AND frequency_quintile <= 2
            THEN 'at_risk'
        ELSE 'regular'
    END AS customer_segment

FROM rfm_scored;

CREATE UNIQUE INDEX idx_mart_cust_seg
    ON mart.mart_customer_segments (customer_unique_id);


-- ============================================================
-- Refresh command (run when upstream data changes)
-- ============================================================
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mart_monthly_revenue;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mart_state_performance;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mart_product_category_analysis;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mart_seller_scorecard;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mart_customer_segments;