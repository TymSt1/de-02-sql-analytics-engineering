-- ============================================================
-- INDEXES & QUERY PLAN ANALYSIS
-- Purpose: Demonstrate indexing strategy and performance tuning
-- ============================================================

-- ============================================================
-- 1. Add indexes to staging tables for common query patterns
-- ============================================================

-- Orders: frequently filtered by status and date
CREATE INDEX idx_orders_status ON staging.stg_orders (order_status);
CREATE INDEX idx_orders_purchase_date ON staging.stg_orders (order_purchase_timestamp);
CREATE INDEX idx_orders_customer ON staging.stg_orders (customer_id);

-- Order items: frequently joined on order_id, seller_id, product_id
CREATE INDEX idx_items_product ON staging.stg_order_items (product_id);
CREATE INDEX idx_items_seller ON staging.stg_order_items (seller_id);

-- Payments: filtered by type
CREATE INDEX idx_payments_type ON staging.stg_order_payments (payment_type);

-- Reviews: filtered by score
CREATE INDEX idx_reviews_score ON staging.stg_order_reviews (review_score);

-- Intermediate: orders enriched is queried heavily
CREATE INDEX idx_enriched_date ON intermediate.int_orders_enriched (order_date);
CREATE INDEX idx_enriched_state ON intermediate.int_orders_enriched (customer_state);
CREATE INDEX idx_enriched_status ON intermediate.int_orders_enriched (delivery_status);

-- Composite index for common filter combo
CREATE INDEX idx_enriched_state_date
    ON intermediate.int_orders_enriched (customer_state, order_date);


-- ============================================================
-- 2. EXPLAIN ANALYZE examples
-- Run these to see how PostgreSQL uses (or ignores) indexes
-- ============================================================

-- Example A: Sequential scan vs index scan
-- This should use idx_orders_status
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM staging.stg_orders
WHERE order_status = 'shipped';

-- Example B: Index scan on timestamp range
EXPLAIN ANALYZE
SELECT order_id, customer_id, order_purchase_timestamp
FROM staging.stg_orders
WHERE order_purchase_timestamp BETWEEN '2018-01-01' AND '2018-03-31';

-- Example C: Join performance with indexes
EXPLAIN ANALYZE
SELECT
    o.order_id,
    o.order_status,
    oi.price,
    p.product_category
FROM staging.stg_orders o
JOIN staging.stg_order_items oi ON o.order_id = oi.order_id
JOIN staging.stg_products p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'
    AND o.order_purchase_timestamp >= '2018-06-01'
LIMIT 100;

-- Example D: Aggregation with filter — uses composite index
EXPLAIN ANALYZE
SELECT
    customer_state,
    COUNT(*) AS orders,
    ROUND(AVG(total_order_value)::NUMERIC, 2) AS avg_value
FROM intermediate.int_orders_enriched
WHERE order_date BETWEEN '2018-01-01' AND '2018-06-30'
    AND delivery_status = 'on_time'
GROUP BY customer_state
ORDER BY orders DESC;

-- Example E: Without index — force sequential scan to compare
SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;

EXPLAIN ANALYZE
SELECT COUNT(*)
FROM staging.stg_orders
WHERE order_status = 'shipped';

-- Re-enable indexes
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;