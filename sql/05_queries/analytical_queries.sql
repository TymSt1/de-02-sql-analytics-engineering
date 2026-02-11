-- ============================================================
-- ANALYTICAL QUERIES
-- 15-20 complex SQL Queries
-- Each query documents the business question it answers
-- ============================================================


-- ============================================================
-- Q1: Monthly revenue with month-over-month growth rate
-- Business question: "What's our revenue trend and growth rate?"
-- Skills: Window function (LAG), CTE, date math
-- ============================================================
WITH monthly AS (
    SELECT
        month_start,
        gmv,
        total_orders,
        LAG(gmv) OVER (ORDER BY month_start) AS prev_month_gmv
    FROM mart.mart_monthly_revenue
)
SELECT
    month_start,
    gmv,
    total_orders,
    prev_month_gmv,
    CASE
        WHEN prev_month_gmv > 0
        THEN ROUND(100.0 * (gmv - prev_month_gmv) / prev_month_gmv, 1)
        ELSE NULL
    END AS mom_growth_pct
FROM monthly
ORDER BY month_start;


-- ============================================================
-- Q2: Running total of revenue (cumulative GMV)
-- Business question: "What's our cumulative revenue over time?"
-- Skills: Window function (SUM OVER), running aggregation
-- ============================================================
SELECT
    month_start,
    gmv,
    SUM(gmv) OVER (ORDER BY month_start) AS cumulative_gmv,
    SUM(total_orders) OVER (ORDER BY month_start) AS cumulative_orders
FROM mart.mart_monthly_revenue
ORDER BY month_start;


-- ============================================================
-- Q3: Top 10 product categories by revenue with rank
-- Business question: "What are our best-selling categories?"
-- Skills: RANK, window function
-- ============================================================
SELECT
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    product_category,
    total_revenue,
    total_orders,
    avg_price,
    avg_review_score
FROM mart.mart_product_category_analysis
LIMIT 10;


-- ============================================================
-- Q4: Revenue contribution by state with cumulative percentage
-- Business question: "Which states make up 80% of our revenue?"
-- Skills: Window functions (SUM OVER, cumulative), Pareto analysis
-- ============================================================
WITH state_rev AS (
    SELECT
        customer_state,
        total_revenue,
        SUM(total_revenue) OVER () AS grand_total
    FROM mart.mart_state_performance
)
SELECT
    customer_state,
    total_revenue,
    ROUND(100.0 * total_revenue / grand_total, 1) AS pct_of_total,
    ROUND(100.0 * SUM(total_revenue) OVER (ORDER BY total_revenue DESC) / grand_total, 1)
        AS cumulative_pct
FROM state_rev
ORDER BY total_revenue DESC;


-- ============================================================
-- Q5: Seller ranking within each state
-- Business question: "Who are the top sellers in each state?"
-- Skills: ROW_NUMBER, PARTITION BY
-- ============================================================
SELECT *
FROM (
    SELECT
        seller_state,
        seller_id,
        seller_city,
        total_revenue,
        total_orders,
        avg_review_score,
        ROW_NUMBER() OVER (
            PARTITION BY seller_state
            ORDER BY total_revenue DESC
        ) AS rank_in_state
    FROM mart.mart_seller_scorecard
) ranked
WHERE rank_in_state <= 3
ORDER BY seller_state, rank_in_state;


-- ============================================================
-- Q6: Orders with previous and next order date per customer
-- Business question: "What's the typical gap between repeat orders?"
-- Skills: LAG, LEAD, PARTITION BY
-- ============================================================
SELECT
    customer_unique_id,
    order_id,
    order_purchase_timestamp,
    LAG(order_purchase_timestamp) OVER (
        PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp
    ) AS previous_order_date,
    LEAD(order_purchase_timestamp) OVER (
        PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp
    ) AS next_order_date,
    EXTRACT(EPOCH FROM (
        order_purchase_timestamp - LAG(order_purchase_timestamp) OVER (
            PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp
        )
    )) / 86400.0 AS days_since_last_order
FROM intermediate.int_orders_enriched
WHERE customer_unique_id IN (
    SELECT customer_unique_id
    FROM intermediate.int_customer_history
    WHERE is_repeat_customer = TRUE
)
ORDER BY customer_unique_id, order_purchase_timestamp
LIMIT 30;


-- ============================================================
-- Q7: Payment method trends over time
-- Business question: "How are payment preferences changing?"
-- Skills: CASE pivot, date aggregation, percentage calculation
-- ============================================================
SELECT
    DATE_TRUNC('quarter', o.order_purchase_timestamp)::DATE AS quarter,
    COUNT(*) AS total_payments,
    ROUND(100.0 * COUNT(*) FILTER (WHERE p.payment_type = 'credit_card') / COUNT(*), 1)
        AS credit_card_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE p.payment_type = 'boleto') / COUNT(*), 1)
        AS boleto_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE p.payment_type = 'voucher') / COUNT(*), 1)
        AS voucher_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE p.payment_type = 'debit_card') / COUNT(*), 1)
        AS debit_card_pct
FROM staging.stg_order_payments p
JOIN staging.stg_orders o ON p.order_id = o.order_id
GROUP BY quarter
ORDER BY quarter;


-- ============================================================
-- Q8: Delivery performance by day of week
-- Business question: "Which days have the worst delivery times?"
-- Skills: EXTRACT, CASE for day names, GROUP BY expression
-- ============================================================
SELECT
    CASE EXTRACT(DOW FROM order_purchase_timestamp)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_of_week,
    EXTRACT(DOW FROM order_purchase_timestamp) AS dow_num,
    COUNT(*) AS total_orders,
    ROUND(AVG(delivery_days)::NUMERIC, 1) AS avg_delivery_days,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE delivery_status = 'late') / COUNT(*)
    , 1) AS late_pct
FROM intermediate.int_orders_enriched
WHERE order_status = 'delivered'
GROUP BY dow_num, day_of_week
ORDER BY dow_num;


-- ============================================================
-- Q9: Cohort analysis — retention by first-purchase month
-- Business question: "Do customers come back after first purchase?"
-- Skills: Self-join, date math, cohort analysis pattern
-- ============================================================
WITH first_purchase AS (
    SELECT
        customer_unique_id,
        DATE_TRUNC('month', MIN(order_purchase_timestamp))::DATE AS cohort_month
    FROM intermediate.int_orders_enriched
    GROUP BY customer_unique_id
),
subsequent AS (
    SELECT
        fp.customer_unique_id,
        fp.cohort_month,
        DATE_TRUNC('month', oe.order_purchase_timestamp)::DATE AS order_month,
        (EXTRACT(YEAR FROM oe.order_purchase_timestamp) - EXTRACT(YEAR FROM fp.cohort_month)) * 12
        + EXTRACT(MONTH FROM oe.order_purchase_timestamp) - EXTRACT(MONTH FROM fp.cohort_month)
            AS months_since_first
    FROM first_purchase fp
    JOIN intermediate.int_orders_enriched oe
        ON fp.customer_unique_id = oe.customer_unique_id
)
SELECT
    cohort_month,
    COUNT(DISTINCT customer_unique_id) FILTER (WHERE months_since_first = 0) AS month_0,
    COUNT(DISTINCT customer_unique_id) FILTER (WHERE months_since_first = 1) AS month_1,
    COUNT(DISTINCT customer_unique_id) FILTER (WHERE months_since_first = 2) AS month_2,
    COUNT(DISTINCT customer_unique_id) FILTER (WHERE months_since_first = 3) AS month_3,
    COUNT(DISTINCT customer_unique_id) FILTER (WHERE months_since_first = 6) AS month_6,
    COUNT(DISTINCT customer_unique_id) FILTER (WHERE months_since_first = 12) AS month_12
FROM subsequent
GROUP BY cohort_month
ORDER BY cohort_month;


-- ============================================================
-- Q10: Revenue by category with ROLLUP (subtotals + grand total)
-- Business question: "Category revenue with state subtotals?"
-- Skills: GROUP BY ROLLUP, GROUPING function
-- ============================================================
SELECT
    COALESCE(oe.customer_state, '** ALL STATES **')    AS state,
    COALESCE(p.product_category, '** ALL CATEGORIES **') AS category,
    COUNT(DISTINCT oi.order_id)                         AS orders,
    ROUND(SUM(oi.price)::NUMERIC, 2)                    AS revenue,
    GROUPING(oe.customer_state)                         AS is_state_total,
    GROUPING(p.product_category)                        AS is_category_total
FROM staging.stg_order_items oi
JOIN staging.stg_products p ON oi.product_id = p.product_id
JOIN intermediate.int_orders_enriched oe ON oi.order_id = oe.order_id
WHERE oe.customer_state IN ('SP', 'RJ', 'MG')
    AND p.product_category IN ('bed_bath_table', 'health_beauty', 'sports_leisure')
GROUP BY ROLLUP (oe.customer_state, p.product_category)
ORDER BY is_state_total, state, is_category_total, revenue DESC;


-- ============================================================
-- Q11: Revenue by state and category with CUBE
-- Business question: "All possible subtotal combinations?"
-- Skills: GROUP BY CUBE
-- ============================================================
SELECT
    COALESCE(oe.customer_state, '** ALL **')         AS state,
    COALESCE(p.product_category, '** ALL **')        AS category,
    COUNT(DISTINCT oi.order_id)                      AS orders,
    ROUND(SUM(oi.price)::NUMERIC, 2)                 AS revenue
FROM staging.stg_order_items oi
JOIN staging.stg_products p ON oi.product_id = p.product_id
JOIN intermediate.int_orders_enriched oe ON oi.order_id = oe.order_id
WHERE oe.customer_state IN ('SP', 'RJ')
    AND p.product_category IN ('bed_bath_table', 'health_beauty')
GROUP BY CUBE (oe.customer_state, p.product_category)
ORDER BY state, category;


-- ============================================================
-- Q12: Sellers who outperform their state average
-- Business question: "Which sellers beat their local competition?"
-- Skills: Subquery, self-join, HAVING, comparison
-- ============================================================
SELECT
    sc.seller_id,
    sc.seller_state,
    sc.seller_city,
    sc.total_orders,
    ROUND(sc.total_revenue::NUMERIC, 2)     AS seller_revenue,
    ROUND(state_avg.avg_revenue::NUMERIC, 2) AS state_avg_revenue,
    ROUND(sc.total_revenue / NULLIF(state_avg.avg_revenue, 0), 1)
        AS times_above_avg
FROM mart.mart_seller_scorecard sc
JOIN (
    SELECT
        seller_state,
        AVG(total_revenue) AS avg_revenue
    FROM mart.mart_seller_scorecard
    GROUP BY seller_state
) state_avg ON sc.seller_state = state_avg.seller_state
WHERE sc.total_revenue > state_avg.avg_revenue * 2
ORDER BY times_above_avg DESC
LIMIT 15;


-- ============================================================
-- Q13: Moving average of daily orders (7-day window)
-- Business question: "What's the smoothed daily order trend?"
-- Skills: Window function with ROWS BETWEEN (moving average)
-- ============================================================
WITH daily_orders AS (
    SELECT
        order_date,
        COUNT(*) AS orders,
        ROUND(SUM(total_order_value)::NUMERIC, 2) AS daily_revenue
    FROM intermediate.int_orders_enriched
    WHERE order_status = 'delivered'
    GROUP BY order_date
)
SELECT
    order_date,
    orders,
    daily_revenue,
    ROUND(AVG(orders) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 1) AS orders_7day_avg,
    ROUND(AVG(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )::NUMERIC, 2) AS revenue_7day_avg
FROM daily_orders
ORDER BY order_date;


-- ============================================================
-- Q14: Recursive CTE — generate a calendar and fill gaps
-- Business question: "Show revenue for every single day,
--   including days with zero orders"
-- Skills: Recursive CTE, LEFT JOIN, COALESCE
-- ============================================================
WITH RECURSIVE calendar AS (
    -- Anchor: first order date
    SELECT MIN(order_date)::DATE AS cal_date
    FROM intermediate.int_orders_enriched

    UNION ALL

    -- Recursive: add one day at a time
    SELECT (cal_date + INTERVAL '1 day')::DATE
    FROM calendar
    WHERE cal_date < (SELECT MAX(order_date) FROM intermediate.int_orders_enriched)
),
daily_actual AS (
    SELECT
        order_date,
        COUNT(*) AS orders,
        ROUND(SUM(total_order_value)::NUMERIC, 2) AS revenue
    FROM intermediate.int_orders_enriched
    WHERE order_status = 'delivered'
    GROUP BY order_date
)
SELECT
    c.cal_date,
    COALESCE(d.orders, 0)   AS orders,
    COALESCE(d.revenue, 0)  AS revenue,
    EXTRACT(DOW FROM c.cal_date) AS day_of_week
FROM calendar c
LEFT JOIN daily_actual d ON c.cal_date = d.order_date
ORDER BY c.cal_date;


-- ============================================================
-- Q15: Review sentiment by delivery performance
-- Business question: "How much does late delivery hurt reviews?"
-- Skills: CASE, conditional aggregation, HAVING
-- ============================================================
SELECT
    delivery_status,
    COUNT(*) AS total_orders,
    ROUND(AVG(review_score)::NUMERIC, 2) AS avg_review,
    COUNT(*) FILTER (WHERE review_score = 5) AS five_star,
    COUNT(*) FILTER (WHERE review_score = 1) AS one_star,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE review_score <= 2) / COUNT(*)
    , 1) AS low_review_pct,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE review_score >= 4) / COUNT(*)
    , 1) AS high_review_pct
FROM intermediate.int_orders_enriched
WHERE order_status = 'delivered'
    AND review_score IS NOT NULL
GROUP BY delivery_status
HAVING COUNT(*) > 100
ORDER BY avg_review DESC;


-- ============================================================
-- Q16: Seller pairs who share the most customers
-- Business question: "Which sellers compete for the same buyers?"
-- Skills: Self-join, DISTINCT, pair analysis
-- ============================================================
WITH seller_customers AS (
    SELECT DISTINCT
        oi.seller_id,
        oe.customer_unique_id
    FROM staging.stg_order_items oi
    JOIN intermediate.int_orders_enriched oe ON oi.order_id = oe.order_id
)
SELECT
    a.seller_id AS seller_a,
    b.seller_id AS seller_b,
    COUNT(DISTINCT a.customer_unique_id) AS shared_customers
FROM seller_customers a
JOIN seller_customers b
    ON a.customer_unique_id = b.customer_unique_id
    AND a.seller_id < b.seller_id
GROUP BY a.seller_id, b.seller_id
HAVING COUNT(DISTINCT a.customer_unique_id) >= 5
ORDER BY shared_customers DESC
LIMIT 15;


-- ============================================================
-- Q17: Month-over-month customer acquisition vs returning
-- Business question: "Are we growing through new or returning
--   customers?"
-- Skills: CTE, conditional aggregation, LAG
-- ============================================================
WITH customer_first_month AS (
    SELECT
        customer_unique_id,
        DATE_TRUNC('month', MIN(order_purchase_timestamp))::DATE AS first_month
    FROM intermediate.int_orders_enriched
    GROUP BY customer_unique_id
),
monthly_breakdown AS (
    SELECT
        DATE_TRUNC('month', oe.order_purchase_timestamp)::DATE AS month,
        COUNT(DISTINCT oe.customer_unique_id) AS total_customers,
        COUNT(DISTINCT oe.customer_unique_id) FILTER (
            WHERE cfm.first_month = DATE_TRUNC('month', oe.order_purchase_timestamp)::DATE
        ) AS new_customers,
        COUNT(DISTINCT oe.customer_unique_id) FILTER (
            WHERE cfm.first_month < DATE_TRUNC('month', oe.order_purchase_timestamp)::DATE
        ) AS returning_customers
    FROM intermediate.int_orders_enriched oe
    JOIN customer_first_month cfm ON oe.customer_unique_id = cfm.customer_unique_id
    GROUP BY month
)
SELECT
    month,
    total_customers,
    new_customers,
    returning_customers,
    ROUND(100.0 * new_customers / total_customers, 1) AS new_pct,
    ROUND(100.0 * returning_customers / total_customers, 1) AS returning_pct
FROM monthly_breakdown
ORDER BY month;


-- ============================================================
-- Q18: Heavy freight analysis — products where freight > 50%
--   of product price
-- Business question: "Which products have disproportionate
--   shipping costs?"
-- Skills: HAVING, ratio analysis, JOIN
-- ============================================================
SELECT
    p.product_category,
    COUNT(*) AS items_sold,
    ROUND(AVG(oi.price)::NUMERIC, 2) AS avg_price,
    ROUND(AVG(oi.freight_value)::NUMERIC, 2) AS avg_freight,
    ROUND(AVG(oi.freight_value / NULLIF(oi.price, 0))::NUMERIC * 100, 1)
        AS freight_pct_of_price,
    ROUND(AVG(p.product_weight_g)::NUMERIC, 0) AS avg_weight_g
FROM staging.stg_order_items oi
JOIN staging.stg_products p ON oi.product_id = p.product_id
GROUP BY p.product_category
HAVING COUNT(*) >= 20
    AND AVG(oi.freight_value / NULLIF(oi.price, 0)) > 0.5
ORDER BY freight_pct_of_price DESC;