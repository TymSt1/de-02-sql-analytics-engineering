#!/bin/bash
set -e

echo "=== Creating raw schema ==="
psql -U dataeng -d ecommerce < /sql/00_schema_raw/create_tables.sql

echo "=== Loading CSV files into raw tables ==="
psql -U dataeng -d ecommerce -c "\COPY raw.customers FROM '/data/raw/olist_customers_dataset.csv' WITH CSV HEADER"
psql -U dataeng -d ecommerce -c "\COPY raw.orders FROM '/data/raw/olist_orders_dataset.csv' WITH CSV HEADER"
psql -U dataeng -d ecommerce -c "\COPY raw.order_items FROM '/data/raw/olist_order_items_dataset.csv' WITH CSV HEADER"
psql -U dataeng -d ecommerce -c "\COPY raw.order_payments FROM '/data/raw/olist_order_payments_dataset.csv' WITH CSV HEADER"
psql -U dataeng -d ecommerce -c "\COPY raw.order_reviews FROM '/data/raw/olist_order_reviews_dataset.csv' WITH CSV HEADER"
psql -U dataeng -d ecommerce -c "\COPY raw.products FROM '/data/raw/olist_products_dataset.csv' WITH CSV HEADER"
psql -U dataeng -d ecommerce -c "\COPY raw.sellers FROM '/data/raw/olist_sellers_dataset.csv' WITH CSV HEADER"
psql -U dataeng -d ecommerce -c "\COPY raw.geolocation FROM '/data/raw/olist_geolocation_dataset.csv' WITH CSV HEADER"
psql -U dataeng -d ecommerce -c "\COPY raw.category_translation FROM '/data/raw/product_category_name_translation.csv' WITH CSV HEADER"

echo "=== Verifying row counts ==="
psql -U dataeng -d ecommerce -c "
SELECT 'customers'          AS table_name, COUNT(*) FROM raw.customers
UNION ALL SELECT 'orders',                 COUNT(*) FROM raw.orders
UNION ALL SELECT 'order_items',            COUNT(*) FROM raw.order_items
UNION ALL SELECT 'order_payments',         COUNT(*) FROM raw.order_payments
UNION ALL SELECT 'order_reviews',          COUNT(*) FROM raw.order_reviews
UNION ALL SELECT 'products',              COUNT(*) FROM raw.products
UNION ALL SELECT 'sellers',               COUNT(*) FROM raw.sellers
UNION ALL SELECT 'geolocation',           COUNT(*) FROM raw.geolocation
UNION ALL SELECT 'category_translation',  COUNT(*) FROM raw.category_translation
ORDER BY table_name;
"

echo "=== Seed complete ==="