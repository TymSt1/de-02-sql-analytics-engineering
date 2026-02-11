# 🔷 Project 2: SQL Analytics Engineering on PostgreSQL

**Part of the [Data Engineering Roadmap](https://github.com/TymSt1)**

A complete analytical data model built on the Brazilian E-Commerce dataset (Olist),
running in PostgreSQL via Docker. Implements a medallion/layered architecture
(raw → staging → intermediate → mart) with performance-tuned indexes,
EXPLAIN ANALYZE profiling, and 18 advanced analytical queries.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                             DATA FLOW                                    │
│                                                                          │
│  CSV Files ──→ Raw Layer ──→ Staging ──→ Intermediate ──→ Mart           │
│  (9 files)     (TEXT cols)   (typed,     (business       (materialized   │
│                              cleaned,    logic, joins,    views, ready   │
│                              PKs added)  SCD Type 2)      for dashboards)│
│                                                                          │
│                         ┌──────────────────────┐                         │
│                         │   Indexes & Tuning    │                        │
│                         │  (strategic indexes,  │                        │
│                         │   EXPLAIN ANALYZE,    │                        │
│                         │   composite indexes)  │                        │
│                         └──────────┬───────────┘                         │
│                                    │                                     │
│                         ┌──────────▼───────────┐                         │
│                         │  Analytical Queries   │                        │
│                         │  (18 queries across   │                        │
│                         │   all layers using    │                        │
│                         │   window functions,   │                        │
│                         │   CTEs, ROLLUP/CUBE)  │                        │
│                         └──────────────────────┘                         │
└──────────────────────────────────────────────────────────────────────────┘
```

## Tech Stack

- **PostgreSQL 16** — analytical database
- **Docker Compose** — containerized infrastructure
- **pgAdmin 4** — database GUI
- **Make** — task automation

## Dataset

**Brazilian E-Commerce Public Dataset by Olist** (Kaggle)
- 9 tables, ~100K orders, ~113K items, ~3K sellers
- Timeframe: Sep 2016 – Oct 2018
- Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Project Structure

```
de-02-sql-analytics-engineering/
├── docker-compose.yml
├── Makefile
├── README.md
├── data/raw/                        # CSV files (not in git)
├── scripts/
│   └── seed.sh                      # Load CSVs into PostgreSQL
└── sql/
    ├── 00_schema_raw/               # Raw table definitions (all TEXT)
    │   └── create_tables.sql
    ├── 01_staging/                   # Type casting, cleaning, PKs
    │   └── create_staging.sql
    ├── 02_intermediate/             # Business logic, SCD Type 2
    │   └── create_intermediate.sql
    ├── 03_mart/                     # Materialized views for analytics
    │   └── create_marts.sql
    ├── 04_indexes/                  # Indexing strategy + EXPLAIN ANALYZE
    │   └── indexes_and_plans.sql
    └── 05_queries/                  # 18 analytical queries
        └── analytical_queries.sql
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Make (optional, commands listed below)
- Olist dataset from Kaggle placed in `data/raw/`

### Setup
```bash
git clone https://github.com/TymSt1/de-02-sql-analytics-engineering.git
cd de-02-sql-analytics-engineering

# Download Olist dataset from Kaggle and place CSVs in data/raw/

make up        # Start PostgreSQL + pgAdmin
make seed      # Create schemas and load data
make psql      # Connect to database
```

### Run the full pipeline
```sql
-- Inside psql, run in order:
\i /sql/01_staging/create_staging.sql
\i /sql/02_intermediate/create_intermediate.sql
\i /sql/03_mart/create_marts.sql
\i /sql/04_indexes/indexes_and_plans.sql
\i /sql/05_queries/analytical_queries.sql
```

### pgAdmin
Open http://localhost:8080 (admin@admin.com / admin)
Add server: host=postgres, port=5432, user=dataeng, password=dataeng123

## Data Model Layers

### Raw
Direct CSV imports. All columns TEXT. No transformations.

### Staging (8 tables)
- Type casting (TEXT → TIMESTAMP, INTEGER, NUMERIC)
- String normalization (INITCAP, TRIM, UPPER)
- Deduplication (reviews, geolocation)
- Primary keys added
- Category names translated to English

### Intermediate (5 tables)
- **int_orders_enriched** — Central fact table joining orders + customers + items + payments + reviews. Includes delivery performance metrics.
- **int_seller_performance** — Aggregated seller KPIs: revenue, review scores, delivery speed.
- **int_product_performance** — Product-level sales and review metrics.
- **int_customer_history** — Customer lifetime value, repeat purchase detection.
- **int_seller_status_history** — SCD Type 2 tracking seller active/inactive status over time using LAG/LEAD window functions.

### Mart (5 materialized views)
- **mart_monthly_revenue** — Time-series business health dashboard
- **mart_state_performance** — Geographic revenue and delivery analysis
- **mart_product_category_analysis** — Category P&L with review scores
- **mart_seller_scorecard** — Seller ranking with PERCENT_RANK and tier classification
- **mart_customer_segments** — RFM segmentation (Recency/Frequency/Monetary)

## Indexes & Performance Tuning

Strategic indexes applied across staging and intermediate layers targeting common WHERE, JOIN, and GROUP BY patterns. Includes single-column indexes on high-filter columns (order_status, purchase_timestamp, product_id, seller_id) and a composite index on (customer_state, order_date) for combined filter queries.

Performance validated with EXPLAIN ANALYZE, comparing Index Only Scan vs Seq Scan on the same query. Demonstrated a **37x speedup** (0.12ms vs 4.48ms) by eliminating full table scans on 99K rows.

## Analytical Queries (18 queries)

Each query answers a specific business question and is documented with comments in the SQL file.

| # | Query | Business Question | Key SQL Techniques |
|---|-------|-------------------|--------------------|
| Q1 | Monthly revenue growth | What's our MoM growth rate? | LAG, CTE |
| Q2 | Cumulative GMV | What's our running total revenue? | SUM OVER |
| Q3 | Top categories by revenue | What are our best-selling categories? | RANK |
| Q4 | Revenue Pareto by state | Which states make up 80% of revenue? | SUM OVER, cumulative % |
| Q5 | Top sellers per state | Who are the best sellers in each state? | ROW_NUMBER, PARTITION BY |
| Q6 | Repeat order gap | How long between repeat purchases? | LAG, LEAD, PARTITION BY |
| Q7 | Payment method trends | How are payment preferences changing? | FILTER (WHERE), pivot |
| Q8 | Delivery by day of week | Which days have worst delivery times? | EXTRACT, CASE |
| Q9 | Cohort retention | Do customers come back after first purchase? | Self-join, cohort pattern |
| Q10 | Revenue with ROLLUP | Category revenue with state subtotals? | GROUP BY ROLLUP, GROUPING |
| Q11 | Revenue with CUBE | All possible subtotal combinations? | GROUP BY CUBE |
| Q12 | Sellers above state avg | Which sellers beat their local competition? | Subquery, ratio analysis |
| Q13 | 7-day moving average | What's the smoothed daily order trend? | ROWS BETWEEN |
| Q14 | Calendar gap-filling | Revenue for every day including zero-order days? | Recursive CTE, LEFT JOIN |
| Q15 | Delivery vs reviews | How much does late delivery hurt reviews? | Conditional aggregation, HAVING |
| Q16 | Shared customer pairs | Which sellers compete for the same buyers? | Self-join, pair analysis |
| Q17 | New vs returning customers | Are we growing through new or returning customers? | CTE, FILTER, LAG |
| Q18 | Freight cost analysis | Which products have disproportionate shipping costs? | HAVING, ratio analysis |

## SQL Techniques

### Window Functions
ROW_NUMBER, RANK, LAG, LEAD, SUM OVER, AVG OVER, NTILE, PERCENT_RANK, ROWS BETWEEN (moving average)

### CTEs & Recursive CTEs
Calendar generation with gap-filling, cohort analysis, RFM scoring

### Aggregation
GROUP BY ROLLUP, GROUP BY CUBE, FILTER (WHERE ...), HAVING, conditional aggregation

### Joins & Subqueries
Self-joins (seller pair analysis), correlated subqueries, multi-table joins

### Data Modeling
Medallion architecture, SCD Type 2, materialized views with concurrent refresh

### Performance
Strategic indexing, composite indexes, EXPLAIN ANALYZE comparison (37x speedup demonstrated)

## Key Findings

| Insight | Detail |
|---------|--------|
| Revenue concentration | SP, RJ, MG = 62.5% of all revenue |
| Late delivery impact | Avg review drops from 4.29 → 2.57 when late |
| Customer retention | Only ~3% returning customers monthly |
| Black Friday spike | Nov 24, 2017: 1,147 orders (7x normal) |
| Top category | Health & Beauty: R$1.23M revenue |
| Freight problem | Electronics freight = 68% of product price |

## Technical Findings

- Layered data modeling enforces clean separation of concerns
- SCD Type 2 requires careful handling of date ranges and current-record flags
- Materialized views with unique indexes enable CONCURRENTLY refresh
- Indexing can improve query performance by 37x on filtered scans
- ROLLUP/CUBE provide multi-level subtotals without multiple queries
- Recursive CTEs solve gap-filling problems elegantly
