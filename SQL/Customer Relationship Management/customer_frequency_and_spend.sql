/* 
--------------------------------------------------------------------------------
SQL Script: Customer Frequency & Spend Distribution (Yearly Buckets)
--------------------------------------------------------------------------------
Objective:
    Build a yearly customer-level view of purchase frequency and spend that:
        • Buckets customers by capped visit frequency (1–5, 6+)
        • Buckets customers into sales bands (0–1K, 1K–2.5K, …, 10K+)
        • Quantifies how many customers and how much sales fall in each bucket
        • Adds shares, ranks, and cumulative sales curves by year

    Results feed CRM analytics for:
        • Segmentation (e.g., low/medium/high-value, high-frequency cohorts)
        • Retention and reactivation strategy
        • Pareto-style analysis of “top X% customers drive Y% of sales”.

Definition:
    - Base Orders:
        • Valid transactions with net_amount > 0 and target_key <> -1
        • Grain: Order Header (order_number × customer × year1)

    - Customer-Year Aggregates:
        • frequency  = distinct orders per customer per year
        • sales      = total net_amount per customer per year
        • avg_order_value = average order value per customer per year

    - Frequency Bucket:
        • 1–5 visits kept as-is
        • 6+ visits grouped into a single “freq_cap + 1” bucket

    - Sales Buckets:
        • 0–1000, 1001–2500, 2501–5000, 5001–7500, 7501–10000, 10000+

    - Year-Level Metrics:
        • customer_count, distinct_customers, total_sales
        • avg_sales_per_customer, avg_order_value_mean
        • customer_share, sales_share, sales_rank, cum_sales_share

Scope:
    - Includes all customers with at least one valid order in the year.
    - Date grain: Year1 from order header.
    - Output grain: Year × Frequency_Bucket × Sales_Bucket.

Processing Steps:
    1. params: Define tunable parameters (frequency cap and sales bucket thresholds).
    2. base_orders: Filter raw order_header to valid transactions (customer + year + amount).
    3. cust_year: Aggregate to customer-year level (frequency, total sales, AOV).
    4. bucketed: Apply frequency capping and assign each customer-year to a sales bucket.
    5. grouped: Aggregate metrics by (year1, frequency_bucket, sales_bucket).
    6. annotated:  Use window functions to:
            - Compute total customers and total sales per year
            - Calculate customer_share and sales_share
            - Rank buckets by total_sales (sales_rank)
            - Compute cum_sales_share for Pareto-style analysis.
    7. Final SELECT:
        • Return bucketed metrics ordered by year, frequency_capped, sales_bucket
          for downstream BI / CRM reporting.

Author: Cesar Rodriguez
--------------------------------------------------------------------------------
*/

WITH
-- 1) Parameters
params AS (
    SELECT
        5  AS freq_cap,        -- cap frequency at 5, 6 = 6+
        1000 AS b1,
        2500 AS b2,
        5000 AS b3,
        7500 AS b4,
        10000 AS b5
),

-- 2) Base orders (valid transactions)
base_orders AS (
    SELECT
        oh.target_key,
        oh.order_number,
        oh.net_amount,
        oh.order_year AS year1
    FROM ims.order_header AS oh
    WHERE oh.target_key <> -1
      AND oh.net_amount > 0
),

-- 3) Customer-year aggregates
cust_year AS (
    SELECT
        target_key,
        year1,
        COUNT(DISTINCT order_number) AS frequency,
        SUM(net_amount)              AS sales,
        AVG(net_amount)              AS avg_order_value
    FROM base_orders
    GROUP BY target_key, year1
),

-- 4) Apply frequency & sales buckets
bucketed AS (
    SELECT
        cy.*,
        CASE
            WHEN cy.frequency <= p.freq_cap THEN cy.frequency
            ELSE p.freq_cap + 1
        END AS frequency_bucket,
        CASE
            WHEN cy.sales <= p.b1                    THEN '0-1000'
            WHEN cy.sales >  p.b1 AND cy.sales <= p.b2 THEN '1001-2500'
            WHEN cy.sales >  p.b2 AND cy.sales <= p.b3 THEN '2501-5000'
            WHEN cy.sales >  p.b3 AND cy.sales <= p.b4 THEN '5001-7500'
            WHEN cy.sales >  p.b4 AND cy.sales <= p.b5 THEN '7501-10000'
            ELSE '10000+'
        END AS sales_bucket
    FROM cust_year cy
    CROSS JOIN params p
),

-- 5) Group metrics by (year, freq_bucket, sales_bucket)
grouped AS (
    SELECT
        year1,
        frequency_bucket,
        sales_bucket,
        COUNT(*)                    AS customer_count,
        COUNT(DISTINCT target_key)  AS distinct_customers,
        SUM(sales)                  AS total_sales,
        AVG(sales)                  AS avg_sales_per_customer,
        AVG(avg_order_value)        AS avg_order_value_mean
    FROM bucketed
    GROUP BY year1, frequency_bucket, sales_bucket
),

-- 6) Year-level shares, ranks, cumulative curves
annotated AS (
    SELECT
        g.*,
        SUM(g.customer_count) OVER (PARTITION BY g.year1) AS year_customer_total,
        SUM(g.total_sales)    OVER (PARTITION BY g.year1) AS year_sales_total,

        -- share of customers / sales within the year
        (g.customer_count * 1.0) / NULLIF(SUM(g.customer_count) OVER (PARTITION BY g.year1), 0) AS customer_share,
        (g.total_sales    * 1.0) / NULLIF(SUM(g.total_sales)    OVER (PARTITION BY g.year1), 0) AS sales_share,

        -- rank buckets by sales within the year (desc)
        RANK() OVER (PARTITION BY g.year1 ORDER BY g.total_sales DESC) AS sales_rank,

        -- cumulative distribution of sales by descending bucket
        SUM(g.total_sales) OVER (
            PARTITION BY g.year1
            ORDER BY g.total_sales DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / NULLIF(SUM(g.total_sales) OVER (PARTITION BY g.year1), 0) AS cum_sales_share
    FROM grouped g
)

-- Final result
SELECT
    year1,
    frequency_bucket AS frequency_capped, 
    sales_bucket,
    customer_count,
    distinct_customers,
    total_sales,
    avg_sales_per_customer,
    avg_order_value_mean,
    year_customer_total,
    year_sales_total,
    customer_share,
    sales_share,
    cum_sales_share,
    sales_rank
FROM annotated
ORDER BY year1, frequency_capped, sales_bucket;

