/* 
--------------------------------------------------------------------------------
SQL Script: Cooking Class Frequency – Buckets, Cohorts & Price Mix
--------------------------------------------------------------------------------
Objective:
    Produce a customer-year view of Cooking School engagement that:
        • Buckets customers by annual class frequency (1–5, 6+).
        • Breaks out Full-Price vs Sale mix for Cooking School purchases.
        • Segments each year into First-Year vs Post–First-Year customers.
        • Summarizes quantity, sales, and mix at the frequency × price × tenure level.
        • Adds yearly KPIs such as customer share, sales share, ranking, and
          cumulative sales distribution.

    Results feed Cooking School analytics to evaluate:
        • Frequency distribution of class attendees.
        • Movement of cohorts across years.
        • Price sensitivity and mix behavior.
        • Revenue concentration and repeat-class patterns.

Definition:
    - Base Cooking School Orders:
        • Orders with positive cooking_school_quantity.
        • Must be valid transactions (purchase_order = 'Y', net_amount > 0, not cancelled).
        • Captures order_date, order_year, quantity, and sales.

    - First-Year Cohort:
        • First Cooking School year per customer based on MIN(order_year).
        • Used to classify each customer-year as:
              – FirstYear (first time taking CS classes)
              – PostFirstYear (subsequent years)

    - Price Mix:
        • Derived from order_lines joined to product SKUs.
        • FullPrice band uses a predefined price list.
        • Sale band = all other prices.
        • Mix types:
              – FullPriceOnly
              – SaleOnly
              – Mixed

    - Customer-Year Metrics:
        • frequency: # distinct CS orders per customer per year.
        • cs_quantity: total CS units purchased.
        • cs_sales: total Cooking School revenue.
        • fullprice_sales / sale_sales: CS revenue split by price band.

    - Buckets:
        • Frequency bucket capped at 5 (i.e., 1–5, 6+).
        • Buckets applied after aggregating at customer-year level.

    - Aggregated Metrics:
        • Customer count per bucket.
        • CS quantity and CS sales per bucket.
        • Full-price vs sale sales within bucket.
        • Tenure and price-mix drill-downs.

    - Year-Level KPIs:
        • customer_share_in_year: bucket customers ÷ total CS customers in that year.
        • sales_share_in_year: bucket CS sales ÷ total CS sales for that year.
        • sales_rank_in_year: rank by total CS sales within each year.
        • cum_sales_share_in_year: cumulative sales curve ordered by CS sales.

    - Cohort Summary:
        • Size of each first-year cohort.
        • Share of each cohort relative to all CS customers.

Scope:
    - Cooking School transactions only.
    - Customer-year output for all years present in the data.
    - Supplemental cohort view for first-year customers.

Processing Steps:
    1. Build params (frequency cap).
    2. Build base_orders to isolate valid CS orders.
    3. Build first_cs_year to assign cohorts.
    4. Join orders to cohorts (orders_with_cohort).
    5. Build cs_order_lines to classify price bands.
    6. Aggregate customer-year CS metrics (customer_year).
    7. Summarize price mix per customer-year (price_mix).
    8. Merge to enriched customer-year records (customer_year_enriched).
    9. Apply frequency buckets (bucketed).
   10. Summarize by frequency × price_mix × tenure (bucket_summary).
   11. Apply window KPIs per year (bucket_summary_with_kpis).
   12. Build the first-year cohort table (cohort_summary).
   13. Output:
          – Detailed bucket table by year
          – Cohort distribution summary

Author: Cesar Rodriguez
--------------------------------------------------------------------------------
*/

WITH
-- 1) Parameters
params AS (
    SELECT
        5   AS freq_cap        -- 1–5, 6 = 6+ visits
),

-- 2) Base Cooking School orders (valid transactions only)
base_orders AS (
    SELECT
        oh.target_key,
        oh.order_number,
        oh.order_date,
        EXTRACT(YEAR FROM oh.order_date) AS order_year,
        oh.cooking_school_quantity,
        oh.cooking_school_amount,
        oh.net_amount
    FROM ims.order_header oh
    WHERE oh.cooking_school_quantity > 0
      AND oh.target_key <> -1
      AND oh.purchase_order = 'Y'
      AND oh.cancel_quantity = 0
      AND oh.net_amount > 0
),

-- 3) First Cooking School year per customer (cohort)
first_cs_year AS (
    SELECT
        bo.target_key,
        MIN(bo.order_year) AS first_year
    FROM base_orders bo
    GROUP BY bo.target_key
),

-- 4) Join base orders with first-year info (for tenure / cohort logic)
orders_with_cohort AS (
    SELECT
        bo.*,
        fcy.first_year
    FROM base_orders bo
    JOIN first_cs_year fcy
      ON bo.target_key = fcy.target_key
),

-- 5) Order lines + price band (FullPrice vs Sale) for Cooking School orders
cs_order_lines AS (
    SELECT
        owc.target_key,
        owc.order_number,
        owc.order_year,
        owc.order_date,
        ol.net_amount    AS line_amount,
        ol.net_quantity  AS line_quantity,
        ol.price,
        CASE
            WHEN ol.price IN (49, 59, 69, 79, 195, 200, 210, 250, 295)
                THEN 'FullPrice'
            ELSE 'Sale'
        END AS price_band
    FROM orders_with_cohort owc
    JOIN ims.order_line     ol
      ON owc.order_number = ol.order_number
    WHERE ol.net_amount > 0
),

-- 6) Customer-year aggregates for Cooking School (frequency, sales, quantity)
customer_year AS (
    SELECT
        owc.target_key,
        owc.order_year,
        COUNT(DISTINCT owc.order_number)    AS frequency,
        SUM(owc.cooking_school_quantity)    AS cs_quantity,
        SUM(owc.cooking_school_amount)      AS cs_sales,
        MIN(owc.first_year)                 AS first_year
    FROM orders_with_cohort owc
    GROUP BY
        owc.target_key,
        owc.order_year
),

-- 7) Price mix per customer-year (how much FullPrice vs Sale they bought)
price_mix AS (
    SELECT
        csl.target_key,
        csl.order_year,
        SUM(CASE WHEN csl.price_band = 'FullPrice'
                 THEN csl.line_amount ELSE 0 END) AS fullprice_sales,
        SUM(CASE WHEN csl.price_band = 'Sale'
                 THEN csl.line_amount ELSE 0 END) AS sale_sales,
        CASE
            WHEN SUM(CASE WHEN csl.price_band = 'FullPrice'
                          THEN csl.line_amount ELSE 0 END)
               > 0
             AND SUM(CASE WHEN csl.price_band = 'Sale'
                          THEN csl.line_amount ELSE 0 END)
               = 0
                THEN 'FullPriceOnly'
            WHEN SUM(CASE WHEN csl.price_band = 'FullPrice'
                          THEN csl.line_amount ELSE 0 END)
               = 0
             AND SUM(CASE WHEN csl.price_band = 'Sale'
                          THEN csl.line_amount ELSE 0 END)
               > 0
                THEN 'SaleOnly'
            WHEN SUM(CASE WHEN csl.price_band = 'FullPrice'
                          THEN csl.line_amount ELSE 0 END)
               > 0
             AND SUM(CASE WHEN csl.price_band = 'Sale'
                          THEN csl.line_amount ELSE 0 END)
               > 0
                THEN 'Mixed'
            ELSE 'Unknown'
        END AS price_mix_type
    FROM cs_order_lines csl
    GROUP BY
        csl.target_key,
        csl.order_year
),

-- 8) Enrich customer-year with price mix and tenure in that year
customer_year_enriched AS (
    SELECT
        cy.target_key,
        cy.order_year,
        cy.frequency,
        cy.cs_quantity,
        cy.cs_sales,
        cy.first_year,
        pm.fullprice_sales,
        pm.sale_sales,
        pm.price_mix_type,
        CASE
            WHEN cy.order_year = cy.first_year THEN 'FirstYear'
            WHEN cy.order_year > cy.first_year THEN 'PostFirstYear'
            ELSE 'PreFirstYear'
        END AS tenure_in_year
    FROM customer_year cy
    LEFT JOIN price_mix pm
      ON cy.target_key = pm.target_key
     AND cy.order_year = pm.order_year
),

-- 9) Apply frequency buckets (1–5, 6+), using params for cap
bucketed AS (
    SELECT
        cye.*,
        CASE
            WHEN cye.frequency <= p.freq_cap THEN cye.frequency
            ELSE p.freq_cap + 1
        END AS frequency_bucket
    FROM customer_year_enriched cye
    CROSS JOIN params p
),

-- 10) Aggregate by year, frequency bucket, price mix, and tenure
bucket_summary AS (
    SELECT
        b.order_year,
        b.frequency_bucket,
        b.price_mix_type,
        b.tenure_in_year,
        COUNT(DISTINCT b.target_key)        AS customer_count,
        SUM(b.cs_quantity)                  AS total_cs_quantity,
        SUM(b.cs_sales)                     AS total_cs_sales,
        SUM(COALESCE(b.fullprice_sales,0))  AS total_fullprice_sales,
        SUM(COALESCE(b.sale_sales,0))       AS total_sale_sales
    FROM bucketed b
    GROUP BY
        b.order_year,
        b.frequency_bucket,
        b.price_mix_type,
        b.tenure_in_year
),

-- 11) Add window-based KPIs (shares, ranking, cumulative curves per year)
bucket_summary_with_kpis AS (
    SELECT
        bs.*,
        -- Totals per year for normalization
        SUM(bs.customer_count)  OVER (PARTITION BY bs.order_year) AS year_total_customers,
        SUM(bs.total_cs_sales)  OVER (PARTITION BY bs.order_year) AS year_total_cs_sales,

        -- Shares within the year
        (bs.customer_count * 1.0)
            / NULLIF(SUM(bs.customer_count) OVER (PARTITION BY bs.order_year), 0)
            AS customer_share_in_year,

        (bs.total_cs_sales * 1.0)
            / NULLIF(SUM(bs.total_cs_sales) OVER (PARTITION BY bs.order_year), 0)
            AS sales_share_in_year,

        -- Rank buckets by CS sales within the year
        RANK() OVER (
            PARTITION BY bs.order_year
            ORDER BY bs.total_cs_sales DESC
        ) AS sales_rank_in_year,

        -- Cumulative sales share (Pareto-style) within the year
        SUM(bs.total_cs_sales) OVER (
            PARTITION BY bs.order_year
            ORDER BY bs.total_cs_sales DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        / NULLIF(
            SUM(bs.total_cs_sales) OVER (PARTITION BY bs.order_year),
            0
        ) AS cum_sales_share_in_year
    FROM bucket_summary bs
),

-- 12) Cohort summary (first-year distribution of CS buyers)
cohort_summary AS (
    SELECT
        fcy.first_year,
        COUNT(DISTINCT fcy.target_key) AS cohort_customers,
        COUNT(DISTINCT fcy.target_key) * 1.0
            / NULLIF(SUM(COUNT(DISTINCT fcy.target_key)) OVER (), 0)
            AS cohort_customer_share
    FROM first_cs_year fcy
    GROUP BY fcy.first_year
)

-- Final result 1: Detailed frequency x price mix x tenure per year
SELECT
    order_year,
    frequency_bucket           AS frequency_bucket_capped,  
    price_mix_type,
    tenure_in_year,
    customer_count,
    total_cs_quantity,
    total_cs_sales,
    total_fullprice_sales,
    total_sale_sales,
    year_total_customers,
    year_total_cs_sales,
    customer_share_in_year,
    sales_share_in_year,
    sales_rank_in_year,
    cum_sales_share_in_year
FROM bucket_summary_with_kpis
ORDER BY
    order_year,
    frequency_bucket_capped,
    price_mix_type,
    tenure_in_year;

-- Final result 2: Cohort distribution of first Cooking School year
SELECT
    first_year,
    cohort_customers,
    cohort_customer_share
FROM cohort_summary
ORDER BY first_year;
