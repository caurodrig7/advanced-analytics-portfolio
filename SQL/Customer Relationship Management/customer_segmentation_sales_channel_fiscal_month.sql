/*
--------------------------------------------------------------------------------
SQL Script: Customer Segmentation by Sales Channel and Fiscal Month
--------------------------------------------------------------------------------
Objective:
    Classify customers by engagement status to measure lifecycle behavior 
    across Retail and Online channels.

Definition:
    This query assigns each customer to one of the following categories:
        - New: First purchase within the selected fiscal year.
        - Retained: Purchases in both the current and prior fiscal years.
        - Reactivated: Returned after at least one fiscal year of inactivity.
        - Anonymous: Orders with no identifiable customer email or ID.

Scope:
    - Includes Retail and Online channel views.
    - Aggregated at the Fiscal Month level.
    - Designed for fiscal month reporting.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

-- enrich xcenter bopis orders with oroms bopis email
WITH sales_header_with_enriched_email AS (
    SELECT 
        peep.sales_header.order_analytics_id,
        peep.sales_header.source,
        peep.sales_header.order_type,
        COALESCE(peep.sales_header.email, online.email) AS enriched_email
    FROM peep.sales_header
    LEFT JOIN peep.sales_header AS online
        ON peep.sales_header.alt_order_number_1 = online.order_number
        AND peep.sales_header.source = 'xcenter'
        AND peep.sales_header.order_type = 'bopis'
        AND online.source = 'oroms'
        AND online.order_type LIKE '%bopis%'
),
-- customer purchases by fiscal month and channel
customers_by_month AS (
    SELECT DISTINCT 
        sales_header_with_enriched_email.enriched_email AS email,
        common.calendar.fiscal_month_id,
        common.calendar.month_analytics_id,
        CASE 
            WHEN sales_header_with_enriched_email.source = 'oroms'
              OR sales_header_with_enriched_email.order_type LIKE '%bopis%'
              OR sales_header_with_enriched_email.order_type = 'ship_from_store' THEN 'Direct'
            WHEN sales_header_with_enriched_email.source = 'xcenter' THEN 'Retail'
        END AS sales_channel_type
    FROM peep.written_sales
    JOIN sales_header_with_enriched_email
        ON peep.written_sales.order_analytics_id = sales_header_with_enriched_email.order_analytics_id
    JOIN peep.sales_line
        ON peep.sales_line.order_line_analytics_id = peep.written_sales.order_line_analytics_id 
    JOIN peep.product_to_merchandising_taxonomy
        ON peep.product_to_merchandising_taxonomy.product_analytics_id = peep.written_sales.product_analytics_id
    JOIN common.calendar
        ON common.calendar.date_analytics_id = peep.written_sales.date_ordered_analytics_id
    WHERE sales_header_with_enriched_email.enriched_email IS NOT NULL
      AND peep.sales_line.order_analytics_id NOT IN (
            SELECT DISTINCT order_analytics_id 
            FROM peep.sales_line 
            WHERE sales_channel LIKE '%amazon%' 
               OR sales_channel LIKE '%amzbopis%' 
               OR order_line_type LIKE '%amazon%'
        )
      AND peep.product_to_merchandising_taxonomy.level_4_name NOT LIKE 'GIFT CERTIFICATES'
),
-- first purchase month per customer and channel
customer_first_month AS (
    SELECT 
        email, 
        sales_channel_type,
        MIN(month_analytics_id) AS first_month_analytics_id
    FROM customers_by_month
    GROUP BY email, sales_channel_type
),
-- activity in the last 12 months
orders_last_12_months AS (
    SELECT a.email, a.sales_channel_type, b.month_analytics_id AS target_month
    FROM customers_by_month a
    JOIN customers_by_month b
        ON a.month_analytics_id BETWEEN b.month_analytics_id - 12 AND b.month_analytics_id - 1
        AND a.email = b.email
        AND a.sales_channel_type = b.sales_channel_type
),
-- activity before the last 12 months
orders_before_last_12_months AS (
    SELECT a.email, a.sales_channel_type, b.month_analytics_id AS target_month
    FROM customers_by_month a
    JOIN customers_by_month b
        ON a.month_analytics_id < b.month_analytics_id - 12
        AND a.email = b.email
        AND a.sales_channel_type = b.sales_channel_type
),
-- classification per customer per month
classified_customers AS (
    SELECT DISTINCT
        customers_by_month.email,
        customers_by_month.fiscal_month_id,
        customers_by_month.month_analytics_id,
        customers_by_month.sales_channel_type,
        CASE
            WHEN customers_by_month.month_analytics_id = customer_first_month.first_month_analytics_id THEN 'New'
            WHEN orders_last_12_months.email IS NOT NULL THEN 'Retained'
            WHEN orders_before_last_12_months.email IS NOT NULL AND orders_last_12_months.email IS NULL THEN 'Reactivated'
            ELSE 'Unclassified'
        END AS customer_type
    FROM customers_by_month
    JOIN customer_first_month
        ON customers_by_month.email = customer_first_month.email
        AND customers_by_month.sales_channel_type = customer_first_month.sales_channel_type
    LEFT JOIN orders_last_12_months
        ON customers_by_month.email = orders_last_12_months.email
        AND customers_by_month.sales_channel_type = orders_last_12_months.sales_channel_type
        AND customers_by_month.month_analytics_id = orders_last_12_months.target_month
    LEFT JOIN orders_before_last_12_months
        ON customers_by_month.email = orders_before_last_12_months.email
        AND customers_by_month.sales_channel_type = orders_before_last_12_months.sales_channel_type
        AND customers_by_month.month_analytics_id = orders_before_last_12_months.target_month
),
-- join classification to orders
orders_with_classification AS (
    SELECT 
        classified_customers.fiscal_month_id,
        classified_customers.sales_channel_type,
        classified_customers.customer_type,
        peep.written_sales.order_analytics_id,
        peep.written_sales.merchandise,
        peep.written_sales.quantity,
        classified_customers.email
    FROM peep.written_sales
    JOIN sales_header_with_enriched_email
        ON peep.written_sales.order_analytics_id = sales_header_with_enriched_email.order_analytics_id
    JOIN peep.sales_line
        ON peep.sales_line.order_line_analytics_id = peep.written_sales.order_line_analytics_id
    JOIN peep.product_to_merchandising_taxonomy
        ON peep.product_to_merchandising_taxonomy.product_analytics_id = peep.written_sales.product_analytics_id
    JOIN common.calendar
        ON common.calendar.date_analytics_id = peep.written_sales.date_ordered_analytics_id
    JOIN classified_customers
        ON classified_customers.email = sales_header_with_enriched_email.enriched_email
        AND classified_customers.fiscal_month_id = common.calendar.fiscal_month_id
        AND classified_customers.sales_channel_type = CASE 
            WHEN sales_header_with_enriched_email.source = 'oroms'
              OR sales_header_with_enriched_email.order_type LIKE '%bopis%'
              OR sales_header_with_enriched_email.order_type = 'ship_from_store' THEN 'Direct'
            WHEN sales_header_with_enriched_email.source = 'xcenter' THEN 'Retail'
        END
    WHERE sales_header_with_enriched_email.enriched_email IS NOT NULL
      AND peep.sales_line.order_analytics_id NOT IN (
            SELECT DISTINCT order_analytics_id 
            FROM peep.sales_line 
            WHERE sales_channel LIKE '%amazon%' 
               OR sales_channel LIKE '%amzbopis%' 
               OR order_line_type LIKE '%amazon%'
        )
      AND peep.product_to_merchandising_taxonomy.level_4_name NOT LIKE 'GIFT CERTIFICATES'
),
-- anonymous orders
anonymous_orders AS (
    SELECT 
        common.calendar.fiscal_month_id,
        CASE 
            WHEN sales_header_with_enriched_email.source = 'oroms'
              OR sales_header_with_enriched_email.order_type LIKE '%bopis%'
              OR sales_header_with_enriched_email.order_type = 'ship_from_store' THEN 'Direct'
            WHEN sales_header_with_enriched_email.source = 'xcenter' THEN 'Retail'
        END AS sales_channel_type,
        'Anonymous' AS customer_type,
        peep.written_sales.order_analytics_id,
        peep.written_sales.merchandise,
        peep.written_sales.quantity,
        NULL AS email
    FROM peep.written_sales
    JOIN sales_header_with_enriched_email
        ON peep.written_sales.order_analytics_id = sales_header_with_enriched_email.order_analytics_id
    JOIN peep.sales_line
        ON peep.sales_line.order_line_analytics_id = peep.written_sales.order_line_analytics_id
    JOIN peep.product_to_merchandising_taxonomy
        ON peep.product_to_merchandising_taxonomy.product_analytics_id = peep.written_sales.product_analytics_id
    JOIN common.calendar
        ON common.calendar.date_analytics_id = peep.written_sales.date_ordered_analytics_id
    WHERE sales_header_with_enriched_email.enriched_email IS NULL
      AND peep.sales_line.order_analytics_id NOT IN (
            SELECT DISTINCT order_analytics_id 
            FROM peep.sales_line 
            WHERE sales_channel LIKE '%amazon%' 
               OR sales_channel LIKE '%amzbopis%' 
               OR order_line_type LIKE '%amazon%'
        )
      AND peep.product_to_merchandising_taxonomy.level_4_name NOT LIKE 'GIFT CERTIFICATES'
)
-- final aggregation
SELECT 
    combined.fiscal_month_id,
    combined.sales_channel_type,
    combined.customer_type,
    COUNT(DISTINCT COALESCE(combined.email, CAST(combined.order_analytics_id AS VARCHAR))) AS customer_count,
    COUNT(DISTINCT combined.order_analytics_id) AS order_count,
    SUM(combined.merchandise) AS total_order_value,
    SUM(combined.quantity) AS total_units
FROM (
    SELECT * FROM orders_with_classification
    UNION ALL
    SELECT * FROM anonymous_orders
) AS combined
GROUP BY combined.fiscal_month_id, combined.sales_channel_type, combined.customer_type
ORDER BY combined.fiscal_month_id, combined.sales_channel_type, combined.customer_type;