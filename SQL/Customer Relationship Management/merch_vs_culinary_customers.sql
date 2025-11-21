/* 
--------------------------------------------------------------------------------
SQL Script: Customer Segments – Merch vs Culinary
--------------------------------------------------------------------------------
Objective:
    Produce a customer-level view for a selected year that:
        • Classifies customers into Culinary+Merch, CulinaryOnly, NonCulinary, or Other.
        • Summarizes customers, orders, and sales for each segment.
        • Provides KPIs such as customer share, sales share, ranking, and
          cumulative sales distribution.

Definition:
    - Base Population:
        • All valid orders for the selected year (real customers, positive sales,
          purchase orders only).
        • Each order is enriched with customer email information.
        • Merchandise sales are calculated as total sales minus culinary sales.

    - Customer Metrics:
        • Count how many orders each customer placed.
        • Sum total sales, culinary sales, and merchandise sales.
        • Count how many orders included a culinary class.
        • Count distinct emails linked to each customer.

    - Segment Rules:
        • Culinary+Merch: Customers who bought both culinary classes and merchandise.
        • CulinaryOnly: Customers who purchased only culinary classes.
        • NonCulinary: Customers with no culinary sales at all.
        • Other: Rare cases that don’t fit standard patterns.

    - Segment KPIs:
        • Customer count and total orders per segment.
        • Total sales, culinary sales, and merchandise sales.
        • Share of customers and share of revenue contributed by each segment.
        • Ranking of segments by revenue.
        • Cumulative share to understand revenue concentration.

Scope:
    - Customer base includes anyone with at least one valid order in the year.
    - Input grain: Order Header.
    - Output grain: One row per customer segment + one AllCustomers roll-up.

Processing Steps:
    1. Create a params CTE to hold the analysis year.
    2. Build base_orders filtering valid orders and adding email + merchandise amount.
    3. Aggregate to customer-year level (orders, sales, culinary counts).
    4. Assign each customer to a segment based on their mix of purchases.
    5. Aggregate results by segment and create an AllCustomers roll-up.
    6. Use window functions to compute shares, rankings, and cumulative revenue.
    7. Return final segment table in business-friendly order.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH
-- 1) Parameters
params AS (
    SELECT 2025 AS order_year_filter
),

-- 2) Base filtered orders with enrichment
base_orders AS (
    SELECT
        oh.target_key,
        oh.order_number,
        oh.net_amount,
        COALESCE(oh.cooking_school_amount, 0)      AS cooking_school_amount,
        COALESCE(oh.cooking_school_quantity, 0)    AS cooking_school_quantity,
        oh.order_year,
        t.email,
        -- derived merchandise amount
        (oh.net_amount - COALESCE(oh.cooking_school_amount, 0)) AS merch_amount
    FROM ims.order_header AS oh
    JOIN ims.target        AS t  ON oh.target_key = t.target_key
    JOIN params            AS p  ON oh.order_year = p.order_year_filter
    WHERE oh.target_key    <> -1
      AND oh.purchase_order = 'Y'
      AND oh.net_amount     > 0
),

-- 3) Customer-level yearly aggregates
customer_year AS (
    SELECT
        bo.target_key,
        COUNT(DISTINCT bo.order_number)                                            AS orders,
        SUM(bo.net_amount)                                                         AS total_sales,
        SUM(bo.cooking_school_amount)                                              AS cs_sales,
        SUM(bo.merch_amount)                                                       AS merch_sales,
        SUM(CASE WHEN bo.cooking_school_quantity > 0 THEN 1 ELSE 0 END)           AS cs_orders,
        COUNT(DISTINCT bo.email)                                                   AS distinct_emails
    FROM base_orders AS bo
    GROUP BY bo.target_key
),

-- 4) Assign customer segment based on sales mix
typed_customers AS (
    SELECT
        cy.*,
        CASE
            WHEN cy.cs_sales > 0 AND cy.merch_sales > 0 THEN 'Culinary+Merch'
            WHEN cy.cs_sales > 0 AND cy.merch_sales = 0 THEN 'CulinaryOnly'
            WHEN cy.cs_sales = 0                         THEN 'NonCulinary'
            ELSE 'Other'
        END AS customer_type
    FROM customer_year AS cy
),

-- 5) Segment aggregation + overall roll-up using GROUPING SETS
aggregated AS (
    SELECT
        CASE WHEN GROUPING(customer_type) = 1 THEN 'AllCustomers'
             ELSE customer_type
        END AS customer_segment,
        COUNT(*)                                   AS customer_count,
        SUM(total_sales)                           AS total_sales,
        SUM(cs_sales)                              AS total_cs_sales,
        SUM(merch_sales)                           AS total_merch_sales,
        SUM(orders)                                AS total_orders
    FROM typed_customers
    GROUP BY GROUPING SETS ((customer_type), ())
),

-- 6) Add segment shares, ranking, and cumulative sales distribution
annotated AS (
    SELECT
        a.*,
        SUM(a.customer_count) OVER ()                                AS grand_customer_count,
        SUM(a.total_sales)    OVER ()                                AS grand_total_sales,
        (a.customer_count * 1.0)
            / NULLIF(SUM(CASE WHEN a.customer_segment <> 'AllCustomers'
                               THEN a.customer_count END)
                     OVER (), 0)                                     AS customer_share,
        (a.total_sales * 1.0)
            / NULLIF(SUM(CASE WHEN a.customer_segment <> 'AllCustomers'
                               THEN a.total_sales END)
                     OVER (), 0)                                     AS sales_share,
        RANK() OVER (
            ORDER BY CASE WHEN a.customer_segment = 'AllCustomers'
                          THEN NULL ELSE a.total_sales END DESC
        )                                                            AS sales_rank,
        SUM(CASE WHEN a.customer_segment <> 'AllCustomers'
                 THEN a.total_sales ELSE 0 END
            ) OVER (
                ORDER BY CASE WHEN a.customer_segment = 'AllCustomers'
                              THEN NULL ELSE a.total_sales END DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            / NULLIF(SUM(CASE WHEN a.customer_segment <> 'AllCustomers'
                               THEN a.total_sales END)
                     OVER (), 0)                                     AS cum_sales_share
    FROM aggregated AS a
)

-- 7) Final output
SELECT
    customer_segment,
    customer_count,
    total_orders,
    total_sales,
    total_cs_sales,
    total_merch_sales,
    grand_customer_count,
    grand_total_sales,
    customer_share,
    sales_share,
    sales_rank,
    cum_sales_share
FROM annotated
ORDER BY
    CASE customer_segment
        WHEN 'AllCustomers'   THEN 0
        WHEN 'Culinary+Merch' THEN 1
        WHEN 'CulinaryOnly'   THEN 2
        WHEN 'NonCulinary'    THEN 3
        ELSE 4
    END;
