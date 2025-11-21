/* 
--------------------------------------------------------------------------------
SQL Script: OCC Buyers – Profile, Proximity, Tenure & Activity Segmentation
--------------------------------------------------------------------------------
Objective:
    Produce a complete customer-level profile for Online Cooking Class (OCC) buyers:
        • Identify OCC buyers and summarize their OCC sales, units, and orders.
        • Compare OCC behavior vs total purchase behavior.
        • Classify customers into New vs Existing and Active / Lapsed / Deep Lapsed.
        • Add proximity attributes (near store, near culinary store).
        • Rank customers by OCC sales and compute share metrics.

    Results feed CRM initiatives to evaluate:
        • OCC customer value and retention opportunity.
        • Cross-category engagement and purchase diversity.
        • High-value OCC prospects for reactivation or loyalty campaigns.

Definition:
    - OCC Orders:
        • Orders with OCC subcategory pattern match (e.g., '%999%').
        • Valid sales only (purchase_order = 'Y', net_amount > 0, not canceled).
        • Includes Cooking School and Home Goods amounts.

    - OCC Customers:
        • Customers appearing in at least one valid OCC order.
        • Used as the base population for all subsequent metrics.

    - All Orders for OCC Customers:
        • Pulls every order (any category) for customers who purchased OCC.
        • Allows comparison of OCC behavior to total spend and category mix.

    - Customer Attributes:
        • Email, original_entered_date, closest store, miles to store.
        • Proximity flags:
              – NEAR STORE vs NOT NEAR STORE
              – CULINARY vs NO CULINARY (closest store)
        • First/last order dates and all-order metrics.

    - Category Mix:
        • Distinct categories purchased and category-level spend.
        • Breaks out Cooking School vs Non-Cooking-School sales.

    - Tenure Classification:
        • New: first OCC order date matches original_entered_date.
        • Existing: all others.

    - Activity Segments:
        • ACTIVE: last gap between orders ≤ 365 days.
        • LAPSED: gap 366–730 days.
        • DEEP LAPSED: gap > 730 days.

    - Ranking Metrics:
        • OCC sales rank, dense rank, row number.
        • Share of OCC sales and cumulative OCC sales share.

Scope:
    - OCC customers in the designated subcategory pattern.
    - Includes all their orders, regardless of product category.
    - Customer-level output: one row per OCC buyer with full profile.

Processing Steps:
    1. Identify valid OCC orders for the analysis subcategory.
    2. Extract distinct OCC customers as the base cohort.
    3. Pull all valid orders (any category) for these customers.
    4. Join to customer dimension to assign store & culinary proximity.
    5. Aggregate OCC-only metrics per customer.
    6. Aggregate total-order metrics per customer.
    7. Build category mix and diversification metrics.
    8. Determine New vs Existing tenure using first OCC order date.
    9. Compute prior-order gaps with window functions for ACTIVE/LAPSED labels.
   10. Combine all customer attributes into full customer profiles.
   11. Apply window functions to compute OCC ranking and share KPIs.
   12. Return final customer-level table ordered by OCC sales descending.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH
-- 1) Parameters
params AS (
    SELECT '%999%'::VARCHAR AS occ_subcat_pattern
),

-- 2) OCC orders (Online Cooking Classes) at header level
occ_orders AS (
    SELECT
        oh.order_number,
        oh.target_key,
        oh.order_date,
        oh.order_year,
        oh.channel_code_desc,
        oh.net_amount,
        oh.net_quantity,
        oh.cooking_school_quantity,
        oh.cooking_school_amount,
        (oh.net_amount - COALESCE(oh.cooking_school_amount,0)) AS hg_amount
    FROM ims.order_header oh
    CROSS JOIN params p
    WHERE oh.sub_categories_purchased LIKE p.occ_subcat_pattern
      AND oh.purchase_order = 'Y'
      AND oh.target_key     <> -1
      AND oh.net_amount     > 0
      AND oh.cancel_quantity = '0'
),

-- 3) Distinct OCC customers
occ_customers AS (
    SELECT DISTINCT
        o.target_key
    FROM occ_orders o
),

-- 4) All valid orders (any category) for OCC customers
all_orders_for_occ AS (
    SELECT
        oh.order_number,
        oh.target_key,
        oh.order_date,
        oh.order_year,
        oh.channel_code_desc,
        oh.net_amount,
        oh.net_quantity,
        oh.cooking_school_quantity,
        oh.cooking_school_amount,
        (oh.net_amount - COALESCE(oh.cooking_school_amount,0)) AS hg_amount
    FROM ims.order_header oh
    JOIN occ_customers oc
      ON oh.target_key = oc.target_key
    WHERE oh.purchase_order = 'Y'
      AND oh.target_key     <> -1
      AND oh.net_amount     > 0
      AND oh.cancel_quantity = '0'
),

-- 5) Customer dimension + store proximity + culinary proximity
customer_dim AS (
    SELECT
        t.target_key,
        t.email,
        t.original_entered_date,
        t.miles_to_closest_store,
        t.closest_store_id,
        CASE
            WHEN t.miles_to_closest_store < 25 THEN 'NEAR STORE'
            ELSE 'NOT NEAR STORE'
        END AS store_proximity,
        s.store_id,
        s.culinary,
        CASE
            WHEN s.culinary = 'Y' THEN 'CULINARY'
            ELSE 'NO CULINARY'
        END AS culinary_proximity
    FROM ims.target      t
    JOIN ims.store_master s
      ON t.closest_store_id = s.store_id
    JOIN occ_customers oc
      ON t.target_key = oc.target_key
),

-- 6) Category-level view of “what else they buy” (excluding pure OCC-only filter)
order_lines AS (
    SELECT
        ao.order_number,
        ao.target_key,
        ao.order_date,
        ao.channel_code_desc,
        ao.net_amount       AS order_amount,
        ao.net_quantity     AS order_quantity,
        p.category_name,
        ol.net_amount       AS category_amount,
        ol.net_quantity     AS category_quantity
    FROM all_orders_for_occ ao
    JOIN ims.order_line    ol ON ao.order_number = ol.order_number
    JOIN ims.product       p  ON ol.item_number  = p.sku
),

-- 7) Customer-level aggregation of OCC-specific metrics
occ_customer_metrics AS (
    SELECT
        o.target_key,
        COUNT(DISTINCT o.order_number)               AS occ_orders,
        SUM(o.net_amount)                            AS occ_sales,
        SUM(o.net_quantity)                          AS occ_units,
        SUM(o.cooking_school_quantity)               AS occ_class_units,
        SUM(o.cooking_school_amount)                 AS occ_class_sales,
        SUM(o.hg_amount)                             AS occ_hg_amount,
        MIN(o.order_date)                            AS first_occ_order_date,
        MAX(o.order_date)                            AS last_occ_order_date
    FROM occ_orders o
    GROUP BY o.target_key
),

-- 8) Customer-level aggregation of ALL orders (for OCC customers)
all_customer_metrics AS (
    SELECT
        ao.target_key,
        COUNT(DISTINCT ao.order_number)              AS total_orders,
        SUM(ao.net_amount)                           AS total_sales,
        SUM(ao.net_quantity)                         AS total_units,
        SUM(ao.cooking_school_quantity)              AS total_class_units,
        SUM(ao.cooking_school_amount)                AS total_class_sales,
        SUM(ao.hg_amount)                            AS total_hg_amount,
        MIN(ao.order_date)                           AS first_order_date,
        MAX(ao.order_date)                           AS last_order_date
    FROM all_orders_for_occ ao
    GROUP BY ao.target_key
),

-- 9) Category mix for OCC customers (how diversified their purchases are)
customer_category_mix AS (
    SELECT
        ol.target_key,
        COUNT(DISTINCT ol.category_name)                               AS distinct_categories,
        SUM(ol.category_amount)                                        AS category_sales,
        SUM(CASE WHEN ol.category_name = 'COOKING SCHOOL'
                 THEN ol.category_amount ELSE 0 END)                   AS cs_category_sales,
        SUM(CASE WHEN ol.category_name <> 'COOKING SCHOOL'
                 THEN ol.category_amount ELSE 0 END)                   AS non_cs_category_sales
    FROM order_lines ol
    GROUP BY ol.target_key
),

-- 10) New vs Existing tenure (based on first OCC order vs original_entered_date)
customer_tenure AS (
    SELECT
        ocm.target_key,
        CASE
            WHEN cd.original_entered_date = ocm.first_occ_order_date
                 THEN 'New'
            ELSE 'Existing'
        END AS customer_tenure
    FROM occ_customer_metrics ocm
    JOIN customer_dim        cd
      ON ocm.target_key = cd.target_key
),

-- 11) Activity segment: ACTIVE / LAPSED / DEEP LAPSED based on recency
--     Use last OCC order and the previous order date via window LAG
recency_calc AS (
    SELECT
        ao.target_key,
        ao.order_date,
        LAG(ao.order_date) OVER (
            PARTITION BY ao.target_key
            ORDER BY ao.order_date
        ) AS prev_order_date
    FROM all_orders_for_occ ao
),
customer_recency AS (
    -- Pick the last OCC order per customer and compute days since prior order
    SELECT
        rc.target_key,
        MAX(rc.order_date)                                            AS last_order_date,
        MAX(rc.prev_order_date) KEEP (
            DENSE_RANK LAST ORDER BY rc.order_date
        )                                                             AS prev_order_date,
        ( MAX(rc.order_date)
          - MAX(rc.prev_order_date) KEEP (
                DENSE_RANK LAST ORDER BY rc.order_date
            )
        )                                                             AS days_since_prev,
        CASE
            WHEN ( MAX(rc.order_date)
                   - MAX(rc.prev_order_date) KEEP (
                        DENSE_RANK LAST ORDER BY rc.order_date
                     ) ) BETWEEN 1 AND 365
                THEN 'ACTIVE'
            WHEN ( MAX(rc.order_date)
                   - MAX(rc.prev_order_date) KEEP (
                        DENSE_RANK LAST ORDER BY rc.order_date
                     ) ) BETWEEN 366 AND 730
                THEN 'LAPSED'
            WHEN ( MAX(rc.order_date)
                   - MAX(rc.prev_order_date) KEEP (
                        DENSE_RANK LAST ORDER BY rc.order_date
                     ) ) > 730
                THEN 'DEEP LAPSED'
            ELSE '(null)'
        END AS buying_segment
    FROM recency_calc rc
    GROUP BY rc.target_key
),

-- 12) Combine all customer-level attributes
customer_profile AS (
    SELECT
        oc.target_key,
        cd.email,
        cd.store_proximity,
        cd.culinary_proximity,
        ocm.occ_orders,
        ocm.occ_sales,
        ocm.occ_units,
        ocm.occ_class_units,
        ocm.occ_class_sales,
        ocm.occ_hg_amount,
        acm.total_orders,
        acm.total_sales,
        acm.total_units,
        acm.total_class_units,
        acm.total_class_sales,
        acm.total_hg_amount,
        ccm.distinct_categories,
        ccm.category_sales,
        ccm.cs_category_sales,
        ccm.non_cs_category_sales,
        ct.customer_tenure,
        cr.buying_segment,
        acm.first_order_date,
        acm.last_order_date,
        ocm.first_occ_order_date,
        ocm.last_occ_order_date
    FROM occ_customers        oc
    LEFT JOIN customer_dim        cd  ON oc.target_key = cd.target_key
    LEFT JOIN occ_customer_metrics ocm ON oc.target_key = ocm.target_key
    LEFT JOIN all_customer_metrics acm ON oc.target_key = acm.target_key
    LEFT JOIN customer_category_mix ccm ON oc.target_key = ccm.target_key
    LEFT JOIN customer_tenure      ct  ON oc.target_key = ct.target_key
    LEFT JOIN customer_recency     cr  ON oc.target_key = cr.target_key
),

-- 13) Add ranking and share metrics across OCC customers
customer_ranked AS (
    SELECT
        cp.*,
        ROW_NUMBER() OVER (ORDER BY cp.occ_sales DESC)        AS occ_sales_rownum,
        RANK()       OVER (ORDER BY cp.occ_sales DESC)        AS occ_sales_rank,
        DENSE_RANK() OVER (ORDER BY cp.occ_sales DESC)        AS occ_sales_dense_rank,
        cp.occ_sales * 1.0
            / NULLIF(SUM(cp.occ_sales) OVER (), 0)            AS occ_sales_share,
        SUM(cp.occ_sales) OVER (
            ORDER BY cp.occ_sales DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / NULLIF(SUM(cp.occ_sales) OVER (), 0)              AS occ_cum_sales_share
    FROM customer_profile cp
)

-- 14) Final result: OCC buyers with full profile
SELECT
    target_key,
    email,
    store_proximity,
    culinary_proximity,
    customer_tenure,
    buying_segment,
    occ_orders,
    occ_sales,
    occ_units,
    occ_class_units,
    occ_class_sales,
    occ_hg_amount,
    total_orders,
    total_sales,
    total_units,
    total_class_units,
    total_class_sales,
    total_hg_amount,
    distinct_categories,
    category_sales,
    cs_category_sales,
    non_cs_category_sales,
    first_order_date,
    last_order_date,
    first_occ_order_date,
    last_occ_order_date,
    occ_sales_rownum,
    occ_sales_rank,
    occ_sales_dense_rank,
    occ_sales_share,
    occ_cum_sales_share
FROM customer_ranked
ORDER BY occ_sales DESC;
