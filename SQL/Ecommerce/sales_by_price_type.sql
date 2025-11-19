/* 
--------------------------------------------------------------------------------
SQL Script: Sales by Price Type
--------------------------------------------------------------------------------
Objective:
    Produce Delivered Net Sales and Delivered Net GM by Price Type, Merch Group,
    Department, and Channel, for a selected fiscal week. The query separates
    Regular Price, Promotional Sale, Markdown Sale, and Other price types, and
    integrates Delivered Sales, Delivered Returns, and Product Price History to
    produce accurate net performance metrics.

Definition:
    - Delivered Sales:
        • Derived from delivered_sales joined to sales_line and product_price_history
        • Includes merchandise $, GM$, Units, and Cost per product × price type
        • Classified into price_type (regular_price, promotional_sale, markdown_sale, etc.)

    - Delivered Returns:
        • Includes DC remap (904 → 2) for Amazon/Web/SFS-related channels
        • Includes OROMS, BORIS, BOPISRO, and in-store return events
        • Returns inherit price_type from product_price_history on return date
        • Aggregated at product × price_type × location × channel

    - Net Delivered Metrics:
        • FULL OUTER JOIN emulated via UNION ALL of sales and returns
        • Net Units   = Sales Units − Return Units
        • Net $       = Sales $ − Return $
        • Net GM $    = Sales GM $ − Return GM $
        • Net Cost $  = Sales Cost $ − Return Cost $

    - Price Type Normalization:
        • Missing price types are assigned “other”
        • Window function ROW_NUMBER() identifies last transaction per price type

Processing Steps:
    1. Aggregate Delivered Sales by price_type using CTE grouped_sales.
    2. Aggregate Delivered Returns by price_type using CTE grouped_returns.
    3. FULL OUTER JOIN Sales and Returns into price_type_net.
    4. Normalize price types and add ROW_NUMBER() (price_type_enriched).
    5. Join to Locations, Product Taxonomy, Calendar, and Merch Group mapping.
    6. Filter to desired fiscal week and valid merchandising departments.
    7. Aggregate by price_type × merch_group × department × channel.
    8. Add advanced window-function KPIs:
        • Total netsales across entire report
        • Netsales share within merch group
        • Netsales share within department
        • Netsales GM% (GM$ / Net$)

Scope:
    - Includes Retail and Ecommerce store channels (channel_analytics_id IN ('1','2')).
    - Filters to Level-3 departments defined in the merchandising taxonomy list.
    - Fiscal week selectable via calendar.fiscal_week_id.
    - Includes all price types: regular, promotional, markdown, marked_out_of_stock, other.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH
-- 1) Grouped Delivered Sales by Price Type
grouped_sales AS (
    SELECT
        s.date_analytics_id,
        DATE(s.dt)                         AS gregorian_date,
        COALESCE(l.sales_channel, 'pos')   AS sales_channel,
        COALESCE(pph.price_type, 'other')  AS price_type,
        s.product_analytics_id,
        l.attribution_location_analytics_id,
        SUM(s.merchandise)                                     AS delivered_sales_dollars,
        SUM(s.quantity)                                        AS delivered_sales_quantity,
        SUM(COALESCE(s.fair_market_value, 0))                  AS delivered_sales_cost,
        SUM(s.merchandise - COALESCE(s.fair_market_value, 0))  AS delivered_sales_gross_margin_dollars
    FROM peep.delivered_sales s
    LEFT JOIN peep.sales_line l
      ON l.order_line_analytics_id = s.order_line_analytics_id
    LEFT JOIN peep.product_price_history pph
      ON pph.product_analytics_id = s.product_analytics_id
     AND pph.date_analytics_id    = s.date_analytics_id
    WHERE s.date_analytics_id > 8000
    GROUP BY
        s.date_analytics_id,
        DATE(s.dt),
        COALESCE(l.sales_channel, 'pos'),
        COALESCE(pph.price_type, 'other'),
        s.product_analytics_id,
        l.attribution_location_analytics_id
),

-- 2) Grouped Delivered Returns by Price Type (DC + BORIS/BOPISRO)
grouped_returns AS (
    /* Returns to DC with remap of 904 */
    SELECT
        r.date_analytics_id,
        DATE(r.dt)                         AS gregorian_date,
        CASE
            WHEN r.location_analytics_id = 904
                 AND line.sales_channel IN ('amazon_pickup','amazon_delivery','slt_bopis','walmart_go_local')
                 THEN 2
            WHEN r.location_analytics_id = 904
                 AND line.sales_channel IN ('web','customer_service','amazon_marketplace','culinary_orders')
                 THEN 2
            ELSE r.location_analytics_id
        END                                AS attribution_location_analytics_id,
        COALESCE(pph.price_type, 'other')  AS price_type,
        COALESCE(line.sales_channel, 'pos')AS sales_channel,
        r.product_analytics_id,
        SUM(r.merchandise)                                     AS delivered_returns_dollars,
        SUM(r.merchandise - COALESCE(r.fair_market_value, 0))  AS delivered_returns_gross_margin_dollars,
        SUM(r.quantity)                                        AS delivered_returns_quantity,
        SUM(COALESCE(r.fair_market_value, 0))                  AS delivered_returns_cost
    FROM peep.delivered_returns r
    INNER JOIN peep.sales_line line
      ON r.order_line_analytics_id = line.order_line_analytics_id
    INNER JOIN peep.sales_header h
      ON line.order_analytics_id = h.order_analytics_id
    INNER JOIN common.calendar cal
      ON cal.date_analytics_id = r.date_analytics_id
    LEFT JOIN peep.product_price_history pph
      ON pph.product_analytics_id = r.product_analytics_id
     AND pph.date_analytics_id    = r.date_analytics_id
    WHERE line.source = 'oroms'
      AND r.date_analytics_id > 8000
    GROUP BY
        r.date_analytics_id,
        DATE(r.dt),
        attribution_location_analytics_id,
        COALESCE(pph.price_type, 'other'),
        COALESCE(line.sales_channel, 'pos'),
        r.product_analytics_id

    UNION ALL

    /* BORIS / BOPISRO / in-store returns */
    SELECT
        r.date_analytics_id,
        DATE(r.dt)                         AS gregorian_date,
        r.location_analytics_id            AS attribution_location_analytics_id,
        COALESCE(pph.price_type, 'other')  AS price_type,
        COALESCE(oroms_line.sales_channel, 'pos') AS sales_channel,
        r.product_analytics_id,
        SUM(r.merchandise)                                     AS delivered_returns_dollars,
        SUM(r.merchandise - COALESCE(r.fair_market_value, 0))  AS delivered_returns_gross_margin_dollars,
        SUM(r.quantity)                                        AS delivered_returns_quantity,
        SUM(COALESCE(r.fair_market_value, 0))                  AS delivered_returns_cost
    FROM peep.delivered_returns r
    INNER JOIN peep.sales_line line
      ON r.order_line_analytics_id = line.order_line_analytics_id
    INNER JOIN peep.sales_header rh
      ON line.order_analytics_id = rh.order_analytics_id
    INNER JOIN common.calendar cal
      ON cal.date_analytics_id = r.date_analytics_id
    LEFT JOIN peep.sales_header oroms_h
      ON r.original_order_analytics_id = oroms_h.order_analytics_id
     AND oroms_h.source = 'oroms'
    LEFT JOIN (
        SELECT
            order_analytics_id,
            product_analytics_id,
            MAX(sales_channel) AS sales_channel
        FROM peep.sales_line
        WHERE source = 'oroms'
        GROUP BY order_analytics_id, product_analytics_id
    ) oroms_line
      ON oroms_line.order_analytics_id  = oroms_h.order_analytics_id
     AND oroms_line.product_analytics_id = r.product_analytics_id
    LEFT JOIN peep.product_price_history pph
      ON pph.product_analytics_id = r.product_analytics_id
     AND pph.date_analytics_id    = r.date_analytics_id
    WHERE line.source = 'xcenter'
      AND r.date_analytics_id > 8000
    GROUP BY
        r.date_analytics_id,
        DATE(r.dt),
        r.location_analytics_id,
        COALESCE(pph.price_type, 'other'),
        COALESCE(oroms_line.sales_channel, 'pos'),
        r.product_analytics_id
),

-- 3) FULL OUTER JOIN emulation: sales ⟷ returns by price_type
price_type_net AS (
    -- Sales with (optional) returns
    SELECT
        COALESCE(s.date_analytics_id, r.date_analytics_id)           AS date_analytics_id,
        COALESCE(s.gregorian_date, r.gregorian_date)                 AS gregorian_date,
        COALESCE(s.product_analytics_id, r.product_analytics_id)     AS product_analytics_id,
        COALESCE(s.price_type, r.price_type)                         AS price_type,
        COALESCE(s.attribution_location_analytics_id,
                 r.attribution_location_analytics_id)                AS attribution_location_analytics_id,
        COALESCE(s.sales_channel, r.sales_channel)                   AS sales_channel,

        COALESCE(s.delivered_sales_quantity, 0)                      AS pt_delivered_sales_units,
        COALESCE(r.delivered_returns_quantity, 0)                    AS pt_delivered_return_units,
        COALESCE(s.delivered_sales_quantity, 0)
          - COALESCE(r.delivered_returns_quantity, 0)                AS pt_delivered_net_units,

        COALESCE(s.delivered_sales_dollars, 0)                       AS pt_delivered_sales_dollars,
        COALESCE(r.delivered_returns_dollars, 0)                     AS pt_delivered_return_dollars,
        COALESCE(s.delivered_sales_dollars, 0)
          - COALESCE(r.delivered_returns_dollars, 0)                 AS pt_delivered_net_dollars,

        COALESCE(s.delivered_sales_gross_margin_dollars, 0)          AS pt_delivered_sales_FMV_gm_dollars,
        COALESCE(r.delivered_returns_gross_margin_dollars, 0)        AS pt_delivered_return_FMV_gm_dollars,
        COALESCE(s.delivered_sales_gross_margin_dollars, 0)
          - COALESCE(r.delivered_returns_gross_margin_dollars, 0)    AS pt_delivered_net_FMV_gm_dollars,

        COALESCE(s.delivered_sales_cost, 0)
          - COALESCE(r.delivered_returns_cost, 0)                    AS pt_delivered_net_FMV_cost_dollars,
        COALESCE(s.delivered_sales_cost, 0)                          AS pt_delivered_sales_FMV_cost_dollars
    FROM grouped_sales s
    LEFT JOIN grouped_returns r
      ON  s.date_analytics_id               = r.date_analytics_id
      AND s.product_analytics_id            = r.product_analytics_id
      AND s.sales_channel                   = r.sales_channel
      AND s.price_type                      = r.price_type
      AND s.attribution_location_analytics_id = r.attribution_location_analytics_id

    UNION ALL

    -- Returns with no matching sales
    SELECT
        r.date_analytics_id,
        r.gregorian_date,
        r.product_analytics_id,
        r.price_type,
        r.attribution_location_analytics_id,
        r.sales_channel,

        0                                       AS pt_delivered_sales_units,
        r.delivered_returns_quantity            AS pt_delivered_return_units,
        0 - r.delivered_returns_quantity        AS pt_delivered_net_units,

        0                                       AS pt_delivered_sales_dollars,
        r.delivered_returns_dollars             AS pt_delivered_return_dollars,
        0 - r.delivered_returns_dollars         AS pt_delivered_net_dollars,

        0                                       AS pt_delivered_sales_FMV_gm_dollars,
        r.delivered_returns_gross_margin_dollars AS pt_delivered_return_FMV_gm_dollars,
        0 - r.delivered_returns_gross_margin_dollars AS pt_delivered_net_FMV_gm_dollars,

        0 - r.delivered_returns_cost            AS pt_delivered_net_FMV_cost_dollars,
        0                                       AS pt_delivered_sales_FMV_cost_dollars
    FROM grouped_returns r
    LEFT JOIN grouped_sales s
      ON  s.date_analytics_id               = r.date_analytics_id
      AND s.product_analytics_id            = r.product_analytics_id
      AND s.sales_channel                   = r.sales_channel
      AND s.price_type                      = r.price_type
      AND s.attribution_location_analytics_id = r.attribution_location_analytics_id
    WHERE s.date_analytics_id IS NULL
),

/* Normalize price_type and add a row_number for “last transaction per price_type” */
price_type_enriched AS (
    SELECT
        pn.*,
        CASE
            WHEN pn.price_type IS NULL THEN 'other'
            ELSE pn.price_type
        END AS price_type_norm,
        ROW_NUMBER() OVER (
            PARTITION BY pn.price_type
            ORDER BY pn.gregorian_date DESC, pn.product_analytics_id
        ) AS rn_last_txn_in_price_type
    FROM price_type_net pn
),

-- 4) Join to Locations, Taxonomy, Calendar, Merch Group
joined_data AS (
    SELECT
        e.date_analytics_id,
        e.gregorian_date,
        e.product_analytics_id,
        e.price_type_norm             AS price_type,
        e.attribution_location_analytics_id,
        e.sales_channel,
        e.pt_delivered_net_dollars,
        e.pt_delivered_net_FMV_gm_dollars,
        l.channel_analytics_id        AS channel,
        t.level_3_analytics_id,
        cal.fiscal_week_id,
        mg.merch_group,
        dept.department_name          AS level_3_name,
        dept.department_code          AS taxonomy_code
    FROM price_type_enriched e
    JOIN locations l
      ON e.attribution_location_analytics_id = l.location_analytics_id
    JOIN product_to_merchandising_taxonomy t
      ON e.product_analytics_id = t.product_analytics_id
    JOIN calendar cal
      ON e.date_analytics_id = cal.date_analytics_id
    JOIN (
        SELECT
            taxonomy_analytics_id AS department_id,
            CASE
                WHEN taxonomy_analytics_id IN (500007,500010,250007,500012,8)
                    THEN 'Entertaining'
                WHEN taxonomy_analytics_id IN (500005,250003,3,500006,250004,
                                               250005,6,500008,4,500004)
                    THEN 'Kitchen'
                ELSE 'Other'
            END AS merch_group
        FROM peep.merchandising_taxonomies
        WHERE level = 3
    ) mg
      ON t.level_3_analytics_id = mg.department_id
    JOIN (
        SELECT
            taxonomy_analytics_id        AS department_id,
            parent_taxonomy_analytics_id AS division_id,
            name                         AS department_name,
            CAST(taxonomy_code AS SIGNED) AS department_code
        FROM peep.merchandising_taxonomies
        WHERE level = 3
    ) dept
      ON t.level_3_analytics_id = dept.department_id
    WHERE cal.fiscal_week_id IN (202313)
      AND t.level_3_analytics_id IN
          (500004,500005,250003,3,500006,250004,500007,250005,
           500008,500010,6,250007,500012,8)
      AND l.channel_analytics_id IN ('1','2')
),

-- 5) Aggregate by Price Type / Merch Group / Department / Channel
agg_by_price_type AS (
    SELECT
        price_type,
        MAX(
            CASE
                WHEN price_type = 'regular_price'    THEN 1
                WHEN price_type = 'promotional_sale' THEN 2
                WHEN price_type = 'markdown_sale'    THEN 3
                WHEN price_type = 'marked_out_of_stock' THEN 4
                WHEN price_type = 'other'            THEN 5
                ELSE 6
            END
        )                                         AS CustCol_35,
        merch_group,
        level_3_analytics_id,
        MAX(level_3_name)                         AS level_3_name,
        MAX(taxonomy_code)                        AS taxonomy_code,
        channel                                   AS channel,
        SUM(pt_delivered_net_dollars)            AS WJXBFS1,
        SUM(pt_delivered_net_FMV_gm_dollars)     AS WJXBFS2
    FROM joined_data
    GROUP BY
        price_type,
        merch_group,
        level_3_analytics_id,
        channel
),

-- 6) Add window-function KPIs (share of total, GM %) – “as complex as possible”
agg_with_windows AS (
    SELECT
        a.*,
        -- Total Net Sales (PT) across the whole report
        SUM(a.WJXBFS1) OVER ()                               AS total_net_sales_pt,

        -- Net Sales share within Merch Group
        a.WJXBFS1
          / NULLIF(SUM(a.WJXBFS1) OVER (PARTITION BY a.merch_group), 0)
                                                              AS pct_of_merch_group_net_sales,

        -- Net Sales share within Department
        a.WJXBFS1
          / NULLIF(SUM(a.WJXBFS1) OVER (PARTITION BY a.level_3_analytics_id), 0)
                                                              AS pct_of_dept_net_sales,

        -- Netsales GM % (LC/PT analog)
        a.WJXBFS2 / NULLIF(a.WJXBFS1, 0)                      AS netsales_gm_pct
    FROM agg_by_price_type a
)

-- FINAL SELECT
SELECT
    price_type,
    CustCol_35,
    merch_group,
    level_3_analytics_id,
    level_3_name,
    taxonomy_code,
    channel,
    WJXBFS1,                     -- Netsales $s (PT)
    WJXBFS2,                     -- Netsales GM $s (LC/PT)
    total_net_sales_pt,
    pct_of_merch_group_net_sales,
    pct_of_dept_net_sales,
    netsales_gm_pct
FROM agg_with_windows
ORDER BY
    CustCol_35,
    merch_group,
    level_3_analytics_id,
    channel,
    price_type;
