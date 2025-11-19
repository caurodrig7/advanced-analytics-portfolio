/* 
--------------------------------------------------------------------------------
SQL Script: Stores Margin Report (TY Week, COSA-Adjusted)
--------------------------------------------------------------------------------
Objective:
    Produce a store-level profitability report for the current fiscal week,
    combining Delivered Sales, Delivered Returns, and COSA-adjusted product
    costs to compute accurate Net Sales and Net Gross Margin for Retail stores.

Definition:
    - Delivered Sales:
        • Merchandise delivered to customers (units, dollars, landed cost, GM)
        • Attributed to stores using sales_line.attribution_location_analytics_id

    - Delivered Returns:
        • Customer returns from delivered_returns, mapped back to stores
        • Returns to DC (location 904) re-attributed when linked to Amazon,
          Web, Customer Service, Oroms, or BOPIS channels
        • Includes BORIS, BOPISRO, and in-store returns

    - COSA Cost Adjustment:
        • Product-level cost adjustments stored in peep.cosa
        • Applied to each product/date/store combination
        • COSA reduces GM when a cost adjustment exists (COSA_net_GM = Net Sales – COSA)

SQL Steps:
    1. Determine the relevant fiscal year/week from CURRENT_DATE − 7.
    2. Build aggregated Delivered Sales and Delivered Returns datasets.
    3. FULL OUTER JOIN sales and returns using UNION logic.
    4. Filter for the fiscal week and valid departments; compute:
         • Net Dollars (Sales − Returns)
         • Net GM   (Sales GM − Returns GM)
    5. Build COSA-adjusted product-level dataset for Sales and Returns.
    6. FULL OUTER JOIN COSA with sales/returns to compute:
         • COSA Net Cost
         • COSA Net GM
    7. Aggregate COSA Net GM by store for the TY week.
    8. Merge store TY net metrics with COSA metrics.
    9. Join store attributes (Region, District, Region Manager, Win-Store flag).
   10. Add contextual analytics using window functions:
         • Region-level GM and Net Sales totals
         • Store ranking within each region

Scope:
    - Retail stores only.
    - Includes COSA adjustments, Delivered Sales, and Delivered Returns.
    - Outputs: Net Sales $, Net GM $, COSA-Adjusted GM $, and regional rollups.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH
-- 1) Anchor fiscal context
anchor_calendar AS (
    SELECT
        c.fiscal_year,
        c.fiscal_month_id,
        c.fiscal_week_id,
        MOD(c.fiscal_week_id, 100) AS week_in_year,
        c.gregorian_date
    FROM common.calendar c
    WHERE c.gregorian_date = CURRENT_DATE - INTERVAL 7 DAY
    LIMIT 1
),

-- 2) THIS YEAR week calendar window
ty_week_calendar AS (
    SELECT c.*
    FROM common.calendar c
    JOIN anchor_calendar a
      ON c.fiscal_year = a.fiscal_year
    WHERE c.gregorian_date BETWEEN '2025-11-09' AND '2025-11-15'
      AND c.gregorian_date <= '2025-11-15'
),

-- 3) LAST YEAR week window (kept for symmetry / future use)
ly_week_calendar AS (
    SELECT c.*
    FROM common.calendar c
    JOIN anchor_calendar a
      ON c.fiscal_year = a.fiscal_year
    WHERE c.gregorian_date BETWEEN '2025-11-09' AND '2025-11-15'
      AND c.gregorian_date <= '2025-11-15'
),

-- Delivered Sales (grouped)
grouped_sales AS (
    SELECT
        s.date_analytics_id,
        DATE(s.dt) AS gregorian_date,
        p.vendor_analytics_id,
        ph.level_3_analytics_id,
        sl.attribution_location_analytics_id,
        COALESCE(sl.sales_channel, 'pos') AS sales_channel,
        SUM(s.merchandise)                    AS delivered_sales_dollars,
        SUM(s.quantity)                       AS delivered_sales_quantity,
        SUM(COALESCE(s.fair_market_value,0))  AS delivered_sales_cost,
        SUM(s.merchandise - COALESCE(s.fair_market_value,0))
                                                AS delivered_sales_gm_dollars
    FROM peep.delivered_sales s
    LEFT JOIN peep.sales_line sl
      ON sl.order_line_analytics_id = s.order_line_analytics_id
    LEFT JOIN peep.product_to_merchandising_taxonomy ph
      ON s.product_analytics_id = ph.product_analytics_id
    LEFT JOIN peep.products p
      ON p.product_analytics_id = s.product_analytics_id
    GROUP BY
        s.date_analytics_id,
        DATE(s.dt),
        p.vendor_analytics_id,
        ph.level_3_analytics_id,
        sl.attribution_location_analytics_id,
        COALESCE(sl.sales_channel, 'pos')
),

-- Delivered Returns (grouped)
grouped_returns AS (
    /* Returns to DC with remap */
    SELECT
        r.date_analytics_id,
        DATE(r.dt) AS gregorian_date,
        ph.level_3_analytics_id,
        p.vendor_analytics_id,
        CASE
            WHEN r.location_analytics_id = 904
                 AND sl.sales_channel IN ('amazon_pickup','amazon_delivery','slt_bopis','walmart_go_local')
                THEN 2
            WHEN r.location_analytics_id = 904
                 AND sl.sales_channel IN ('web','customer_service','amazon_marketplace','culinary_orders')
                THEN 2
            ELSE r.location_analytics_id
        END AS attribution_location_analytics_id,
        COALESCE(sl.sales_channel,'pos') AS sales_channel,
        SUM(r.merchandise)                   AS delivered_returns_dollars,
        SUM(r.quantity)                      AS delivered_returns_quantity,
        SUM(COALESCE(r.fair_market_value,0)) AS delivered_returns_cost,
        SUM(r.merchandise - COALESCE(r.fair_market_value,0))
                                               AS delivered_returns_gm_dollars
    FROM peep.delivered_returns r
    JOIN peep.sales_line sl
      ON r.order_line_analytics_id = sl.order_line_analytics_id
    JOIN peep.sales_header h
      ON sl.order_analytics_id = h.order_analytics_id
    JOIN common.calendar c
      ON c.date_analytics_id = r.date_analytics_id
    LEFT JOIN peep.product_to_merchandising_taxonomy ph
      ON r.product_analytics_id = ph.product_analytics_id
    LEFT JOIN peep.products p
      ON p.product_analytics_id = r.product_analytics_id
    WHERE sl.source = 'oroms'
    GROUP BY
        r.date_analytics_id,
        DATE(r.dt),
        ph.level_3_analytics_id,
        p.vendor_analytics_id,
        attribution_location_analytics_id,
        COALESCE(sl.sales_channel,'pos')

    UNION ALL

    /* BORIS / BOPISRO / in-store */
    SELECT
        r.date_analytics_id,
        DATE(r.dt) AS gregorian_date,
        ph.level_3_analytics_id,
        p.vendor_analytics_id,
        r.location_analytics_id AS attribution_location_analytics_id,
        COALESCE(or_sl.sales_channel,'pos') AS sales_channel,
        SUM(r.merchandise)                   AS delivered_returns_dollars,
        SUM(r.quantity)                      AS delivered_returns_quantity,
        SUM(COALESCE(r.fair_market_value,0)) AS delivered_returns_cost,
        SUM(r.merchandise - COALESCE(r.fair_market_value,0))
                                               AS delivered_returns_gm_dollars
    FROM peep.delivered_returns r
    JOIN peep.sales_line sl
      ON r.order_line_analytics_id = sl.order_line_analytics_id
    JOIN peep.sales_header rh
      ON sl.order_analytics_id = rh.order_analytics_id
    JOIN common.calendar c
      ON c.date_analytics_id = r.date_analytics_id
    LEFT JOIN peep.product_to_merchandising_taxonomy ph
      ON r.product_analytics_id = ph.product_analytics_id
    LEFT JOIN peep.products p
      ON p.product_analytics_id = r.product_analytics_id
    LEFT JOIN peep.sales_header or_h
      ON r.original_order_analytics_id = or_h.order_analytics_id
     AND or_h.source = 'oroms'
    LEFT JOIN (
        SELECT
            order_analytics_id,
            product_analytics_id,
            MAX(sales_channel) AS sales_channel
        FROM peep.sales_line
        WHERE source = 'oroms'
        GROUP BY order_analytics_id, product_analytics_id
    ) or_sl
      ON or_sl.order_analytics_id = or_h.order_analytics_id
     AND or_sl.product_analytics_id = r.product_analytics_id
    WHERE sl.source = 'xcenter'
    GROUP BY
        r.date_analytics_id,
        DATE(r.dt),
        ph.level_3_analytics_id,
        p.vendor_analytics_id,
        r.location_analytics_id,
        COALESCE(or_sl.sales_channel,'pos')
),

-- FULL OUTER JOIN emulation (sales ⟷ returns)
sales_returns_combined AS (
    -- Sales + (optional) Returns
    SELECT
        s.date_analytics_id,
        s.gregorian_date,
        s.level_3_analytics_id        AS department_id,
        s.vendor_analytics_id,
        s.attribution_location_analytics_id,
        s.sales_channel,
        s.delivered_sales_quantity,
        s.delivered_sales_dollars,
        s.delivered_sales_cost,
        s.delivered_sales_gm_dollars,
        COALESCE(r.delivered_returns_quantity,0)       AS delivered_returns_quantity,
        COALESCE(r.delivered_returns_dollars,0)        AS delivered_returns_dollars,
        COALESCE(r.delivered_returns_cost,0)           AS delivered_returns_cost,
        COALESCE(r.delivered_returns_gm_dollars,0)     AS delivered_returns_gm_dollars
    FROM grouped_sales s
    LEFT JOIN grouped_returns r
      ON  r.date_analytics_id                = s.date_analytics_id
      AND r.level_3_analytics_id             = s.level_3_analytics_id
      AND r.vendor_analytics_id              = s.vendor_analytics_id
      AND r.attribution_location_analytics_id= s.attribution_location_analytics_id
      AND r.sales_channel                    = s.sales_channel

    UNION ALL

    -- Returns with no Sales
    SELECT
        r.date_analytics_id,
        r.gregorian_date,
        r.level_3_analytics_id        AS department_id,
        r.vendor_analytics_id,
        r.attribution_location_analytics_id,
        r.sales_channel,
        0 AS delivered_sales_quantity,
        0 AS delivered_sales_dollars,
        0 AS delivered_sales_cost,
        0 AS delivered_sales_gm_dollars,
        r.delivered_returns_quantity,
        r.delivered_returns_dollars,
        r.delivered_returns_cost,
        r.delivered_returns_gm_dollars
    FROM grouped_returns r
    LEFT JOIN grouped_sales s
      ON  s.date_analytics_id                = r.date_analytics_id
      AND s.level_3_analytics_id             = r.level_3_analytics_id
      AND s.vendor_analytics_id              = r.vendor_analytics_id
      AND s.attribution_location_analytics_id= r.attribution_location_analytics_id
      AND s.sales_channel                    = r.sales_channel
    WHERE s.date_analytics_id IS NULL
),

-- Store TY Week Net Metrics (department filter inline)
ty_week_store AS (
    SELECT
        c.attribution_location_analytics_id        AS location_analytics_id,
        SUM(c.delivered_sales_dollars
          - c.delivered_returns_dollars)          AS net_dollars_ty_week,
        SUM(c.delivered_sales_gm_dollars
          - c.delivered_returns_gm_dollars)       AS net_gm_ty_week
    FROM sales_returns_combined c
    JOIN ty_week_calendar cal
      ON c.date_analytics_id = cal.date_analytics_id
    JOIN locations loc
      ON loc.location_analytics_id = c.attribution_location_analytics_id
    WHERE c.department_id IN (500004,500005,250003,3,500006,250004,500007,
                              250005,500008,500010,6,250007,500012,8)
      AND loc.channel_analytics_id = '1'
    GROUP BY c.attribution_location_analytics_id
),

-- COSA – Sales (product/store/date)
grouped_sales_cosa AS (
    SELECT
        s.date_analytics_id,
        DATE(s.dt)                  AS gregorian_date,
        s.product_analytics_id,
        sl.attribution_location_analytics_id,
        SUM(s.merchandise)         AS delivered_sales_dollars,
        SUM(s.fair_market_value)   AS delivered_landed_cost_dollars,
        SUM(s.merchandise - s.fair_market_value)
                                   AS delivered_landed_gm_dollars,
        SUM(s.quantity * sl.unit_last_cost)
                                   AS vendor_cost_dollars,
        SUM(s.merchandise - (s.quantity * sl.unit_last_cost))
                                   AS delivered_vendor_gm_dollars,
        SUM(s.quantity)            AS delivered_sales_quantity
    FROM peep.delivered_sales s
    LEFT JOIN peep.sales_line sl
      ON sl.order_line_analytics_id = s.order_line_analytics_id
    WHERE s.product_analytics_id IS NOT NULL
    GROUP BY
        s.date_analytics_id,
        DATE(s.dt),
        s.product_analytics_id,
        sl.attribution_location_analytics_id
),

-- COSA – Returns (product/store/date)
grouped_returns_cosa AS (
    /* Returns to DC */
    SELECT
        r.date_analytics_id,
        DATE(r.dt)                      AS gregorian_date,
        r.product_analytics_id,
        CASE
            WHEN r.location_analytics_id = 904
                 AND sl.sales_channel IN ('amazon_pickup','amazon_delivery','slt_bopis','walmart_go_local')
                THEN 2
            WHEN r.location_analytics_id = 904
                 AND sl.sales_channel IN ('web','customer_service','amazon_marketplace','culinary_orders')
                THEN 2
            ELSE r.location_analytics_id
        END                             AS attribution_location_analytics_id,
        SUM(r.merchandise)              AS delivered_returns_dollars,
        SUM(r.fair_market_value)        AS delivered_returns_landed_cost_dollars,
        SUM(r.merchandise - r.fair_market_value)
                                        AS delivered_returns_landed_gm_dollars,
        SUM(r.quantity * sl.unit_last_cost)
                                        AS vendor_returns_cost_dollars,
        SUM(r.merchandise - (r.quantity * sl.unit_last_cost))
                                        AS delivered_returns_vendor_gm_dollars,
        SUM(r.quantity)                 AS delivered_returns_quantity
    FROM peep.delivered_returns r
    JOIN peep.sales_line sl
      ON r.order_line_analytics_id = sl.order_line_analytics_id
    JOIN peep.sales_header h
      ON sl.order_analytics_id = h.order_analytics_id
    WHERE sl.source = 'oroms'
    GROUP BY
        r.date_analytics_id,
        DATE(r.dt),
        r.product_analytics_id,
        attribution_location_analytics_id

    UNION ALL

    /* BORIS / BOPISRO / in-store */
    SELECT
        r.date_analytics_id,
        DATE(r.dt)                      AS gregorian_date,
        r.product_analytics_id,
        r.location_analytics_id         AS attribution_location_analytics_id,
        SUM(r.merchandise)              AS delivered_returns_dollars,
        SUM(r.fair_market_value)        AS delivered_returns_landed_cost_dollars,
        SUM(r.merchandise - r.fair_market_value)
                                        AS delivered_returns_landed_gm_dollars,
        SUM(r.quantity * sl.unit_last_cost)
                                        AS vendor_returns_cost_dollars,
        SUM(r.merchandise - (r.quantity * sl.unit_last_cost))
                                        AS delivered_returns_vendor_gm_dollars,
        SUM(r.quantity)                 AS delivered_returns_quantity
    FROM peep.delivered_returns r
    JOIN peep.sales_line sl
      ON r.order_line_analytics_id = sl.order_line_analytics_id
    JOIN peep.sales_header rh
      ON sl.order_analytics_id = rh.order_analytics_id
    LEFT JOIN peep.sales_header or_h
      ON r.original_order_analytics_id = or_h.order_analytics_id
     AND or_h.source = 'oroms'
    LEFT JOIN (
        SELECT
            order_analytics_id,
            product_analytics_id,
            MAX(sales_channel) AS sales_channel
        FROM peep.sales_line
        WHERE source = 'oroms'
        GROUP BY order_analytics_id, product_analytics_id
    ) or_sl
      ON or_sl.order_analytics_id = or_h.order_analytics_id
     AND or_sl.product_analytics_id = r.product_analytics_id
    WHERE sl.source = 'xcenter'
    GROUP BY
        r.date_analytics_id,
        DATE(r.dt),
        r.product_analytics_id,
        r.location_analytics_id
),

-- COSA base (COSA cost at product/store/date)
cosa_base AS (
    SELECT
        c.date_analytics_id,
        DATE(c.dt)                 AS gregorian_date,
        c.location_analytics_id    AS attribution_location_analytics_id,
        c.product_analytics_id,
        c.cosa
    FROM peep.cosa c
    WHERE c.cosa <> 0
),

-- COSA – FULL OUTER JOIN emulation (sales ⟷ returns ⟷ cosa)
cosa_combined AS (
    -- Start from sales_cosa
    SELECT
        COALESCE(s.date_analytics_id, r.date_analytics_id, cb.date_analytics_id) AS date_analytics_id,
        COALESCE(s.gregorian_date, r.gregorian_date, cb.gregorian_date)          AS gregorian_date,
        COALESCE(s.product_analytics_id, r.product_analytics_id, cb.product_analytics_id)
                                                                                AS product_analytics_id,
        COALESCE(s.attribution_location_analytics_id, r.attribution_location_analytics_id,
                 cb.attribution_location_analytics_id)
                                                                                AS attribution_location_analytics_id,
        COALESCE(s.delivered_sales_dollars,0) - COALESCE(r.delivered_returns_dollars,0)
                                                                                AS delivered_net_dollars,
        COALESCE(s.delivered_landed_cost_dollars,0)
          - COALESCE(r.delivered_returns_landed_cost_dollars,0)                 AS delivered_net_landed_cost_dollars,
        COALESCE(s.delivered_landed_gm_dollars,0)
          - COALESCE(r.delivered_returns_landed_gm_dollars,0)                   AS delivered_net_landed_gm_dollars,
        COALESCE(s.vendor_cost_dollars,0)
          - COALESCE(r.vendor_returns_cost_dollars,0)                           AS delivered_net_vendor_cost_dollars,
        COALESCE(s.delivered_vendor_gm_dollars,0)
          - COALESCE(r.delivered_returns_vendor_gm_dollars,0)                   AS delivered_net_vendor_gm_dollars,
        COALESCE(cb.cosa,0)                                                     AS cosa_net_cost_dollars,
        (COALESCE(s.delivered_sales_dollars,0)
         - COALESCE(r.delivered_returns_dollars,0)
         - COALESCE(cb.cosa,0))                                                 AS cosa_net_gm_dollars,
        COALESCE(s.delivered_sales_quantity,0)
          - COALESCE(r.delivered_returns_quantity,0)                            AS delivered_net_units
    FROM grouped_sales_cosa s
    LEFT JOIN grouped_returns_cosa r
      ON  r.date_analytics_id                = s.date_analytics_id
      AND r.product_analytics_id             = s.product_analytics_id
      AND r.attribution_location_analytics_id= s.attribution_location_analytics_id
    LEFT JOIN cosa_base cb
      ON  cb.date_analytics_id               = COALESCE(s.date_analytics_id, r.date_analytics_id)
      AND cb.product_analytics_id            = COALESCE(s.product_analytics_id, r.product_analytics_id)
      AND cb.attribution_location_analytics_id
           = COALESCE(s.attribution_location_analytics_id, r.attribution_location_analytics_id)

    UNION ALL

    -- Returns_cosa that had no sales_cosa
    SELECT
        COALESCE(r.date_analytics_id, cb.date_analytics_id)       AS date_analytics_id,
        COALESCE(r.gregorian_date, cb.gregorian_date)             AS gregorian_date,
        COALESCE(r.product_analytics_id, cb.product_analytics_id) AS product_analytics_id,
        COALESCE(r.attribution_location_analytics_id,
                 cb.attribution_location_analytics_id)            AS attribution_location_analytics_id,
        - COALESCE(r.delivered_returns_dollars,0)                 AS delivered_net_dollars,
        - COALESCE(r.delivered_returns_landed_cost_dollars,0)     AS delivered_net_landed_cost_dollars,
        - COALESCE(r.delivered_returns_landed_gm_dollars,0)       AS delivered_net_landed_gm_dollars,
        - COALESCE(r.vendor_returns_cost_dollars,0)               AS delivered_net_vendor_cost_dollars,
        - COALESCE(r.delivered_returns_vendor_gm_dollars,0)       AS delivered_net_vendor_gm_dollars,
        COALESCE(cb.cosa,0)                                       AS cosa_net_cost_dollars,
        ( - COALESCE(r.delivered_returns_dollars,0)
          - COALESCE(cb.cosa,0))                                  AS cosa_net_gm_dollars,
        - COALESCE(r.delivered_returns_quantity,0)                AS delivered_net_units
    FROM grouped_returns_cosa r
    LEFT JOIN grouped_sales_cosa s
      ON  s.date_analytics_id                = r.date_analytics_id
      AND s.product_analytics_id             = r.product_analytics_id
      AND s.attribution_location_analytics_id= r.attribution_location_analytics_id
    LEFT JOIN cosa_base cb
      ON  cb.date_analytics_id               = r.date_analytics_id
      AND cb.product_analytics_id            = r.product_analytics_id
      AND cb.attribution_location_analytics_id
           = r.attribution_location_analytics_id
    WHERE s.date_analytics_id IS NULL

    UNION ALL

    -- COSA rows without sales or returns
    SELECT
        cb.date_analytics_id,
        cb.gregorian_date,
        cb.product_analytics_id,
        cb.attribution_location_analytics_id,
        0                                 AS delivered_net_dollars,
        0                                 AS delivered_net_landed_cost_dollars,
        0                                 AS delivered_net_landed_gm_dollars,
        0                                 AS delivered_net_vendor_cost_dollars,
        0                                 AS delivered_net_vendor_gm_dollars,
        cb.cosa                            AS cosa_net_cost_dollars,
        - cb.cosa                          AS cosa_net_gm_dollars,
        0                                 AS delivered_net_units
    FROM cosa_base cb
    LEFT JOIN grouped_sales_cosa s
      ON  s.date_analytics_id                = cb.date_analytics_id
      AND s.product_analytics_id             = cb.product_analytics_id
      AND s.attribution_location_analytics_id= cb.attribution_location_analytics_id
    LEFT JOIN grouped_returns_cosa r
      ON  r.date_analytics_id                = cb.date_analytics_id
      AND r.product_analytics_id             = cb.product_analytics_id
      AND r.attribution_location_analytics_id= cb.attribution_location_analytics_id
    WHERE s.date_analytics_id IS NULL
      AND r.date_analytics_id IS NULL
),

-- COSA TY Week Store Aggregation
cosa_ty_week_store AS (
    SELECT
        cc.attribution_location_analytics_id AS location_analytics_id,
        SUM(cc.cosa_net_gm_dollars)          AS cosa_net_gm_ty_week
    FROM cosa_combined cc
    JOIN ty_week_calendar cal
      ON cc.date_analytics_id = cal.date_analytics_id
    GROUP BY cc.attribution_location_analytics_id
),

-- Merge TY net metrics + COSA net GM at store level
store_week_metrics AS (
    SELECT
        COALESCE(t.location_analytics_id, c.location_analytics_id) AS location_analytics_id,
        t.net_dollars_ty_week,
        t.net_gm_ty_week,
        c.cosa_net_gm_ty_week
    FROM ty_week_store t
    LEFT JOIN cosa_ty_week_store c
      ON c.location_analytics_id = t.location_analytics_id

    UNION ALL

    SELECT
        c.location_analytics_id,
        NULL AS net_dollars_ty_week,
        NULL AS net_gm_ty_week,
        c.cosa_net_gm_ty_week
    FROM cosa_ty_week_store c
    LEFT JOIN ty_week_store t
      ON t.location_analytics_id = c.location_analytics_id
    WHERE t.location_analytics_id IS NULL
),

-- Region / District mapping
region_mapping AS (
    SELECT
        CASE 
            WHEN district_code IN ('NW','WC')          THEN 'West Coast'
            WHEN district_code IN ('TX','CHI')         THEN 'Mid-West'
            WHEN district_code IN ('FL','WDC')         THEN 'Florida'
            WHEN district_code IN ('OV','EC')          THEN 'East Coast'
            WHEN district_code = 'DC'                  THEN 'ecommerce'
            WHEN channel_code = 'warehouse'            THEN 'DC'
            ELSE 'zOther'
        END AS region,
        CASE 
            WHEN district_code IN ('NW','WC')          THEN 'Tina Spangler'
            WHEN district_code IN ('TX','CHI')         THEN 'Kimberly Taylor'
            WHEN district_code IN ('FL','WDC')         THEN 'Jose Fasenda'
            WHEN district_code IN ('OV','EC')          THEN 'Heather Dean'
            WHEN district_code = 'DC'                  THEN 'ecommerce'
            WHEN channel_code = 'warehouse'            THEN 'DC'
            ELSE 'zOther'
        END AS region_manager,
        CASE
            WHEN district_code IN ('NW','WC')          THEN 1
            WHEN district_code IN ('TX','CHI')         THEN 2
            WHEN district_code IN ('FL','WDC')         THEN 3
            WHEN district_code IN ('OV','EC')          THEN 4
            WHEN district_code = 'DC'                  THEN 5
            WHEN channel_code = 'warehouse'            THEN 6
            ELSE 7
        END AS region_order,
        COALESCE(district_code,'No Code')             AS district_code,
        CASE
            WHEN district_code = 'NW'                 THEN 'West Coast North'
            WHEN district_code = 'WC'                 THEN 'West Coast South'
            WHEN district_code = 'CHI'                THEN 'Greater Chicago'
            WHEN district_code = 'TX'                 THEN 'Texas'
            WHEN district_code = 'FL'                 THEN 'Florida'
            WHEN district_code = 'EC'                 THEN 'East Coast'
            WHEN district_code = 'WDC'                THEN 'District of Columbia'
            WHEN district_code = 'OV'                 THEN 'Ohio Valley'
            WHEN district_code = 'DC'                 THEN 'ecommerce'
            WHEN channel_code = 'warehouse'           THEN 'DC'
            ELSE 'Other'
        END AS district_name,
        CASE
            WHEN district_code = 'NW' THEN 1
            WHEN district_code = 'WC' THEN 2
            WHEN district_code = 'CHI' THEN 3
            WHEN district_code = 'TX' THEN 4
            WHEN district_code = 'FL' THEN 5
            WHEN district_code = 'EC' THEN 6
            WHEN district_code = 'OV' THEN 7
            WHEN district_code = 'WDC' THEN 8
            WHEN district_code = 'DC' THEN 9
            WHEN channel_code = 'warehouse' THEN 10
            ELSE 11
        END AS district_order,
        l.location_analytics_id,
        l.name,
        CASE 
            WHEN l.location_analytics_id IN (
                14,18,42,76,113,122,125,138,144,154,
                157,159,160,161,162,171,172,174,175
            ) THEN 'Y' ELSE 'N'
        END AS win_store
    FROM peep.locations l
)

-- final select
SELECT
    rm.region                        AS region_analytics_id,
    rm.region_manager,
    rm.region_order,
    rm.district_code,
    rm.district_name                 AS CustCol_25,
    swm.location_analytics_id,
    rm.name                          AS store_name,

    swm.net_dollars_ty_week          AS WJXBFS1,   -- TY net sales $
    swm.cosa_net_gm_ty_week          AS WJXBFS2,   -- COSA-adjusted TY net GM $
    swm.net_gm_ty_week               AS WJXBFS3,   -- TY net GM $

    -- Extra window-function metrics for context:
    SUM(swm.net_dollars_ty_week) OVER (PARTITION BY rm.region)
                                      AS region_net_dollars_ty_week,
    SUM(swm.net_gm_ty_week)      OVER (PARTITION BY rm.region)
                                      AS region_net_gm_ty_week,
    RANK() OVER (PARTITION BY rm.region ORDER BY swm.net_gm_ty_week DESC)
                                      AS store_rank_by_gm_in_region
FROM store_week_metrics swm
JOIN region_mapping rm
  ON rm.location_analytics_id = swm.location_analytics_id
ORDER BY
    rm.region_order,
    rm.district_order,
    rm.name;
