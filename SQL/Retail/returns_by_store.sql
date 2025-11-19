/* 
--------------------------------------------------------------------------------
SQL Script: Returns by Store – TY vs LY (Retail Only)
--------------------------------------------------------------------------------
Objective:
    Generate store-level Delivered Sales and Delivered Returns for:
        • Fiscal Week-to-Date (This Year vs Last Year)
        • Month-to-Date (This Year vs Last Year)
        • Year-to-Date (This Year vs Last Year)

Definition:
    - Delivered Sales:
        • Merchandise delivered to customers (units, dollars, cost, margin)
        • Attributed to stores using the sales_line attribution store

    - Delivered Returns:
        • Customer returns in delivered_returns, mapped back to stores
        • Returns to DC (location 904) are re-attributed when the original
          order came from Amazon, Web, Oroms, Customer Service, or similar channels
        • Includes BORIS, BOPISRO, and in-store return events

    - TY vs LY Logic:
        • “This Year” metrics use calendar.date_analytics_id
        • “Last Year” metrics use calendar.last_year_date_analytics_id
        • Filters follow the CURRENT_DATE − 7 fiscal alignment window

Scope:
    - Retail stores only (channel_analytics_id = '1').
    - Includes only selected departments (Cookware, Cutlery, Electrics, etc.).
    - Aggregated at: Store × Date × Department × Vendor × Channel.
    - Separate temporary tables compute WEEK, MTD, and YTD for TY and LY.

Author: Cesar Rodriguez
--------------------------------------------------------------------------------
*/

CREATE OR REPLACE VIEW vw_delivered_sales_returns AS
WITH
-- --------------------
-- Delivered Sales (grouped)
-- --------------------
grouped_sales AS (
    SELECT
        s.date_analytics_id,
        DATE(s.dt) AS gregorian_date,
        p.vendor_analytics_id,
        ph.level_3_analytics_id,
        l.attribution_location_analytics_id,
        COALESCE(l.sales_channel, 'pos') AS sales_channel,
        SUM(s.merchandise)                              AS delivered_sales_dollars,
        SUM(s.quantity)                                 AS delivered_sales_quantity,
        SUM(COALESCE(s.fair_market_value, 0))           AS delivered_sales_cost,
        SUM(s.merchandise - COALESCE(s.fair_market_value, 0))
                                                        AS delivered_sales_gross_margin_dollars
    FROM `peep`.`delivered_sales` s
    LEFT JOIN `peep`.`sales_line` l
        ON l.order_line_analytics_id = s.order_line_analytics_id
    LEFT JOIN `peep`.`product_to_merchandising_taxonomy` ph
        ON s.product_analytics_id = ph.product_analytics_id
    LEFT JOIN `peep`.`products` p
        ON p.product_analytics_id = s.product_analytics_id
    GROUP BY
        s.date_analytics_id,
        DATE(s.dt),
        p.vendor_analytics_id,
        ph.level_3_analytics_id,
        l.attribution_location_analytics_id,
        COALESCE(l.sales_channel, 'pos')
),

-- --------------------
-- Delivered Returns (grouped)
-- --------------------
grouped_returns AS (
    /* Returns to DC (location 904) with remapped attribution */
    SELECT
        r.date_analytics_id,
        DATE(r.dt) AS gregorian_date,
        ph.level_3_analytics_id,
        p.vendor_analytics_id,
        CASE
            WHEN r.location_analytics_id = 904
                 AND sl.sales_channel IN ('amazon_pickup', 'amazon_delivery', 'slt_bopis', 'walmart_go_local')
                THEN 2
            WHEN r.location_analytics_id = 904
                 AND sl.sales_channel IN ('web','customer_service', 'amazon_marketplace', 'culinary_orders')
                THEN 2
            ELSE r.location_analytics_id
        END AS attribution_location_analytics_id,
        COALESCE(sl.sales_channel, 'pos') AS sales_channel,
        SUM(r.merchandise)                            AS delivered_returns_dollars,
        SUM(r.quantity)                               AS delivered_returns_quantity,
        SUM(COALESCE(r.fair_market_value, 0))         AS delivered_returns_cost,
        SUM(r.merchandise - COALESCE(r.fair_market_value, 0))
                                                     AS delivered_returns_gross_margin_dollars
    FROM `peep`.`delivered_returns` r
    INNER JOIN `peep`.`sales_line` sl
        ON r.order_line_analytics_id = sl.order_line_analytics_id
    INNER JOIN `peep`.`sales_header` h
        ON sl.order_analytics_id = h.order_analytics_id
    INNER JOIN `common`.`calendar` c
        ON c.date_analytics_id = r.date_analytics_id
    LEFT JOIN `peep`.`product_to_merchandising_taxonomy` ph
        ON r.product_analytics_id = ph.product_analytics_id
    LEFT JOIN `peep`.`products` p
        ON p.product_analytics_id = r.product_analytics_id
    WHERE sl.source = 'oroms'
    GROUP BY
        r.date_analytics_id,
        DATE(r.dt),
        ph.level_3_analytics_id,
        p.vendor_analytics_id,
        attribution_location_analytics_id,
        COALESCE(sl.sales_channel, 'pos')

    UNION ALL

    /* BORIS, BOPISRO, in-store returns (xcenter) */
    SELECT
        r.date_analytics_id,
        DATE(r.dt) AS gregorian_date,
        ph.level_3_analytics_id,
        p.vendor_analytics_id,
        r.location_analytics_id AS attribution_location_analytics_id,
        COALESCE(orl.sales_channel, 'pos') AS sales_channel,
        SUM(r.merchandise)                            AS delivered_returns_dollars,
        SUM(r.quantity)                               AS delivered_returns_quantity,
        SUM(COALESCE(r.fair_market_value, 0))         AS delivered_returns_cost,
        SUM(r.merchandise - COALESCE(r.fair_market_value, 0))
                                                     AS delivered_returns_gross_margin_dollars
    FROM `peep`.`delivered_returns` r
    INNER JOIN `peep`.`sales_line` sl
        ON r.order_line_analytics_id = sl.order_line_analytics_id
    INNER JOIN `peep`.`sales_header` rh
        ON sl.order_analytics_id = rh.order_analytics_id
    INNER JOIN `common`.`calendar` c
        ON c.date_analytics_id = r.date_analytics_id
    LEFT JOIN `peep`.`product_to_merchandising_taxonomy` ph
        ON r.product_analytics_id = ph.product_analytics_id
    LEFT JOIN `peep`.`products` p
        ON p.product_analytics_id = r.product_analytics_id
    LEFT JOIN `peep`.`sales_header` orh
        ON r.original_order_analytics_id = orh.order_analytics_id
       AND orh.source = 'oroms'
    LEFT JOIN (
        SELECT
            order_analytics_id,
            product_analytics_id,
            MAX(sales_channel) AS sales_channel
        FROM `peep`.`sales_line`
        WHERE source = 'oroms'
        GROUP BY order_analytics_id, product_analytics_id
    ) orl
        ON orl.order_analytics_id = orh.order_analytics_id
       AND orl.product_analytics_id = r.product_analytics_id
    WHERE sl.source = 'xcenter'
    GROUP BY
        r.date_analytics_id,
        DATE(r.dt),
        ph.level_3_analytics_id,
        p.vendor_analytics_id,
        r.location_analytics_id,
        COALESCE(orl.sales_channel, 'pos')
),

-- --------------------
-- FULL OUTER JOIN: sales ⟷ returns
-- --------------------
combined AS (
    -- Sales with matching (or null) returns
    SELECT
        s.date_analytics_id,
        s.gregorian_date,
        s.level_3_analytics_id,
        s.vendor_analytics_id,
        s.attribution_location_analytics_id,
        s.sales_channel,
        s.delivered_sales_quantity,
        s.delivered_sales_dollars,
        s.delivered_sales_cost,
        s.delivered_sales_gross_margin_dollars,
        r.delivered_returns_quantity,
        r.delivered_returns_dollars,
        r.delivered_returns_cost,
        r.delivered_returns_gross_margin_dollars
    FROM grouped_sales s
    LEFT JOIN grouped_returns r
        ON  s.date_analytics_id              = r.date_analytics_id
        AND s.level_3_analytics_id           = r.level_3_analytics_id
        AND s.vendor_analytics_id            = r.vendor_analytics_id
        AND s.attribution_location_analytics_id = r.attribution_location_analytics_id
        AND s.sales_channel                  = r.sales_channel

    UNION ALL

    -- Returns that do not have matching sales rows
    SELECT
        r.date_analytics_id,
        r.gregorian_date,
        r.level_3_analytics_id,
        r.vendor_analytics_id,
        r.attribution_location_analytics_id,
        r.sales_channel,
        NULL AS delivered_sales_quantity,
        NULL AS delivered_sales_dollars,
        NULL AS delivered_sales_cost,
        NULL AS delivered_sales_gross_margin_dollars,
        r.delivered_returns_quantity,
        r.delivered_returns_dollars,
        r.delivered_returns_cost,
        r.delivered_returns_gross_margin_dollars
    FROM grouped_returns r
    LEFT JOIN grouped_sales s
        ON  s.date_analytics_id              = r.date_analytics_id
        AND s.level_3_analytics_id           = r.level_3_analytics_id
        AND s.vendor_analytics_id            = r.vendor_analytics_id
        AND s.attribution_location_analytics_id = r.attribution_location_analytics_id
        AND s.sales_channel                  = r.sales_channel
    WHERE s.date_analytics_id IS NULL
)

SELECT
    date_analytics_id,
    gregorian_date,
    level_3_analytics_id     AS department_id,
    vendor_analytics_id,
    attribution_location_analytics_id,
    sales_channel,
    COALESCE(delivered_sales_quantity, 0)          AS delivered_sales_units,
    COALESCE(delivered_returns_quantity, 0)        AS delivered_return_units,
    COALESCE(delivered_sales_dollars, 0)           AS delivered_sales_dollars,
    COALESCE(delivered_returns_dollars, 0)         AS delivered_return_dollars,
    COALESCE(delivered_sales_dollars, 0)
      - COALESCE(delivered_returns_dollars, 0)     AS delivered_net_dollars,
    COALESCE(delivered_sales_gross_margin_dollars, 0)
      AS delivered_sales_landed_gm_dollars,
    COALESCE(delivered_returns_gross_margin_dollars, 0)
      AS delivered_return_landed_gm_dollars,
    COALESCE(delivered_sales_gross_margin_dollars, 0)
      - COALESCE(delivered_returns_gross_margin_dollars, 0)
      AS delivered_net_landed_gm_dollars,
    COALESCE(delivered_sales_cost, 0)
      - COALESCE(delivered_returns_cost, 0)
      AS delivered_net_landed_cost_dollars,
    COALESCE(delivered_sales_cost, 0)
      AS delivered_gross_landed_cost_dollars
FROM combined;
