/* 
--------------------------------------------------------------------------------
SQL Script: Sales by Discount Code
--------------------------------------------------------------------------------
Objective:
    Calculate Delivered Net Sales by Product × Discount Code × Sub-Discount Code
    for This Year (TY) and Last Year (LY), integrating Delivered Sales,
    Delivered Returns, and POS discount details, filtered to Retail stores and
    valid Merchandising departments.

Definition:
    - Delivered Sales:
        • Pulled from delivered_sales and joined to sales_line for channel/store
        • Provides sales dollars, units, cost, and GM at order-line level
        • Aggregated by product_analytics_id × date × location

    - Delivered Returns:
        • Includes DC remap (904 → 2) for Amazon/Web/SFS-related channels
        • Includes OROMS, BORIS, and BOPISRO returns
        • Joined back to original OROMS orders via order_line and original order

    - Discount-Level Data:
        • From pos_discounts per order_line_analytics_id
        • Includes discount_code and subdiscount_code
        • Creates a unique_factor using ROW_NUMBER() to avoid double-counting
        • Computes delivered sales and returns at discount granularity

    - Net Delivered Metrics:
        • FULL OUTER JOIN of Sales and Returns (emulated via UNION ALL)
        • Net Dollars = Sales $ − Return $
        • Net GM = Sales GM − Return GM
        • Net Discount = Sales Discount − Return Discount

    - TY vs LY Alignment:
        • TY uses calendar.date_analytics_id filtered to dates ≤ 15-Nov-2025
        • LY uses calendar.last_year_date_analytics_id for 1-year alignment
        • Both filtered to same Merchandising departments and Retail channel

Processing Steps:
    1. Determine anchor fiscal year using (CURRENT_DATE − 7 days).
    2. Build base Delivered Sales and Delivered Returns at order-line level.
    3. FULL OUTER JOIN Sales and Returns to compute base Net Delivered metrics.
    4. Build discount-level Sales and Returns via pos_discounts with window
       functions (ROW_NUMBER()) to ensure unique per-line discount attribution.
    5. FULL OUTER JOIN discount sales/returns to compute net discount metrics.
    6. Aggregate TY by product × discount × subdiscount.
    7. Aggregate LY using last_year_date_analytics_id for aligned fiscal dates.
    8. FULL OUTER JOIN TY and LY results.
    9. Join Merchandising taxonomy and product attributes.
   10. Add analytic window metric: discount share of department TY net sales.

Scope:
    - Retail stores only (channel_analytics_id = '1').
    - Level-3 Merchandising departments listed in the IN(...) filter.
    - Includes all discount codes and sub-discounts from POS systems.
    - TY and LY data up to 15-Nov-2025 or the specified cutoff date.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/


WITH
-- 1) Anchor fiscal year 
anchor_year AS (
    SELECT
        c.fiscal_year
    FROM common.calendar c
    WHERE c.gregorian_date = CURRENT_DATE - INTERVAL 7 DAY
    LIMIT 1
),

-- 2) Base SALES and RETURNS (no discounts) at order_line × product × store × day
base_grouped_sales AS (
    SELECT
        s.order_analytics_id,
        s.order_line_analytics_id,
        s.date_analytics_id,
        DATE(s.dt)                                AS gregorian_date,
        COALESCE(l.sales_channel, 'pos')          AS sales_channel,
        s.product_analytics_id,
        l.attribution_location_analytics_id,
        SUM(s.merchandise)                        AS delivered_sales_dollars,
        SUM(s.quantity)                           AS delivered_sales_quantity,
        SUM(COALESCE(s.fair_market_value, 0))     AS delivered_sales_cost,
        SUM(s.merchandise - COALESCE(s.fair_market_value, 0))
                                                 AS delivered_sales_gm_dollars
    FROM peep.delivered_sales s
    LEFT JOIN peep.sales_line l
      ON l.order_line_analytics_id = s.order_line_analytics_id
    GROUP BY
        s.order_analytics_id,
        s.order_line_analytics_id,
        s.date_analytics_id,
        DATE(s.dt),
        COALESCE(l.sales_channel, 'pos'),
        s.product_analytics_id,
        l.attribution_location_analytics_id
),

base_grouped_returns AS (
    /* Returns to DC (remap 904) + BORIS/BOPISRO */
    SELECT
        r.order_analytics_id,
        r.order_line_analytics_id,
        r.date_analytics_id,
        DATE(r.dt)                                AS gregorian_date,
        CASE
            WHEN r.location_analytics_id = 904
             AND line.sales_channel IN ('amazon_pickup','amazon_delivery','slt_bopis','walmart_go_local')
                THEN 2
            WHEN r.location_analytics_id = 904
             AND line.sales_channel IN ('web','customer_service','amazon_marketplace','culinary_orders')
                THEN 2
            ELSE r.location_analytics_id
        END                                      AS attribution_location_analytics_id,
        COALESCE(line.sales_channel, 'pos')      AS sales_channel,
        r.product_analytics_id,
        SUM(r.merchandise)                       AS delivered_returns_dollars,
        SUM(r.merchandise - COALESCE(r.fair_market_value, 0))
                                                 AS delivered_returns_gm_dollars,
        SUM(r.quantity)                          AS delivered_returns_quantity,
        SUM(COALESCE(r.fair_market_value, 0))    AS delivered_returns_cost
    FROM peep.delivered_returns r
    JOIN peep.sales_line line
      ON r.order_line_analytics_id = line.order_line_analytics_id
    JOIN peep.sales_header h
      ON line.order_analytics_id = h.order_analytics_id
    JOIN common.calendar c
      ON c.date_analytics_id = r.date_analytics_id
    WHERE line.source = 'oroms'
    GROUP BY
        r.order_analytics_id,
        r.order_line_analytics_id,
        r.date_analytics_id,
        DATE(r.dt),
        attribution_location_analytics_id,
        COALESCE(line.sales_channel, 'pos'),
        r.product_analytics_id

    UNION ALL

    SELECT
        r.order_analytics_id,
        r.order_line_analytics_id,
        r.date_analytics_id,
        DATE(r.dt)                                AS gregorian_date,
        r.location_analytics_id                   AS attribution_location_analytics_id,
        COALESCE(oroms_line.sales_channel, 'pos') AS sales_channel,
        r.product_analytics_id,
        SUM(r.merchandise)                        AS delivered_returns_dollars,
        SUM(r.merchandise - COALESCE(r.fair_market_value, 0))
                                                 AS delivered_returns_gm_dollars,
        SUM(r.quantity)                           AS delivered_returns_quantity,
        SUM(COALESCE(r.fair_market_value, 0))     AS delivered_returns_cost
    FROM peep.delivered_returns r
    JOIN peep.sales_line line
      ON r.order_line_analytics_id = line.order_line_analytics_id
    JOIN peep.sales_header rh
      ON line.order_analytics_id = rh.order_analytics_id
    JOIN common.calendar c
      ON c.date_analytics_id = r.date_analytics_id
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
      ON oroms_line.order_analytics_id = oroms_h.order_analytics_id
     AND oroms_line.product_analytics_id = r.product_analytics_id
    WHERE line.source = 'xcenter'
    GROUP BY
        r.order_analytics_id,
        r.order_line_analytics_id,
        r.date_analytics_id,
        DATE(r.dt),
        r.location_analytics_id,
        COALESCE(oroms_line.sales_channel, 'pos'),
        r.product_analytics_id
),

/* FULL OUTER JOIN emulation: sales ⟷ returns (base) */
base_net AS (
    SELECT
        COALESCE(s.order_analytics_id, r.order_analytics_id)                     AS order_analytics_id,
        COALESCE(s.order_line_analytics_id, r.order_line_analytics_id)           AS order_line_analytics_id,
        COALESCE(s.date_analytics_id, r.date_analytics_id)                       AS date_analytics_id,
        COALESCE(s.gregorian_date, r.gregorian_date)                             AS gregorian_date,
        COALESCE(s.product_analytics_id, r.product_analytics_id)                 AS product_analytics_id,
        COALESCE(s.attribution_location_analytics_id, r.attribution_location_analytics_id)
                                                                                AS attribution_location_analytics_id,
        COALESCE(s.sales_channel, r.sales_channel)                               AS sales_channel,
        COALESCE(s.delivered_sales_quantity, 0)                                  AS delivered_sales_units,
        COALESCE(r.delivered_returns_quantity, 0)                                AS delivered_return_units,
        COALESCE(s.delivered_sales_quantity, 0) - COALESCE(r.delivered_returns_quantity, 0)
                                                                                AS delivered_net_units,
        COALESCE(s.delivered_sales_dollars, 0)                                   AS delivered_sales_dollars,
        COALESCE(r.delivered_returns_dollars, 0)                                 AS delivered_return_dollars,
        COALESCE(s.delivered_sales_dollars, 0) - COALESCE(r.delivered_returns_dollars, 0)
                                                                                AS delivered_net_dollars,
        COALESCE(s.delivered_sales_gm_dollars, 0)                                AS delivered_sales_fmv_gm_dollars,
        COALESCE(r.delivered_returns_gm_dollars, 0)                              AS delivered_return_fmv_gm_dollars,
        COALESCE(s.delivered_sales_gm_dollars, 0) - COALESCE(r.delivered_returns_gm_dollars, 0)
                                                                                AS delivered_net_fmv_gm_dollars,
        COALESCE(s.delivered_sales_cost, 0) - COALESCE(r.delivered_returns_cost, 0)
                                                                                AS delivered_net_fmv_cost_dollars,
        COALESCE(s.delivered_sales_cost, 0)                                      AS delivered_sales_fmv_cost_dollars
    FROM base_grouped_sales s
    LEFT JOIN base_grouped_returns r
      ON  r.date_analytics_id              = s.date_analytics_id
      AND r.product_analytics_id           = s.product_analytics_id
      AND r.order_line_analytics_id        = s.order_line_analytics_id
      AND r.order_analytics_id             = s.order_analytics_id
      AND r.attribution_location_analytics_id = s.attribution_location_analytics_id

    UNION ALL

    SELECT
        r.order_analytics_id,
        r.order_line_analytics_id,
        r.date_analytics_id,
        r.gregorian_date,
        r.product_analytics_id,
        r.attribution_location_analytics_id,
        r.sales_channel,
        0                                    AS delivered_sales_units,
        r.delivered_returns_quantity         AS delivered_return_units,
        0 - r.delivered_returns_quantity     AS delivered_net_units,
        0                                    AS delivered_sales_dollars,
        r.delivered_returns_dollars          AS delivered_return_dollars,
        0 - r.delivered_returns_dollars      AS delivered_net_dollars,
        0                                    AS delivered_sales_fmv_gm_dollars,
        r.delivered_returns_gm_dollars       AS delivered_return_fmv_gm_dollars,
        0 - r.delivered_returns_gm_dollars   AS delivered_net_fmv_gm_dollars,
        0 - r.delivered_returns_cost         AS delivered_net_fmv_cost_dollars,
        0                                    AS delivered_sales_fmv_cost_dollars
    FROM base_grouped_returns r
    LEFT JOIN base_grouped_sales s
      ON  s.date_analytics_id              = r.date_analytics_id
      AND s.product_analytics_id           = r.product_analytics_id
      AND s.order_line_analytics_id        = r.order_line_analytics_id
      AND s.order_analytics_id             = r.order_analytics_id
      AND s.attribution_location_analytics_id = r.attribution_location_analytics_id
    WHERE s.date_analytics_id IS NULL
),

-- 3) Discount-level SALES and RETURNS (POS_DISCOUNTS) with unique_factor
disc_sales AS (
    SELECT
        s.date_analytics_id,
        l.attribution_location_analytics_id,
        s.product_analytics_id,
        s.order_line_analytics_id,
        s.order_analytics_id,
        COALESCE(dis.discount_code, 'No Discount')     AS discount_code,
        COALESCE(dis.subdiscount_code, 'No Sub-Code')  AS subdiscount_code,
        COALESCE(l.sales_channel, 'pos')               AS sales_channel,
        'Sale'                                         AS reason,
        'Sale'                                         AS reason_code,
        SUM(s.merchandise)                             AS delivered_sales_dollars,
        SUM(s.quantity)                                AS delivered_sales_quantity,
        SUM(s.merchandise - COALESCE(s.fair_market_value, 0))
                                                      AS delivered_sales_gm_dollars,
        SUM(COALESCE(s.fair_market_value, 0))          AS delivered_sales_cost_dollars,
        SUM(dis.discount_amount)                       AS delivered_sales_discount_dollars,
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY s.date_analytics_id,
                             l.attribution_location_analytics_id,
                             s.product_analytics_id,
                             l.sales_channel,
                             s.order_line_analytics_id
                ORDER BY dis.discount_code, dis.subdiscount_code
            ) = 1 THEN 1
            ELSE 0
        END                                            AS unique_factor
    FROM peep.delivered_sales s
    LEFT JOIN peep.sales_line l
      ON l.order_line_analytics_id = s.order_line_analytics_id
    LEFT JOIN peep.pos_discounts dis
      ON dis.order_line_analytics_id = s.order_line_analytics_id
    GROUP BY
        s.date_analytics_id,
        l.attribution_location_analytics_id,
        s.product_analytics_id,
        dis.discount_code,
        dis.subdiscount_code,
        l.sales_channel,
        s.order_line_analytics_id,
        s.order_analytics_id,
        reason,
        reason_code
),

disc_returns AS (
    SELECT
        r.date_analytics_id,
        CASE
            WHEN r.location_analytics_id = 904
             AND line.sales_channel IN ('amazon_pickup','amazon_delivery','slt_bopis','walmart_go_local')
                THEN 2
            WHEN r.location_analytics_id = 904
             AND line.sales_channel IN ('web','customer_service','amazon_marketplace','culinary_orders')
                THEN 2
            ELSE r.location_analytics_id
        END                                           AS attribution_location_analytics_id,
        r.product_analytics_id,
        r.order_line_analytics_id,
        r.order_analytics_id,
        COALESCE(dis.discount_code, 'No Discount')    AS discount_code,
        COALESCE(dis.subdiscount_code, 'No Sub-Code') AS subdiscount_code,
        COALESCE(line.sales_channel, 'pos')           AS sales_channel,
        reason_code,
        reason,
        SUM(r.merchandise)                            AS delivered_return_dollars,
        SUM(r.quantity)                               AS delivered_return_quantity,
        SUM(r.merchandise - COALESCE(r.fair_market_value, 0))
                                                     AS delivered_return_gm_dollars,
        SUM(COALESCE(r.fair_market_value, 0))         AS delivered_return_cost_dollars,
        SUM(dis.discount_amount)                      AS delivered_return_discount_dollars
    FROM peep.delivered_returns r
    JOIN peep.sales_line line
      ON r.order_line_analytics_id = line.order_line_analytics_id
    JOIN peep.sales_header h
      ON line.order_analytics_id = h.order_analytics_id
    LEFT JOIN peep.pos_discounts dis
      ON dis.order_line_analytics_id = r.order_line_analytics_id
    WHERE line.source = 'oroms'
    GROUP BY
        r.date_analytics_id,
        attribution_location_analytics_id,
        r.product_analytics_id,
        r.order_line_analytics_id,
        r.order_analytics_id,
        COALESCE(dis.discount_code, 'No Discount'),
        COALESCE(dis.subdiscount_code, 'No Sub-Code'),
        COALESCE(line.sales_channel, 'pos'),
        reason_code,
        reason

    UNION ALL

    SELECT
        r.date_analytics_id,
        r.location_analytics_id                       AS attribution_location_analytics_id,
        r.product_analytics_id,
        r.order_line_analytics_id,
        r.order_analytics_id,
        COALESCE(dis.discount_code, 'No Discount')    AS discount_code,
        COALESCE(dis.subdiscount_code, 'No Sub-Code') AS subdiscount_code,
        COALESCE(line.sales_channel, 'pos')           AS sales_channel,
        reason_code,
        reason,
        SUM(r.merchandise)                            AS delivered_return_dollars,
        SUM(r.quantity)                               AS delivered_return_quantity,
        SUM(r.merchandise - COALESCE(r.fair_market_value, 0))
                                                     AS delivered_return_gm_dollars,
        SUM(COALESCE(r.fair_market_value, 0))         AS delivered_return_cost_dollars,
        SUM(dis.discount_amount)                      AS delivered_return_discount_dollars
    FROM peep.delivered_returns r
    JOIN peep.sales_line line
      ON r.order_line_analytics_id = line.order_line_analytics_id
    JOIN peep.sales_header rh
      ON line.order_analytics_id = rh.order_analytics_id
    LEFT JOIN peep.pos_discounts dis
      ON dis.order_line_analytics_id = r.order_line_analytics_id
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
      ON oroms_line.order_analytics_id = oroms_h.order_analytics_id
     AND oroms_line.product_analytics_id = r.product_analytics_id
    WHERE line.source = 'xcenter'
    GROUP BY
        r.date_analytics_id,
        r.location_analytics_id,
        r.product_analytics_id,
        r.order_line_analytics_id,
        r.order_analytics_id,
        COALESCE(dis.discount_code, 'No Discount'),
        COALESCE(dis.subdiscount_code, 'No Sub-Code'),
        COALESCE(line.sales_channel, 'pos'),
        reason_code,
        reason
),

disc_net AS (
    SELECT
        COALESCE(s.date_analytics_id, r.date_analytics_id)         AS date_analytics_id,
        COALESCE(s.product_analytics_id, r.product_analytics_id)   AS product_analytics_id,
        COALESCE(s.attribution_location_analytics_id, r.attribution_location_analytics_id)
                                                                   AS attribution_location_analytics_id,
        COALESCE(s.discount_code, r.discount_code)                 AS discount_code,
        COALESCE(s.subdiscount_code, r.subdiscount_code)           AS subdiscount_code,
        COALESCE(s.reason_code, r.reason_code)                     AS reason_code,
        COALESCE(s.reason, r.reason)                               AS reason,
        COALESCE(s.sales_channel, r.sales_channel)                 AS sales_channel,
        COALESCE(s.order_line_analytics_id, r.order_line_analytics_id)
                                                                   AS order_line_analytics_id,
        COALESCE(s.order_analytics_id, r.order_analytics_id)       AS order_analytics_id,
        COALESCE(s.delivered_sales_quantity * s.unique_factor, 0)  AS delivered_sales_units,
        COALESCE(r.delivered_return_quantity, 0)                   AS delivered_return_units,
        COALESCE(s.delivered_sales_quantity * s.unique_factor, 0)
          - COALESCE(r.delivered_return_quantity, 0)               AS delivered_net_units,
        COALESCE(s.delivered_sales_dollars * s.unique_factor, 0)   AS delivered_sales_dollars,
        COALESCE(r.delivered_return_dollars, 0)                    AS delivered_return_dollars,
        COALESCE(s.delivered_sales_dollars * s.unique_factor, 0)
          - COALESCE(r.delivered_return_dollars, 0)                AS delivered_net_dollars,
        COALESCE(s.delivered_sales_gm_dollars * s.unique_factor, 0)
                                                                   AS delivered_sales_fmv_gm_dollars,
        COALESCE(r.delivered_return_gm_dollars, 0)                 AS delivered_return_fmv_gm_dollars,
        COALESCE(s.delivered_sales_gm_dollars * s.unique_factor, 0)
          - COALESCE(r.delivered_return_gm_dollars, 0)             AS delivered_net_fmv_gm_dollars,
        COALESCE(s.delivered_sales_cost_dollars * s.unique_factor, 0)
          - COALESCE(r.delivered_return_cost_dollars, 0)           AS delivered_net_fmv_cost_dollars,
        COALESCE(s.delivered_sales_discount_dollars, 0)
          - COALESCE(r.delivered_return_discount_dollars, 0)       AS delivered_net_discount_dollars,
        COALESCE(s.delivered_sales_discount_dollars, 0)            AS delivered_gross_discount_dollars
    FROM disc_sales s
    LEFT JOIN disc_returns r
      ON  r.date_analytics_id                 = s.date_analytics_id
      AND r.product_analytics_id              = s.product_analytics_id
      AND r.attribution_location_analytics_id = s.attribution_location_analytics_id
      AND r.discount_code                     = s.discount_code
      AND r.subdiscount_code                  = s.subdiscount_code
      AND r.sales_channel                     = s.sales_channel
      AND r.order_line_analytics_id           = s.order_line_analytics_id

    UNION ALL

    SELECT
        r.date_analytics_id,
        r.product_analytics_id,
        r.attribution_location_analytics_id,
        r.discount_code,
        r.subdiscount_code,
        r.reason_code,
        r.reason,
        r.sales_channel,
        r.order_line_analytics_id,
        r.order_analytics_id,
        0                                                       AS delivered_sales_units,
        r.delivered_return_quantity                             AS delivered_return_units,
        0 - r.delivered_return_quantity                         AS delivered_net_units,
        0                                                       AS delivered_sales_dollars,
        r.delivered_return_dollars                              AS delivered_return_dollars,
        0 - r.delivered_return_dollars                          AS delivered_net_dollars,
        0                                                       AS delivered_sales_fmv_gm_dollars,
        r.delivered_return_gm_dollars                           AS delivered_return_fmv_gm_dollars,
        0 - r.delivered_return_gm_dollars                       AS delivered_net_fmv_gm_dollars,
        0 - r.delivered_return_cost_dollars                     AS delivered_net_fmv_cost_dollars,
        0 - r.delivered_return_discount_dollars                 AS delivered_net_discount_dollars,
        0                                                       AS delivered_gross_discount_dollars
    FROM disc_returns r
    LEFT JOIN disc_sales s
      ON  s.date_analytics_id                 = r.date_analytics_id
      AND s.product_analytics_id              = r.product_analytics_id
      AND s.attribution_location_analytics_id = r.attribution_location_analytics_id
      AND s.discount_code                     = r.discount_code
      AND s.subdiscount_code                  = r.subdiscount_code
      AND s.sales_channel                     = r.sales_channel
      AND s.order_line_analytics_id           = r.order_line_analytics_id
    WHERE s.date_analytics_id IS NULL
),

-- 4) TY aggregation by product × discount × subdiscount
ty_data AS (
    SELECT
        b.product_analytics_id,
        d.subdiscount_code,
        d.discount_code,
        SUM(b.delivered_net_dollars) AS WJXBFS1
    FROM base_net b
    JOIN disc_net d
      ON  d.attribution_location_analytics_id = b.attribution_location_analytics_id
      AND d.date_analytics_id                 = b.date_analytics_id
      AND d.order_line_analytics_id           = b.order_line_analytics_id
      AND d.product_analytics_id              = b.product_analytics_id
      AND COALESCE(d.sales_channel,'pos')     = COALESCE(b.sales_channel,'pos')
    JOIN common.calendar c
      ON c.date_analytics_id = b.date_analytics_id
    JOIN anchor_year ay
      ON c.fiscal_year = ay.fiscal_year
    JOIN peep.locations loc
      ON loc.location_analytics_id = b.attribution_location_analytics_id
    JOIN peep.product_to_merchandising_taxonomy mt
      ON mt.product_analytics_id = b.product_analytics_id
    WHERE DATE(c.gregorian_date) <= DATE('2025-11-15')
      AND mt.level_3_analytics_id IN (500004,500005,250003,3,500006,250004,
                                      500007,250005,500008,500010,6,250007,
                                      500012,8)
      AND loc.channel_analytics_id = '1'
    GROUP BY
        b.product_analytics_id,
        d.subdiscount_code,
        d.discount_code
),

-- 5) LY aggregation – same logic but via last_year_date_analytics_id
ly_data AS (
    SELECT
        b.product_analytics_id,
        d.subdiscount_code,
        d.discount_code,
        SUM(b.delivered_net_dollars) AS WJXBFS1
    FROM base_net b
    JOIN disc_net d
      ON  d.attribution_location_analytics_id = b.attribution_location_analytics_id
      AND d.date_analytics_id                 = b.date_analytics_id
      AND d.order_line_analytics_id           = b.order_line_analytics_id
      AND d.product_analytics_id              = b.product_analytics_id
      AND COALESCE(d.sales_channel,'pos')     = COALESCE(b.sales_channel,'pos')
    JOIN common.calendar c_ly
      ON c_ly.last_year_date_analytics_id = b.date_analytics_id
    JOIN anchor_year ay
      ON c_ly.fiscal_year = ay.fiscal_year
    JOIN peep.locations loc
      ON loc.location_analytics_id = b.attribution_location_analytics_id
    JOIN peep.product_to_merchandising_taxonomy mt
      ON mt.product_analytics_id = b.product_analytics_id
    WHERE DATE(c_ly.gregorian_date) <= DATE('2025-11-15')
      AND mt.level_3_analytics_id IN (500004,500005,250003,3,500006,250004,
                                      500007,250005,500008,500010,6,250007,
                                      500012,8)
      AND loc.channel_analytics_id = '1'
    GROUP BY
        b.product_analytics_id,
        d.subdiscount_code,
        d.discount_code
),

-- 6) FULL OUTER JOIN TY vs LY by product × discount × subdiscount
ty_ly_combined AS (
    SELECT
        COALESCE(ty.product_analytics_id, ly.product_analytics_id) AS product_analytics_id,
        COALESCE(ty.discount_code, ly.discount_code)               AS discount_code,
        COALESCE(ty.subdiscount_code, ly.subdiscount_code)         AS subdiscount_code,
        ty.WJXBFS1                                                 AS WJXBFS1, -- TY
        ly.WJXBFS1                                                 AS WJXBFS2  -- LY
    FROM ty_data ty
    LEFT JOIN ly_data ly
      ON  ly.product_analytics_id = ty.product_analytics_id
      AND ly.discount_code        = ty.discount_code
      AND ly.subdiscount_code     = ty.subdiscount_code

    UNION ALL

    SELECT
        ly.product_analytics_id,
        ly.discount_code,
        ly.subdiscount_code,
        NULL             AS WJXBFS1,
        ly.WJXBFS1       AS WJXBFS2
    FROM ly_data ly
    LEFT JOIN ty_data ty
      ON  ty.product_analytics_id = ly.product_analytics_id
      AND ty.discount_code        = ly.discount_code
      AND ty.subdiscount_code     = ly.subdiscount_code
    WHERE ty.product_analytics_id IS NULL
)

-- FINAL SELECT 
SELECT
    mt.level_3_analytics_id                         AS level_3_analytics_id,
    dept.department_name                            AS level_3_name,
    dept.department_code                            AS taxonomy_code,
    t.product_analytics_id,
    p.product_name,
    t.discount_code,
    t.subdiscount_code,
    t.WJXBFS1,                                      -- Delivered Stores Netsales $s (TY)
    t.WJXBFS2,                                      -- Delivered Stores Netsales $s LY
    /* extra complex metric: discount share of dept net sales TY */
    t.WJXBFS1
      / NULLIF(SUM(t.WJXBFS1) OVER (PARTITION BY mt.level_3_analytics_id), 0)
                                                    AS discount_share_in_dept_ty
FROM ty_ly_combined t
JOIN peep.product_to_merchandising_taxonomy mt
  ON t.product_analytics_id = mt.product_analytics_id
JOIN peep.products p
  ON p.product_analytics_id = mt.product_analytics_id
JOIN (
    SELECT
        taxonomy_analytics_id AS department_id,
        parent_taxonomy_analytics_id AS division_id,
        name AS department_name,
        CAST(taxonomy_code AS UNSIGNED) AS department_code
    FROM peep.merchandising_taxonomies
    WHERE level = 3
) dept
  ON mt.level_3_analytics_id = dept.department_id
ORDER BY
    mt.level_3_analytics_id,
    dept.department_code,
    t.product_analytics_id,
    t.discount_code,
    t.subdiscount_code;
