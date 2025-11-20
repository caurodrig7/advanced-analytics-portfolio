/* 
--------------------------------------------------------------------------------
SQL Script: Delivered Netsales & Inventory – Department & Vendor Master
--------------------------------------------------------------------------------
Objective:
    Build a consolidated vendor- and department-level view of Delivered Netsales
    and Units across multiple time windows (Last Week, MTD, QTD, YTD) for both
    This Year (TY) and Last Year (LY), including COSA-adjusted GM, vendor share,
    and ranking by performance.

Definition:
    - Delivered Netsales:
        • Delivered Sales – Delivered Returns at product × location × date level.
        • Includes both landed-cost and vendor-cost perspectives.
        • COSA (Cost of Sales Allocation) applied to derive COSA Net GM.

    - Time Buckets:
        • Last Week (LW): Current fiscal week based on (CURRENT_DATE – 7).
        • Month-to-Date (MTD): Current fiscal month.
        • Quarter-to-Date (QTD): Current fiscal quarter.
        • Year-to-Date (YTD): Current fiscal year.
        • LY versions use last_year_date_analytics_id for calendar alignment.

    - Dimensions:
        • Vendor: vendor_analytics_id (later used for share and ranking).
        • Department: level_5_analytics_id (within valid Level-3 department list).
        • Channel: location.channel_analytics_id.
        • Comparable Flag: has_comparable_sales (Y/N).

    - Key Metrics by Bucket:
        • Net Dollars (TY, LY).
        • Net Landed GM $.
        • COSA Net GM $.
        • Net Units.
        • Derived GM% and vendor share for YTD.

Processing Steps:
    1. Determine anchor fiscal week/month/quarter/year from (CURRENT_DATE – 7)
       using calendar in anchor_date CTE.
    2. Build grouped Delivered Sales and Delivered Returns, including landed and
       vendor cost/GM, at product × location × date level.
    3. Join in COSA data (cost of sales allocation) and emulate a FULL OUTER JOIN
       to compute unified Net Sales metrics (netsales_base).
    4. Enrich Net Sales with calendar, locations, product, taxonomy, and vendor
       attributes, filtering to valid Level-3 departments and dates (netsales_enriched).
    5. Split into time buckets (LW, MTD, QTD, YTD) and build matching LY buckets
       using last_year_date_analytics_id.
    6. Aggregate each bucket by vendor × comparable flag × level_5_analytics_id
       × channel, computing Net Dollars, Landed GM, COSA GM, and Units.
    7. Merge all TY and LY aggregates into a single fact table (merged_periods)
       keyed by vendor, department (level_5), channel, and comparable flag.
    8. Add advanced window metrics:
        • YTD GM% = net_landed_gm_ytd / net_dollars_ytd.
        • Vendor share of YTD Net Dollars within level_5 + channel.
        • Vendor rank within level_5 + channel by YTD Net Dollars.
    9. Output the final Department & Vendor master dataset with LW, MTD, QTD,
       YTD (TY/LY) metrics, GM%, vendor share, and ranking for reporting.

Scope:
    - Includes only products in the specified Level-3 departments.
    - Includes all locations with valid channel_analytics_id and comparable flag.
    - Aligns TY and LY based on fiscal calendar for fair comparison.
    - Supports vendor scorecards, department performance reviews, planning,
      and inventory/assortment strategy.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH
-- 0) Anchor fiscal period based on (CURRENT_DATE - 7)
anchor_date AS (
    SELECT
        c.date_analytics_id,
        c.fiscal_week_id,
        c.fiscal_month_id,
        c.fiscal_quarter_id,
        c.fiscal_season_id,
        c.fiscal_year,
        MOD(c.fiscal_week_id, 100) AS week_in_year
    FROM common.calendar c
    WHERE c.gregorian_date = CURRENT_DATE - INTERVAL 7 DAY
    LIMIT 1
),

/* Valid L3 departments (same list as in original script) */
valid_depts AS (
    SELECT level_3_analytics_id
    FROM product_to_merchandising_taxonomy
    WHERE level_3_analytics_id IN (
        500004,500010,6,250007,500012,500005,8,250003,
        3,500006,250004,500007,250005,500008
    )
    GROUP BY level_3_analytics_id
),

-- 1) Base grouped SALES
grouped_sales AS (
    SELECT
        s.date_analytics_id,
        DATE(s.dt) AS gregorian_date,
        s.product_analytics_id,
        l.attribution_location_analytics_id,
        SUM(s.merchandise)                                 AS delivered_sales_dollars,
        SUM(s.fair_market_value)                           AS delivered_landed_cost_dollars,
        SUM(s.merchandise - s.fair_market_value)           AS delivered_landed_gm_dollars,
        SUM(s.quantity * l.unit_last_cost)                 AS vendor_cost_dollars,
        SUM(s.merchandise - (s.quantity * l.unit_last_cost)) AS delivered_vendor_gm_dollars,
        SUM(s.quantity)                                    AS delivered_sales_quantity
    FROM peep.delivered_sales s
    LEFT JOIN peep.sales_line l
        ON l.order_line_analytics_id = s.order_line_analytics_id
    WHERE s.product_analytics_id IS NOT NULL
    GROUP BY
        s.date_analytics_id,
        s.product_analytics_id,
        l.attribution_location_analytics_id,
        DATE(s.dt)
),

-- 2) Base grouped RETURNS (DC + BORIS/BOPISRO/in-store)
grouped_returns AS (
    -- Returns to DC (904 remap)
    SELECT
        r.date_analytics_id,
        DATE(r.dt) AS gregorian_date,
        r.product_analytics_id,
        CASE
            WHEN r.location_analytics_id = 904
                 AND sl.sales_channel IN ('amazon_pickup','amazon_delivery','slt_bopis','walmart_go_local')
            THEN 2
            WHEN r.location_analytics_id = 904
                 AND sl.sales_channel IN ('web','customer_service','amazon_marketplace','culinary_orders')
            THEN 2
            ELSE r.location_analytics_id
        END AS attribution_location_analytics_id,
        SUM(r.merchandise)                                AS delivered_returns_dollars,
        SUM(r.fair_market_value)                          AS delivered_returns_landed_cost_dollars,
        SUM(r.merchandise - r.fair_market_value)          AS delivered_returns_landed_gm_dollars,
        SUM(r.quantity * sl.unit_last_cost)               AS vendor_returns_cost_dollars,
        SUM(r.merchandise - (r.quantity * sl.unit_last_cost))
                                                          AS delivered_returns_vendor_gm_dollars,
        SUM(r.quantity)                                   AS delivered_returns_quantity
    FROM peep.delivered_returns r
    JOIN peep.sales_line sl
        ON r.order_line_analytics_id = sl.order_line_analytics_id
    JOIN peep.sales_header sh
        ON sl.order_analytics_id = sh.order_analytics_id
    WHERE sl.source = 'oroms'
    GROUP BY
        r.date_analytics_id,
        DATE(r.dt),
        r.product_analytics_id,
        attribution_location_analytics_id

    UNION ALL

    -- BORIS, BOPISRO, in-store
    SELECT
        r.date_analytics_id,
        DATE(r.dt) AS gregorian_date,
        r.product_analytics_id,
        r.location_analytics_id              AS attribution_location_analytics_id,
        SUM(r.merchandise)                   AS delivered_returns_dollars,
        SUM(r.fair_market_value)             AS delivered_returns_landed_cost_dollars,
        SUM(r.merchandise - r.fair_market_value)
                                             AS delivered_returns_landed_gm_dollars,
        SUM(r.quantity * sl.unit_last_cost)  AS vendor_returns_cost_dollars,
        SUM(r.merchandise - (r.quantity * sl.unit_last_cost))
                                             AS delivered_returns_vendor_gm_dollars,
        SUM(r.quantity)                      AS delivered_returns_quantity
    FROM peep.delivered_returns r
    JOIN peep.sales_line sl
        ON r.order_line_analytics_id = sl.order_line_analytics_id
    JOIN peep.sales_header sh_x
        ON sl.order_analytics_id = sh_x.order_analytics_id
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
    WHERE sl.source = 'xcenter'
    GROUP BY
        r.date_analytics_id,
        DATE(r.dt),
        r.product_analytics_id,
        r.location_analytics_id
),

-- 3) COSA (cost of sales allocation) – base
cosa_base AS (
    SELECT
        c.location_analytics_id          AS attribution_location_analytics_id,
        c.product_analytics_id,
        c.date_analytics_id,
        DATE(c.dt)                       AS gregorian_date,
        c.cosa
    FROM peep.cosa c
    WHERE c.cosa <> 0
),

-- 4) NET SALES BASE – FULL OUTER JOIN (sales + returns + cosa)
netsales_base AS (
    -- Left side: sales with optional returns & cosa
    SELECT
        COALESCE(s.date_analytics_id, r.date_analytics_id, cb.date_analytics_id)
            AS date_analytics_id,
        COALESCE(s.gregorian_date, r.gregorian_date, cb.gregorian_date)
            AS gregorian_date,
        COALESCE(s.product_analytics_id, r.product_analytics_id, cb.product_analytics_id)
            AS product_analytics_id,
        COALESCE(s.attribution_location_analytics_id,
                 r.attribution_location_analytics_id,
                 cb.attribution_location_analytics_id)
            AS attribution_location_analytics_id,

        COALESCE(s.delivered_sales_dollars, 0)
          - COALESCE(r.delivered_returns_dollars, 0)
            AS delivered_net_dollars,

        COALESCE(s.delivered_landed_cost_dollars, 0)
          - COALESCE(r.delivered_returns_landed_cost_dollars, 0)
            AS delivered_net_landed_cost_dollars,

        COALESCE(s.delivered_landed_gm_dollars, 0)
          - COALESCE(r.delivered_returns_landed_gm_dollars, 0)
            AS delivered_net_landed_gm_dollars,

        COALESCE(s.vendor_cost_dollars, 0)
          - COALESCE(r.vendor_returns_cost_dollars, 0)
            AS delivered_net_vendor_cost_dollars,

        COALESCE(s.delivered_vendor_gm_dollars, 0)
          - COALESCE(r.delivered_returns_vendor_gm_dollars, 0)
            AS delivered_net_vendor_gm_dollars,

        COALESCE(cb.cosa, 0)            AS cosa_delivered_net_cost_dollars,

        (COALESCE(s.delivered_sales_dollars, 0)
         - COALESCE(r.delivered_returns_dollars, 0)
         - COALESCE(cb.cosa, 0))        AS cosa_delivered_net_gm_dollars,

        COALESCE(s.delivered_sales_quantity, 0)
          - COALESCE(r.delivered_returns_quantity, 0)
            AS delivered_net_units
    FROM grouped_sales s
    LEFT JOIN grouped_returns r
      ON s.date_analytics_id = r.date_analytics_id
     AND s.product_analytics_id = r.product_analytics_id
     AND s.attribution_location_analytics_id = r.attribution_location_analytics_id
    LEFT JOIN cosa_base cb
      ON cb.date_analytics_id = COALESCE(s.date_analytics_id, r.date_analytics_id)
     AND cb.product_analytics_id = COALESCE(s.product_analytics_id, r.product_analytics_id)
     AND cb.attribution_location_analytics_id =
         COALESCE(s.attribution_location_analytics_id, r.attribution_location_analytics_id)

    UNION ALL

    -- Right anti-join: returns (or cosa) existing without sales
    SELECT
        COALESCE(r.date_analytics_id, cb.date_analytics_id)   AS date_analytics_id,
        COALESCE(r.gregorian_date, cb.gregorian_date)         AS gregorian_date,
        COALESCE(r.product_analytics_id, cb.product_analytics_id) AS product_analytics_id,
        COALESCE(r.attribution_location_analytics_id,
                 cb.attribution_location_analytics_id)        AS attribution_location_analytics_id,

        0 - COALESCE(r.delivered_returns_dollars, 0)          AS delivered_net_dollars,
        0 - COALESCE(r.delivered_returns_landed_cost_dollars, 0)
                                                              AS delivered_net_landed_cost_dollars,
        0 - COALESCE(r.delivered_returns_landed_gm_dollars, 0)
                                                              AS delivered_net_landed_gm_dollars,
        0 - COALESCE(r.vendor_returns_cost_dollars, 0)        AS delivered_net_vendor_cost_dollars,
        0 - COALESCE(r.delivered_returns_vendor_gm_dollars, 0)
                                                              AS delivered_net_vendor_gm_dollars,
        COALESCE(cb.cosa, 0)                                  AS cosa_delivered_net_cost_dollars,
        (0 - COALESCE(r.delivered_returns_dollars, 0)
           - COALESCE(cb.cosa, 0))                            AS cosa_delivered_net_gm_dollars,
        0 - COALESCE(r.delivered_returns_quantity, 0)         AS delivered_net_units
    FROM grouped_returns r
    LEFT JOIN grouped_sales s
      ON s.date_analytics_id = r.date_analytics_id
     AND s.product_analytics_id = r.product_analytics_id
     AND s.attribution_location_analytics_id = r.attribution_location_analytics_id
    LEFT JOIN cosa_base cb
      ON cb.date_analytics_id = r.date_analytics_id
     AND cb.product_analytics_id = r.product_analytics_id
     AND cb.attribution_location_analytics_id = r.attribution_location_analytics_id
    WHERE s.date_analytics_id IS NULL
),

-- 5) Netsales joined with calendar / location / product / taxonomy
      (vendor- and level-5-aware; base for all time buckets)
netsales_enriched AS (
    SELECT
        n.*,
        cal.gregorian_date,
        cal.fiscal_week_id,
        cal.fiscal_month_id,
        cal.fiscal_quarter_id,
        cal.fiscal_year,
        loc.channel_analytics_id                            AS channel,
        loc.has_comparable_sales,
        pmt.level_3_analytics_id,
        pmt.level_5_analytics_id,
        COALESCE(prod.vendor_analytics_id, 0)              AS vendor_analytics_id
    FROM netsales_base n
    JOIN calendar cal
      ON n.date_analytics_id = cal.date_analytics_id
    JOIN locations loc
      ON n.attribution_location_analytics_id = loc.location_analytics_id
    JOIN product_to_merchandising_taxonomy pmt
      ON n.product_analytics_id = pmt.product_analytics_id
    JOIN peep.products prod
      ON n.product_analytics_id = prod.product_analytics_id
    WHERE pmt.level_3_analytics_id IN (SELECT level_3_analytics_id FROM valid_depts)
      AND DATE(cal.gregorian_date) <= DATE('2025-11-15')
),

-- 6) Define PERIOD FILTERS (Last Week, MTD, QTD, YTD, and LY versions)
period_lw AS (
    SELECT DISTINCT fiscal_week_id
    FROM anchor_date
),
period_mtd AS (
    SELECT DISTINCT fiscal_month_id
    FROM anchor_date
),
period_qtd AS (
    SELECT DISTINCT fiscal_quarter_id
    FROM anchor_date
),
period_ytd AS (
    SELECT DISTINCT fiscal_year
    FROM anchor_date
),

/* Current-year buckets */
netsales_lw AS (
    SELECT *
    FROM netsales_enriched ne
    JOIN period_lw lw
      ON ne.fiscal_week_id = lw.fiscal_week_id
),
netsales_mtd AS (
    SELECT *
    FROM netsales_enriched ne
    JOIN period_mtd m
      ON ne.fiscal_month_id = m.fiscal_month_id
),
netsales_qtd AS (
    SELECT *
    FROM netsales_enriched ne
    JOIN period_qtd q
      ON ne.fiscal_quarter_id = q.fiscal_quarter_id
),
netsales_ytd AS (
    SELECT *
    FROM netsales_enriched ne
    JOIN period_ytd y
      ON ne.fiscal_year = y.fiscal_year
),

/* Last-year versions using calendar.last_year_date_analytics_id */
netsales_enriched_ly AS (
    SELECT
        n.*,
        cal.gregorian_date,
        cal.fiscal_week_id,
        cal.fiscal_month_id,
        cal.fiscal_quarter_id,
        cal.fiscal_year,
        loc.channel_analytics_id                            AS channel,
        loc.has_comparable_sales,
        pmt.level_3_analytics_id,
        pmt.level_5_analytics_id,
        COALESCE(prod.vendor_analytics_id, 0)              AS vendor_analytics_id
    FROM netsales_base n
    JOIN calendar cal
      ON n.date_analytics_id = cal.last_year_date_analytics_id
    JOIN locations loc
      ON n.attribution_location_analytics_id = loc.location_analytics_id
    JOIN product_to_merchandising_taxonomy pmt
      ON n.product_analytics_id = pmt.product_analytics_id
    JOIN peep.products prod
      ON n.product_analytics_id = prod.product_analytics_id
    WHERE pmt.level_3_analytics_id IN (SELECT level_3_analytics_id FROM valid_depts)
      AND DATE(cal.gregorian_date) <= DATE('2025-11-15')
),

netsales_lw_ly AS (
    SELECT *
    FROM netsales_enriched_ly ne
    JOIN period_lw lw
      ON ne.fiscal_week_id = lw.fiscal_week_id
),
netsales_mtd_ly AS (
    SELECT *
    FROM netsales_enriched_ly ne
    JOIN period_mtd m
      ON ne.fiscal_month_id = m.fiscal_month_id
),
netsales_qtd_ly AS (
    SELECT *
    FROM netsales_enriched_ly ne
    JOIN period_qtd q
      ON ne.fiscal_quarter_id = q.fiscal_quarter_id
),
netsales_ytd_ly AS (
    SELECT *
    FROM netsales_enriched_ly ne
    JOIN period_ytd y
      ON ne.fiscal_year = y.fiscal_year
),

-- 7) Aggregate by vendor / has_comparable / level_5 / channel for each bucket
agg_template_lw AS (
    SELECT
        vendor_analytics_id,
        CASE WHEN has_comparable_sales = 1 THEN 'Y' ELSE 'N' END AS has_comparable_sales_flag,
        level_5_analytics_id,
        channel,
        SUM(delivered_net_dollars)         AS net_dollars_lw,
        SUM(delivered_net_landed_gm_dollars) AS net_landed_gm_lw,
        SUM(cosa_delivered_net_gm_dollars) AS net_cosa_gm_lw,
        SUM(delivered_net_units)           AS net_units_lw
    FROM netsales_lw
    GROUP BY vendor_analytics_id, has_comparable_sales_flag, level_5_analytics_id, channel
),
agg_template_mtd AS (
    SELECT
        vendor_analytics_id,
        CASE WHEN has_comparable_sales = 1 THEN 'Y' ELSE 'N' END AS has_comparable_sales_flag,
        level_5_analytics_id,
        channel,
        SUM(delivered_net_dollars)         AS net_dollars_mtd,
        SUM(delivered_net_landed_gm_dollars) AS net_landed_gm_mtd,
        SUM(cosa_delivered_net_gm_dollars) AS net_cosa_gm_mtd,
        SUM(delivered_net_units)           AS net_units_mtd
    FROM netsales_mtd
    GROUP BY vendor_analytics_id, has_comparable_sales_flag, level_5_analytics_id, channel
),
agg_template_qtd AS (
    SELECT
        vendor_analytics_id,
        CASE WHEN has_comparable_sales = 1 THEN 'Y' ELSE 'N' END AS has_comparable_sales_flag,
        level_5_analytics_id,
        channel,
        SUM(delivered_net_dollars)         AS net_dollars_qtd,
        SUM(delivered_net_landed_gm_dollars) AS net_landed_gm_qtd,
        SUM(cosa_delivered_net_gm_dollars) AS net_cosa_gm_qtd,
        SUM(delivered_net_units)           AS net_units_qtd
    FROM netsales_qtd
    GROUP BY vendor_analytics_id, has_comparable_sales_flag, level_5_analytics_id, channel
),
agg_template_ytd AS (
    SELECT
        vendor_analytics_id,
        CASE WHEN has_comparable_sales = 1 THEN 'Y' ELSE 'N' END AS has_comparable_sales_flag,
        level_5_analytics_id,
        channel,
        SUM(delivered_net_dollars)         AS net_dollars_ytd,
        SUM(delivered_net_landed_gm_dollars) AS net_landed_gm_ytd,
        SUM(cosa_delivered_net_gm_dollars) AS net_cosa_gm_ytd,
        SUM(delivered_net_units)           AS net_units_ytd
    FROM netsales_ytd
    GROUP BY vendor_analytics_id, has_comparable_sales_flag, level_5_analytics_id, channel
),

/* LY aggregates (same grain) */
agg_template_mtd_ly AS (
    SELECT
        vendor_analytics_id,
        CASE WHEN has_comparable_sales = 1 THEN 'Y' ELSE 'N' END AS has_comparable_sales_flag,
        level_5_analytics_id,
        channel,
        SUM(delivered_net_dollars)         AS net_dollars_mtd_ly,
        SUM(delivered_net_landed_gm_dollars) AS net_landed_gm_mtd_ly,
        SUM(cosa_delivered_net_gm_dollars) AS net_cosa_gm_mtd_ly,
        SUM(delivered_net_units)           AS net_units_mtd_ly
    FROM netsales_mtd_ly
    GROUP BY vendor_analytics_id, has_comparable_sales_flag, level_5_analytics_id, channel
),
agg_template_qtd_ly AS (
    SELECT
        vendor_analytics_id,
        CASE WHEN has_comparable_sales = 1 THEN 'Y' ELSE 'N' END AS has_comparable_sales_flag,
        level_5_analytics_id,
        channel,
        SUM(delivered_net_dollars)         AS net_dollars_qtd_ly,
        SUM(delivered_net_landed_gm_dollars) AS net_landed_gm_qtd_ly,
        SUM(cosa_delivered_net_gm_dollars) AS net_cosa_gm_qtd_ly,
        SUM(delivered_net_units)           AS net_units_qtd_ly
    FROM netsales_qtd_ly
    GROUP BY vendor_analytics_id, has_comparable_sales_flag, level_5_analytics_id, channel
),
agg_template_ytd_ly AS (
    SELECT
        vendor_analytics_id,
        CASE WHEN has_comparable_sales = 1 THEN 'Y' ELSE 'N' END AS has_comparable_sales_flag,
        level_5_analytics_id,
        channel,
        SUM(delivered_net_dollars)         AS net_dollars_ytd_ly,
        SUM(delivered_net_landed_gm_dollars) AS net_landed_gm_ytd_ly,
        SUM(cosa_delivered_net_gm_dollars) AS net_cosa_gm_ytd_ly,
        SUM(delivered_net_units)           AS net_units_ytd_ly
    FROM netsales_ytd_ly
    GROUP BY vendor_analytics_id, has_comparable_sales_flag, level_5_analytics_id, channel
),

-- 8) Merge all aggregates into a single fact
merged_periods AS (
    SELECT
        COALESCE(a_lw.vendor_analytics_id,
                 a_mtd.vendor_analytics_id,
                 a_qtd.vendor_analytics_id,
                 a_ytd.vendor_analytics_id)                 AS vendor_analytics_id,
        COALESCE(a_lw.has_comparable_sales_flag,
                 a_mtd.has_comparable_sales_flag,
                 a_qtd.has_comparable_sales_flag,
                 a_ytd.has_comparable_sales_flag)           AS has_comparable_sales,
        COALESCE(a_lw.level_5_analytics_id,
                 a_mtd.level_5_analytics_id,
                 a_qtd.level_5_analytics_id,
                 a_ytd.level_5_analytics_id)                AS level_5_analytics_id,
        COALESCE(a_lw.channel,
                 a_mtd.channel,
                 a_qtd.channel,
                 a_ytd.channel)                             AS channel,

        -- Last Week
        a_lw.net_dollars_lw,
        a_lw.net_landed_gm_lw,
        a_lw.net_cosa_gm_lw,
        a_lw.net_units_lw,

        -- MTD (TY + LY)
        a_mtd.net_dollars_mtd,
        a_mtd.net_landed_gm_mtd,
        a_mtd.net_cosa_gm_mtd,
        a_mtd.net_units_mtd,

        a_mtd_ly.net_dollars_mtd_ly,
        a_mtd_ly.net_landed_gm_mtd_ly,
        a_mtd_ly.net_cosa_gm_mtd_ly,
        a_mtd_ly.net_units_mtd_ly,

        -- QTD (TY + LY)
        a_qtd.net_dollars_qtd,
        a_qtd.net_landed_gm_qtd,
        a_qtd.net_cosa_gm_qtd,
        a_qtd.net_units_qtd,

        a_qtd_ly.net_dollars_qtd_ly,
        a_qtd_ly.net_landed_gm_qtd_ly,
        a_qtd_ly.net_cosa_gm_qtd_ly,
        a_qtd_ly.net_units_qtd_ly,

        -- YTD (TY + LY)
        a_ytd.net_dollars_ytd,
        a_ytd.net_landed_gm_ytd,
        a_ytd.net_cosa_gm_ytd,
        a_ytd.net_units_ytd,

        a_ytd_ly.net_dollars_ytd_ly,
        a_ytd_ly.net_landed_gm_ytd_ly,
        a_ytd_ly.net_cosa_gm_ytd_ly,
        a_ytd_ly.net_units_ytd_ly
    FROM agg_template_lw a_lw
    FULL JOIN agg_template_mtd a_mtd
      ON  a_mtd.vendor_analytics_id = a_lw.vendor_analytics_id
      AND a_mtd.level_5_analytics_id = a_lw.level_5_analytics_id
      AND a_mtd.has_comparable_sales_flag = a_lw.has_comparable_sales_flag
      AND a_mtd.channel = a_lw.channel
    FULL JOIN agg_template_qtd a_qtd
      ON  a_qtd.vendor_analytics_id = COALESCE(a_lw.vendor_analytics_id, a_mtd.vendor_analytics_id)
      AND a_qtd.level_5_analytics_id = COALESCE(a_lw.level_5_analytics_id, a_mtd.level_5_analytics_id)
      AND a_qtd.has_comparable_sales_flag =
          COALESCE(a_lw.has_comparable_sales_flag, a_mtd.has_comparable_sales_flag)
      AND a_qtd.channel = COALESCE(a_lw.channel, a_mtd.channel)
    FULL JOIN agg_template_ytd a_ytd
      ON  a_ytd.vendor_analytics_id = COALESCE(a_lw.vendor_analytics_id, a_mtd.vendor_analytics_id, a_qtd.vendor_analytics_id)
      AND a_ytd.level_5_analytics_id = COALESCE(a_lw.level_5_analytics_id, a_mtd.level_5_analytics_id, a_qtd.level_5_analytics_id)
      AND a_ytd.has_comparable_sales_flag =
          COALESCE(a_lw.has_comparable_sales_flag, a_mtd.has_comparable_sales_flag, a_qtd.has_comparable_sales_flag)
      AND a_ytd.channel = COALESCE(a_lw.channel, a_mtd.channel, a_qtd.channel)

    LEFT JOIN agg_template_mtd_ly a_mtd_ly
      ON  a_mtd_ly.vendor_analytics_id = COALESCE(a_lw.vendor_analytics_id, a_mtd.vendor_analytics_id)
      AND a_mtd_ly.level_5_analytics_id = COALESCE(a_lw.level_5_analytics_id, a_mtd.level_5_analytics_id)
      AND a_mtd_ly.has_comparable_sales_flag =
          COALESCE(a_lw.has_comparable_sales_flag, a_mtd.has_comparable_sales_flag)
      AND a_mtd_ly.channel = COALESCE(a_lw.channel, a_mtd.channel)

    LEFT JOIN agg_template_qtd_ly a_qtd_ly
      ON  a_qtd_ly.vendor_analytics_id = COALESCE(a_lw.vendor_analytics_id, a_qtd.vendor_analytics_id)
      AND a_qtd_ly.level_5_analytics_id = COALESCE(a_lw.level_5_analytics_id, a_qtd.level_5_analytics_id)
      AND a_qtd_ly.has_comparable_sales_flag =
          COALESCE(a_lw.has_comparable_sales_flag, a_qtd.has_comparable_sales_flag)
      AND a_qtd_ly.channel = COALESCE(a_lw.channel, a_qtd.channel)

    LEFT JOIN agg_template_ytd_ly a_ytd_ly
      ON  a_ytd_ly.vendor_analytics_id = COALESCE(a_lw.vendor_analytics_id, a_ytd.vendor_analytics_id)
      AND a_ytd_ly.level_5_analytics_id = COALESCE(a_lw.level_5_analytics_id, a_ytd.level_5_analytics_id)
      AND a_ytd_ly.has_comparable_sales_flag =
          COALESCE(a_lw.has_comparable_sales_flag, a_ytd.has_comparable_sales_flag)
      AND a_ytd_ly.channel = COALESCE(a_lw.channel, a_ytd.channel)
),

-- 9) Add window metrics (GM%, vendor share, ranking)
final_with_windows AS (
    SELECT
        mp.*,

        -- YTD GM%
        mp.net_landed_gm_ytd / NULLIF(mp.net_dollars_ytd, 0)           AS gm_pct_ytd,

        -- Vendor share of YTD netsales within level_5 + channel
        mp.net_dollars_ytd / NULLIF(
            SUM(mp.net_dollars_ytd) OVER (
                PARTITION BY mp.level_5_analytics_id, mp.channel
            ),
            0
        ) AS vendor_share_ytd_in_dept,

        -- Rank vendor within level_5 by YTD netsales
        ROW_NUMBER() OVER (
            PARTITION BY mp.level_5_analytics_id, mp.channel
            ORDER BY mp.net_dollars_ytd DESC
        ) AS vendor_rank_ytd_in_dept
    FROM merged_periods mp
)

-- FINAL SELECT
SELECT
    vendor_analytics_id,
    has_comparable_sales                AS has_comp,
    level_5_analytics_id,
    channel,

    -- Last week
    net_dollars_lw,
    net_landed_gm_lw,
    net_cosa_gm_lw,
    net_units_lw,

    -- MTD TY & LY
    net_dollars_mtd,
    net_landed_gm_mtd,
    net_cosa_gm_mtd,
    net_units_mtd,
    net_dollars_mtd_ly,
    net_landed_gm_mtd_ly,
    net_cosa_gm_mtd_ly,
    net_units_mtd_ly,

    -- QTD TY & LY
    net_dollars_qtd,
    net_landed_gm_qtd,
    net_cosa_gm_qtd,
    net_units_qtd,
    net_dollars_qtd_ly,
    net_landed_gm_qtd_ly,
    net_cosa_gm_qtd_ly,
    net_units_qtd_ly,

    -- YTD TY & LY
    net_dollars_ytd,
    net_landed_gm_ytd,
    net_cosa_gm_ytd,
    net_units_ytd,
    net_dollars_ytd_ly,
    net_landed_gm_ytd_ly,
    net_cosa_gm_ytd_ly,
    net_units_ytd_ly,

    -- Window metrics
    gm_pct_ytd,
    vendor_share_ytd_in_dept,
    vendor_rank_ytd_in_dept
FROM final_with_windows
ORDER BY
    level_5_analytics_id,
    channel,
    vendor_rank_ytd_in_dept;
