/* 
--------------------------------------------------------------------------------
SQL Script: Market Basket – Items Sold (SKU Level)
--------------------------------------------------------------------------------
Objective:
    Build a complete market basket dataset that shows SKU-level sales,
    units, channel distribution, product metadata, and order-level attributes
    across all products and all vendors, without any filtering. Provide
    visibility into how each SKU performs across channels and how each SKU
    appears across all orders within a defined date range.

Definition:
    - Base Written Sales:
        • Order × SKU × Channel written sales, units, cost, and margin.
        • Channels derived from attribution locations or defaulted to POS.
    - SKU × Channel Sales:
        • Aggregates sales at SKU + channel for filtered orders.
    - Dense SKU–Channel Grid:
        • Full universe of order × SKU × channel combinations to ensure
          all possible SKU–channel combos appear in final output.
    - Enriched Market Basket:
        • Adds product names, merchandising taxonomy (Lvl 3),
          channel codes, and order metadata.
        • Adds window metrics: SKU-level totals, channel share, and ranking.

Scope:
    - Covers ALL products and ALL vendors.
    - Includes ALL channels defined in the channels dimension.
    - Applies only a date filter (start_date → end_date).
    - No restrictions by department, vendor, or SKU attributes.

Processing Steps:
    1. Determine valid date range using params CTE.
    2. Build base written sales from written_sales + sales_line + locations.
    3. Identify all SKUs and orders within the date range (no filtering).
    4. Aggregate SKU × channel written sales for filtered orders.
    5. Count distinct store-agent orders per order as “primary product identifier”.
    6. Generate full SKU × channel grid for each qualifying order.
    7. Merge real sales and grid via FULL OUTER JOIN emulation (UNION ALL).
    8. Enrich with:
         • Product metadata
         • Level-3 merchandising taxonomy
         • Channel descriptions
         • Order numbers
    9. Add advanced analytics:
         • Total SKU sales across all channels (window sum)
         • Channel share of each SKU (ratio over window sum)
         • Channel ranking per SKU (ROW_NUMBER)
    10. Produce final SKU-level Market Basket dataset including:
         • Sales $, units, primary-product identifier,
           SKU totals, channel share, and channel ranking.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH
-- 0) Parameters (date range)
params AS (
    SELECT
        DATE('2025-11-16') AS start_date,
        DATE('2025-11-17') AS end_date
),

-- 1) Base written sales (order × line × sku × channel)
base_written_sales AS (
    SELECT
        s.order_analytics_id,
        s.order_number,
        s.order_line_analytics_id,
        s.date_analytics_id,
        s.product_analytics_id,
        COALESCE(l.sales_channel, 'pos') AS sales_channel,
        st.channel_analytics_id,
        SUM(s.merchandise)               AS written_sales_dollars,
        SUM(s.quantity)                  AS written_sales_quantity,
        SUM(s.fair_market_value)         AS written_sales_cost_dollars,
        SUM(s.merchandise - COALESCE(s.fair_market_value, 0))
                                         AS written_sales_gross_margin_dollars
    FROM peep.written_sales s
    LEFT JOIN peep.sales_line l
        ON l.order_line_analytics_id = s.order_line_analytics_id
    LEFT JOIN peep.locations st
        ON st.location_analytics_id = l.attribution_location_analytics_id
    GROUP BY
        s.order_analytics_id,
        s.order_number,
        s.order_line_analytics_id,
        s.date_analytics_id,
        s.product_analytics_id,
        COALESCE(l.sales_channel, 'pos'),
        st.channel_analytics_id
),

-- 2) Filtered order × SKU 
filtered_order_sku AS (
    SELECT
        bws.order_analytics_id          AS original_order_analytics_id,
        bws.product_analytics_id,
        SUM(bws.written_sales_dollars)  AS sku_written_sales_dollars
    FROM base_written_sales bws
    JOIN calendar c
        ON bws.date_analytics_id = c.date_analytics_id
    JOIN params pr
        ON DATE(c.gregorian_date) BETWEEN pr.start_date AND pr.end_date
    GROUP BY
        bws.order_analytics_id,
        bws.product_analytics_id
),

-- 3) Filtered orders 
filtered_orders AS (
    SELECT
        bws.order_analytics_id          AS original_order_analytics_id,
        SUM(bws.written_sales_dollars)  AS order_written_sales_dollars,
        SUM(bws.written_sales_quantity) AS order_written_sales_quantity
    FROM base_written_sales bws
    JOIN calendar c
        ON bws.date_analytics_id = c.date_analytics_id
    JOIN params pr
        ON DATE(c.gregorian_date) BETWEEN pr.start_date AND pr.end_date
    GROUP BY
        bws.order_analytics_id
),

-- 4) SKU × channel sales 
sku_channel_sales AS (
    SELECT
        bws.order_analytics_id          AS original_order_analytics_id,
        bws.product_analytics_id,
        bws.channel_analytics_id        AS channel,
        SUM(bws.written_sales_dollars)  AS sku_channel_written_sales_dollars,
        SUM(bws.written_sales_quantity) AS sku_channel_written_sales_quantity
    FROM base_written_sales bws
    JOIN filtered_orders fo
        ON fo.original_order_analytics_id = bws.order_analytics_id
    GROUP BY
        bws.order_analytics_id,
        bws.product_analytics_id,
        bws.channel_analytics_id
),

-- 5) Order-level agent count (store_agents lookup)
order_agent_counts AS (
    SELECT
        fo.original_order_analytics_id,
        COUNT(DISTINCT sa.order_analytics_id) AS distinct_agent_orders
    FROM store_agents sa
    JOIN peep.sales_line sl
        ON sa.order_line_analytics_id = sl.order_line_analytics_id
    JOIN filtered_orders fo
        ON sl.order_analytics_id = fo.original_order_analytics_id
    GROUP BY
        fo.original_order_analytics_id
),

-- 6) Dense grid of order × sku × channel (ensures all combinations exist)
sku_channel_grid AS (
    SELECT
        fos.original_order_analytics_id,
        fos.product_analytics_id,
        ch.channel_analytics_id AS channel,
        1 AS grid_flag
    FROM filtered_order_sku fos
    CROSS JOIN channels ch
),

-- 7) FULL OUTER JOIN: sku_channel_sales ↔ sku_channel_grid
full_mb_union AS (
    -- Left side: actual sales
    SELECT
        scs.channel,
        scs.original_order_analytics_id,
        scs.product_analytics_id,
        scs.sku_channel_written_sales_dollars,
        scs.sku_channel_written_sales_quantity,
        scg.grid_flag
    FROM sku_channel_sales scs
    LEFT JOIN sku_channel_grid scg
        ON scs.channel = scg.channel
       AND scs.original_order_analytics_id = scg.original_order_analytics_id
       AND scs.product_analytics_id = scg.product_analytics_id

    UNION ALL

    -- Right anti-join: grid combinations with no sales
    SELECT
        scg.channel,
        scg.original_order_analytics_id,
        scg.product_analytics_id,
        NULL AS sku_channel_written_sales_dollars,
        NULL AS sku_channel_written_sales_quantity,
        scg.grid_flag
    FROM sku_channel_grid scg
    LEFT JOIN sku_channel_sales scs
        ON scs.channel = scg.channel
       AND scs.original_order_analytics_id = scg.original_order_analytics_id
       AND scs.product_analytics_id = scg.product_analytics_id
    WHERE scs.original_order_analytics_id IS NULL
),

-- 8) Enrich with product, taxonomy, channel labels, order metadata
mb_enriched AS (
    SELECT
        fmu.channel,
        ch.channel_code,
        fmu.original_order_analytics_id,
        sl.order_number AS original_order_number,
        pmt.level_3_analytics_id,
        dept.department_name AS level_3_name,
        dept.department_code AS taxonomy_code,
        fmu.product_analytics_id,
        p.product_name,
        fmu.sku_channel_written_sales_dollars,
        fmu.sku_channel_written_sales_quantity,
        oac.distinct_agent_orders AS primary_product_identifier,
        fmu.grid_flag,

        -- Window metric: Total SKU sales (all channels)
        SUM(COALESCE(fmu.sku_channel_written_sales_dollars, 0)) OVER (
            PARTITION BY fmu.product_analytics_id
        ) AS total_sales_per_sku,

        -- Window metric: Channel share % within SKU
        COALESCE(fmu.sku_channel_written_sales_dollars, 0)
        / NULLIF(
            SUM(COALESCE(fmu.sku_channel_written_sales_dollars, 0)) OVER (
                PARTITION BY fmu.product_analytics_id
            ), 0
        ) AS channel_sales_share_for_sku,

        -- Rank channels for each SKU
        ROW_NUMBER() OVER (
            PARTITION BY fmu.product_analytics_id
            ORDER BY SUM(COALESCE(fmu.sku_channel_written_sales_dollars, 0))
                     OVER (PARTITION BY fmu.product_analytics_id, fmu.channel) DESC,
                     ch.channel_code
        ) AS sku_channel_rank_within_sku

    FROM full_mb_union fmu
    JOIN channels ch
        ON fmu.channel = ch.channel_analytics_id
    JOIN product_to_merchandising_taxonomy pmt
        ON fmu.product_analytics_id = pmt.product_analytics_id
    JOIN (
        SELECT
            taxonomy_analytics_id AS department_id,
            parent_taxonomy_analytics_id AS division_id,
            name AS department_name,
            CAST(taxonomy_code AS SIGNED) AS department_code
        FROM peep.merchandising_taxonomies
        WHERE level = 3
    ) dept
        ON pmt.level_3_analytics_id = dept.department_id
    JOIN peep.products p
        ON fmu.product_analytics_id = p.product_analytics_id
    JOIN peep.sales_line sl
        ON sl.order_analytics_id = fmu.original_order_analytics_id
       AND sl.product_analytics_id = fmu.product_analytics_id
    LEFT JOIN order_agent_counts oac
        ON oac.original_order_analytics_id = fmu.original_order_analytics_id
),

-- 9) Final fact table (SKU level)
final_market_basket AS (
    SELECT
        channel,
        channel_code,
        level_3_analytics_id,
        level_3_name,
        taxonomy_code,
        product_analytics_id,
        product_name,
        COALESCE(sku_channel_written_sales_dollars, 0) AS written_gross_sales_market_basket,
        COALESCE(sku_channel_written_sales_quantity, 0) AS written_gross_units_market_basket,
        COALESCE(primary_product_identifier, 0) AS market_basket_primary_product_identifier,
        grid_flag AS market_basket_order_presence_flag,
        total_sales_per_sku,
        channel_sales_share_for_sku,
        sku_channel_rank_within_sku
    FROM mb_enriched
)

-- FINAL SELECT
SELECT
    channel,
    channel_code,
    level_3_analytics_id,
    level_3_name,
    taxonomy_code,
    product_analytics_id,
    product_name,
    written_gross_sales_market_basket,
    written_gross_units_market_basket,
    market_basket_primary_product_identifier,
    market_basket_order_presence_flag,
    total_sales_per_sku,
    channel_sales_share_for_sku,
    sku_channel_rank_within_sku
FROM final_market_basket
ORDER BY
    channel_code,
    level_3_analytics_id,
    product_analytics_id,
    sku_channel_rank_within_sku;
