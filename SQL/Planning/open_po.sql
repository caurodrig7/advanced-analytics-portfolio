/* 
--------------------------------------------------------------------------------
SQL Script: APOO Report
--------------------------------------------------------------------------------
Objective:
    Generate a comprehensive Open PO and On-Order report combining Open Orders,
    Received Orders, Product Attributes, Merchandising Taxonomy, Vendor Info,
    and Location data. Provide visibility into ordered units, received units,
    open units, open cost, and aging across vendors, departments, and products.

Definition:
    - Open Purchase Orders:
        • Lines where is_closed = 0
        • Includes ordered units, received units, and remaining open units
        • Computes open cost using both landed and vendor cost
        • Adds stale aging using DATEDIFF(CURRENT_DATE, arrival_date)

    - Received Purchase Orders:
        • Aggregated by PO line, vendor, department, and location
        • Joined using PO analytics ID, line number, arrival date, and SKU
        • Captures total units received per line

    - Full Outer Join Logic:
        • Open PO and Received PO datasets merged using UNION ALL
        • Ensures PO lines that exist only in Received or only in Open are included

    - Price & Age Details:
        • Latest retail price determined with window function RANK()
        • Price type computed from price rules (regular, Markdown, MOS, POS)
        • Receipt age bucket computed using last receipt date

    - Merchandising Details:
        • Taxonomy Level-3 department, category, and code
        • Collection and product attributes enriched via subqueries

Processing Steps:
    1. Build base PO details with cost, open units, and stale days.
    2. Build aggregated Open PO and Received PO datasets filtered to valid departments.
    3. Merge datasets using UNION ALL to emulate FULL OUTER JOIN.
    4. Construct merchandising and collection dimensions with CTEs.
    5. Determine latest price via window functions (RANK OVER partition by SKU).
    6. Create price_type and receipt aging buckets.
    7. Join all entities (vendor, product, collections, taxonomy, locations).
    8. Add complex window metrics:
         • Total open units by vendor
         • PO recency rank within vendor
         • Average stale days per product
         • Open-cost share within department
    9. Produce final APOO fact table with PO, product, vendor, dept., cost, units,
       price type, stale metrics, and aging information.

Scope:
    - Includes only Level-3 merchandising departments.
    - Includes Open and Received PO activity across all retail locations.
    - Output is suitable for planning, replenishment, inventory management, and
      vendor performance tracking.

Author: Cesar Rodriguez
--------------------------------------------------------------------------------
*/

WITH
-- 1) Base PO details (open lines only)
po_details_base AS (
    SELECT
        pod.purchase_order_analytics_id,
        pod.po_line_number,
        pod.location_analytics_id,
        pod.product_analytics_id,
        pod.vendor_analytics_id,
        pod.sku,
        CASE WHEN pod.is_closed = 0 THEN 'Open' ELSE 'Closed' END AS po_line_open_status,
        pod.written_date                          AS po_written_date,
        pod.current_ship_date                     AS po_ship_date,
        pod.current_arrival_date                  AS po_arrival_date,
        pod.current_arrival_date_analytics_id,
        pod.quantity_ordered,
        pod.quantity_received,
        CASE
            WHEN pod.next_cost_effective_date IS NULL THEN DATE('2099-12-31')
            ELSE pod.next_cost_effective_date
        END                                       AS next_cost_effective_date,
        CASE
            WHEN pod.cancel_date IS NULL THEN DATE('2099-12-31')
            ELSE pod.cancel_date
        END                                       AS cancel_date,
        CASE
            WHEN pod.vendor_po_name IS NULL THEN 'not populated'
            ELSE pod.vendor_po_name
        END                                       AS vendor_po_name,
        CASE
            WHEN pod.quantity_open < 0 THEN 0
            ELSE pod.quantity_open
        END                                       AS quantity_open,
        pod.quantity_ordered * pod.landed_cost    AS ordered_landed_cost,
        pod.quantity_ordered * pod.vendor_cost    AS ordered_vendor_cost,
        pod.quantity_received * pod.landed_cost   AS received_landed_cost,
        pod.quantity_received * pod.vendor_cost   AS received_vendor_cost,
        (CASE WHEN pod.quantity_open < 0 THEN 0 ELSE pod.quantity_open END)
            * pod.landed_cost                     AS open_landed_cost,
        (CASE WHEN pod.quantity_open < 0 THEN 0 ELSE pod.quantity_open END)
            * pod.vendor_cost                     AS open_vendor_cost,
        DATEDIFF(CURRENT_DATE(), pod.current_arrival_date) AS stale_days
    FROM peep.po_details pod
    WHERE pod.is_closed = 0
),

-- 2) OPEN PO aggregation
open_po_agg AS (
    SELECT
        b.vendor_po_name,
        b.purchase_order_analytics_id,
        b.po_line_number,
        b.next_cost_effective_date,
        b.cancel_date,
        b.location_analytics_id,
        SUM(b.open_landed_cost) AS WJXBFS1,   -- Open On Order ext Cost $s (LC)
        SUM(b.open_vendor_cost) AS WJXBFS2,   -- Open On Order ext Cost $s (Vendor)
        SUM(b.quantity_open)    AS WJXBFS3,   -- Open On Order Units
        SUM(b.quantity_ordered) AS WJXBFS4    -- PO Ordered Units
    FROM po_details_base b
    JOIN product_to_merchandising_taxonomy pmt
      ON b.product_analytics_id = pmt.product_analytics_id
    WHERE pmt.level_3_analytics_id IN
          (500004,500005,250003,3,500006,250004,500007,250005,
           500008,500010,6,250007,500012,8)
    GROUP BY
        b.vendor_po_name,
        b.purchase_order_analytics_id,
        b.po_line_number,
        b.next_cost_effective_date,
        b.cancel_date,
        b.location_analytics_id
),

-- 3) RECEIVED PO aggregation
received_po_agg AS (
    SELECT
        b.vendor_po_name,
        pr.purchase_order_analytics_id,
        pr.po_line_number,
        b.next_cost_effective_date,
        b.cancel_date,
        pr.location_analytics_id,
        SUM(pr.recieved_quantity) AS WJXBFS1     -- PO Received Units
    FROM po_receipts pr
    JOIN po_details_base b
      ON  pr.location_analytics_id           = b.location_analytics_id
      AND pr.po_line_number                  = b.po_line_number
      AND pr.purchase_order_analytics_id     = b.purchase_order_analytics_id
      AND pr.received_date_analytics_id      = b.current_arrival_date_analytics_id
    JOIN product_to_merchandising_taxonomy pmt
      ON pr.product_analytics_id = pmt.product_analytics_id
    WHERE pmt.level_3_analytics_id IN
          (500004,500005,250003,3,500006,250004,500007,250005,
           500008,500010,6,250007,500012,8)
    GROUP BY
        b.vendor_po_name,
        pr.purchase_order_analytics_id,
        pr.po_line_number,
        b.next_cost_effective_date,
        b.cancel_date,
        pr.location_analytics_id
),

-- 4) FULL OUTER JOIN EMULATION: OPEN + RECEIVED
po_open_vs_received AS (
    -- LEFT side: all open lines, with matching receipts when present
    SELECT
        o.vendor_po_name,
        o.purchase_order_analytics_id,
        o.po_line_number,
        o.next_cost_effective_date,
        o.cancel_date,
        o.location_analytics_id,
        o.WJXBFS1,
        o.WJXBFS2,
        o.WJXBFS3,
        o.WJXBFS4,
        r.WJXBFS1 AS WJXBFS5
    FROM open_po_agg o
    LEFT JOIN received_po_agg r
      ON  r.vendor_po_name              = o.vendor_po_name
      AND r.purchase_order_analytics_id = o.purchase_order_analytics_id
      AND r.po_line_number              = o.po_line_number
      AND r.next_cost_effective_date    = o.next_cost_effective_date
      AND r.cancel_date                 = o.cancel_date
      AND r.location_analytics_id       = o.location_analytics_id

    UNION ALL

    -- RIGHT anti-join: receipts that have no corresponding open lines
    SELECT
        r.vendor_po_name,
        r.purchase_order_analytics_id,
        r.po_line_number,
        r.next_cost_effective_date,
        r.cancel_date,
        r.location_analytics_id,
        NULL        AS WJXBFS1,
        NULL        AS WJXBFS2,
        NULL        AS WJXBFS3,
        NULL        AS WJXBFS4,
        r.WJXBFS1   AS WJXBFS5
    FROM received_po_agg r
    LEFT JOIN open_po_agg o
      ON  o.vendor_po_name              = r.vendor_po_name
      AND o.purchase_order_analytics_id = r.purchase_order_analytics_id
      AND o.po_line_number              = r.po_line_number
      AND o.next_cost_effective_date    = r.next_cost_effective_date
      AND o.cancel_date                 = r.cancel_date
      AND o.location_analytics_id       = r.location_analytics_id
    WHERE o.purchase_order_analytics_id IS NULL
),

-- 5) Collections / department mapping
collection_dim AS (
    SELECT
        p.product_analytics_id,
        pa.collection,
        mt3.department_code,
        mt3.Department_id,
        MAX(c.collection_name) AS collection_name,
        '  '                   AS collction_description
    FROM peep.products p
    JOIN peep.product_to_merchandising_taxonomy mt
      ON p.product_analytics_id = mt.product_analytics_id
    JOIN (
        SELECT
            taxonomy_analytics_id        AS Department_id,
            parent_taxonomy_analytics_id AS division_id,
            name                         AS department_name,
            CAST(taxonomy_code AS SIGNED) AS department_code
        FROM peep.merchandising_taxonomies
        WHERE level = 3
    ) mt3
      ON mt.level_3_analytics_id = mt3.Department_id
    JOIN peep.slt_product_attributes pa
      ON pa.product_analytics_id = p.product_analytics_id
    LEFT JOIN peep.collections c
      ON c.collection = pa.collection
     AND mt3.department_code = c.department
    GROUP BY
        p.product_analytics_id,
        pa.collection,
        mt3.department_code,
        mt3.Department_id
),

-- 6) Latest price & price-type / age buckets
latest_price AS (
    SELECT
        sku,
        product_analytics_id,
        price,
        RANK() OVER (
            PARTITION BY product_analytics_id
            ORDER BY snapshot_date DESC
        ) AS r1
    FROM peep.product_price_history
),

price_info AS (
    SELECT
        p.product_analytics_id,
        p.retail_price                             AS web_price,
        COALESCE(lp.price, p.retail_price)        AS current_retail_price,
        p.last_receipt_date,
        DATEDIFF(CURRENT_DATE(), p.last_receipt_date) AS last_receipt_age,

        -- price type id/code/description
        CASE
            WHEN (FLOOR(COALESCE(lp.price, p.retail_price) * 100) % 10) = 6 THEN 3
            WHEN (FLOOR(COALESCE(lp.price, p.retail_price) * 100) % 10) = 9 THEN 2
            WHEN RIGHT(CAST(COALESCE(lp.price, p.retail_price) AS CHAR(10)), 2) = '01' THEN 4
            WHEN COALESCE(lp.price, p.retail_price) IS NULL THEN 5
            ELSE 1
        END AS current_price_type_id,
        CASE
            WHEN (FLOOR(COALESCE(lp.price, p.retail_price) * 100) % 10) = 6 THEN 'POS'
            WHEN (FLOOR(COALESCE(lp.price, p.retail_price) * 100) % 10) = 9 THEN 'MKD'
            WHEN RIGHT(CAST(COALESCE(lp.price, p.retail_price) AS CHAR(10)), 2) = '01' THEN 'MOS'
            WHEN COALESCE(lp.price, p.retail_price) IS NULL THEN 'NP'
            ELSE 'REG'
        END AS current_price_type_code,
        CASE
            WHEN (FLOOR(COALESCE(lp.price, p.retail_price) * 100) % 10) = 6 THEN 'POS'
            WHEN (FLOOR(COALESCE(lp.price, p.retail_price) * 100) % 10) = 9 THEN 'Markdown'
            WHEN RIGHT(CAST(COALESCE(lp.price, p.retail_price) AS CHAR(10)), 2) = '01' THEN 'MOS'
            WHEN COALESCE(lp.price, p.retail_price) IS NULL THEN 'No Price'
            ELSE 'Regular'
        END AS current_price_type_description,

        -- receipt age bucket
        CASE
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) <= 91 THEN 5
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 92 AND 182 THEN 4
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 183 AND 273 THEN 3
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 274 AND 364 THEN 2
            ELSE 1
        END AS receipt_age_bucket_id,
        CASE
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) <= 91 THEN 'Aged Last 13 Weeks'
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 92 AND 182 THEN 'Aged 14 - 26 Weeks'
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 183 AND 273 THEN 'Aged 27 - 39 Weeks'
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 274 AND 364 THEN 'Aged 40 - 52 Weeks'
            ELSE 'Aged Greater than 52 Weeks'
        END AS receipt_age_bucket
    FROM peep.products p
    LEFT JOIN (
        SELECT
            sku,
            product_analytics_id,
            price
        FROM latest_price
        WHERE r1 = 1
    ) lp
      ON p.product_analytics_id = lp.product_analytics_id
),

-- 7) Department dimension
department_dim AS (
    SELECT
        taxonomy_analytics_id        AS Department_id,
        parent_taxonomy_analytics_id AS division_id,
        name                         AS department_name,
        CAST(taxonomy_code AS SIGNED) AS department_code
    FROM peep.merchandising_taxonomies
    WHERE level = 3
),

-- 8) MAIN FACT CTE – join PO + product + vendor + location + collections
apoo_fact AS (
    SELECT
        d.po_ship_date,
        d.current_arrival_date_analytics_id                     AS po_arrival_date_id,
        d.po_arrival_date                                       AS po_arrival_date,
        COALESCE(d.vendor_analytics_id, 0)                      AS vendor_analytics_id,
        COALESCE(v.vendor_name, 'Blank')                        AS vendor_name,
        v.vendor_style_num,
        pi.current_retail_price                                 AS file_retail_price,
        d.purchase_order_analytics_id,
        d.po_written_date,
        spa.next_vendor_cost,
        col.Department_id                                       AS level_3_analytics_id,
        dept.department_name                                    AS level_3_name,
        dept.department_code                                    AS taxonomy_code,
        d.product_analytics_id,
        p.product_name,
        spa.market,
        p.season,
        spa.status,
        spa.ticket_type,
        p.holiday,
        col.collection,
        col.collection_name,
        col.collction_description                               AS CustCol_34,
        spa.port,
        p.country_of_origin,
        porr.location_analytics_id,
        loc.name                                                AS location_name,
        CASE
            WHEN ven.is_foreign_vendor = 1 THEN 'For'
            WHEN ven.is_foreign_vendor = 0 THEN 'Dom'
            ELSE ''
        END                                                     AS CustCol_18,
        p.minimum_order_quantity,
        p.first_receipt_date,
        p.last_receipt_date,
        porr.vendor_po_name,
        porr.next_cost_effective_date,
        porr.cancel_date,
        p.original_retail_price                                 AS retail_price,
        porr.purchase_order_analytics_id                        AS purchase_order_analytics_id0,
        porr.po_line_number,
        porr.WJXBFS1,       -- Open On Order ext Cost $s (LC)
        porr.WJXBFS2,       -- Open On Order ext Cost $s (Vendor)
        porr.WJXBFS3,       -- Open On Order Units
        porr.WJXBFS4,       -- PO Ordered Units
        porr.WJXBFS5,       -- PO Received Units
        d.stale_days
    FROM po_open_vs_received porr
    JOIN po_details_base d
      ON  d.cancel_date             = porr.cancel_date
      AND d.location_analytics_id   = porr.location_analytics_id
      AND d.next_cost_effective_date= porr.next_cost_effective_date
      AND d.po_line_number          = porr.po_line_number
      AND d.purchase_order_analytics_id = porr.purchase_order_analytics_id
      AND d.vendor_po_name          = porr.vendor_po_name
    JOIN peep.products p
      ON d.product_analytics_id     = p.product_analytics_id
    JOIN collection_dim col
      ON d.product_analytics_id     = col.product_analytics_id
    JOIN price_info pi
      ON d.product_analytics_id     = pi.product_analytics_id
    JOIN vendors ven
      ON COALESCE(d.vendor_analytics_id,0) = COALESCE(ven.vendor_analytics_id,0)
    JOIN peep.slt_product_attributes spa
      ON d.product_analytics_id     = spa.product_analytics_id
    JOIN locations loc
      ON porr.location_analytics_id = loc.location_analytics_id
    JOIN department_dim dept
      ON col.Department_id          = dept.Department_id
),

-- 9) WINDOW METRICS – vendor & department level
apoo_with_windows AS (
    SELECT
        af.*,
        -- Total open units per vendor
        SUM(af.WJXBFS3) OVER (
            PARTITION BY af.vendor_analytics_id
        ) AS total_open_units_by_vendor,

        -- Rank of PO line recency per vendor
        ROW_NUMBER() OVER (
            PARTITION BY af.vendor_analytics_id
            ORDER BY af.po_written_date DESC
        ) AS vendor_po_recency_rank,

        -- Average stale days per product
        AVG(af.stale_days) OVER (
            PARTITION BY af.product_analytics_id
        ) AS avg_stale_days_per_product,

        -- Share of open cost (vendor) within department
        af.WJXBFS2 / NULLIF(
            SUM(af.WJXBFS2) OVER (PARTITION BY af.level_3_analytics_id),
            0
        ) AS pct_open_cost_within_dept
    FROM apoo_fact af
)

-- FINAL SELECT
SELECT
    po_ship_date,
    po_arrival_date_id         AS po_arrival_date,
    po_arrival_date            AS po_arrival_date_display,
    vendor_analytics_id,
    vendor_name,
    vendor_style_num,
    file_retail_price,
    purchase_order_analytics_id,
    po_written_date,
    next_vendor_cost,
    level_3_analytics_id,
    level_3_name,
    taxonomy_code,
    product_analytics_id,
    product_name,
    market,
    season,
    status,
    ticket_type,
    holiday,
    collection,
    collection_name,
    CustCol_34,
    port,
    country_of_origin,
    location_analytics_id,
    location_name,
    CustCol_18,
    minimum_order_quantity,
    first_receipt_date,
    last_receipt_date,
    vendor_po_name,
    next_cost_effective_date,
    cancel_date,
    retail_price,
    purchase_order_analytics_id0,
    po_line_number,
    WJXBFS1,
    WJXBFS2,
    WJXBFS3,
    WJXBFS4,
    WJXBFS5,

    -- Extra window metrics
    total_open_units_by_vendor,
    vendor_po_recency_rank,
    avg_stale_days_per_product,
    pct_open_cost_within_dept
FROM apoo_with_windows
ORDER BY
    vendor_name,
    po_written_date DESC,
    product_name;
