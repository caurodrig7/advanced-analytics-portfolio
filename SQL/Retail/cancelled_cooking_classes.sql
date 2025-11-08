/* 
--------------------------------------------------------------------------------
SQL Script: Canceled Culinary Classes
--------------------------------------------------------------------------------
Objective:
    Identify Culinary classes that were canceled and analyze their related 
    customer orders, refunds, and cancellations at the order-line level. 

Definition:
    - Canceled Class:
        • Class flagged as canceled in the Culinary catalog.
    - Refund and Cancellation Tracking:
        • Links original sales with written returns and canceled order lines.
        • Calculates refunded or canceled quantities and monetary values.
    - Customer Resolution:
        • Replaces “Cozymeal Chef” placeholder names with actual billing names 
          to accurately identify affected customers.

Scope:
    - Aggregated by order number, SKU, and class start date.
    - Provides metrics for total seats sold, refunded, and canceled, along 
      with corresponding extended revenue values.

Author: Cesar Rodriguez
--------------------------------------------------------------------------------
*/

WITH base_sales AS (
    SELECT
        sl.order_number,
        sl.date_ordered,
        sh.email,
        so.customer_name,
        ba.first_name,
        ba.last_name,
        cp.location_code,
        sl.sku,
        p.short_description,
        cp.is_class_cancelled,
        cp.start_date,
        sl.quantity,
        sl.quantity_shipped,
        sl.sub_total,
        sl.is_return,
        wr.reason_code        AS return_reason_code,
        wr.reason             AS return_reason,
        wr.dt                 AS return_date,
        sl.quantity_returned,
        sl.is_canceled,
        sl.cancellation_reason_code,
        sl.cancellation_reason,
        cs.dt                 AS cancellation_date,
        sl.quantity_canceled
    FROM peep.sales_line AS sl
    LEFT JOIN peep.culinary_products AS cp
        ON cp.sku = sl.sku 
    LEFT JOIN peep.products AS p
        ON p.sku = sl.sku
    LEFT JOIN peep.sales_header AS sh
        ON sh.order_number = sl.order_number 
    LEFT JOIN peep.written_returns AS wr
        ON wr.order_number = sl.order_number
       AND wr.order_line_analytics_id = sl.order_line_analytics_id
    LEFT JOIN peep.canceled_sales AS cs
        ON cs.order_number = sl.order_number
       AND cs.order_line_analytics_id = sl.order_line_analytics_id
    LEFT JOIN peep.sfcc_order AS so
        ON so.sfcc_order_number = sh.ecom_order_number 
    LEFT JOIN peep.sfcc_billing_address AS ba
        ON ba.sfcc_order_number = sh.ecom_order_number 
    WHERE cp.is_class_cancelled = 'TRUE'
      AND cp.start_date >= TIMESTAMP '2023-01-29 00:00:00'
),
customer_resolved AS (
    SELECT
        order_number,
        date_ordered,
        email,
        CASE 
            WHEN customer_name = 'Cozymeal Chef'
                THEN TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))
            ELSE customer_name
        END                         AS customer_name,
        location_code,
        sku,
        short_description,
        is_class_cancelled,
        start_date                  AS class_start_date,
        quantity,
        quantity_shipped,
        sub_total,
        is_return,
        return_reason_code,
        return_reason,
        return_date,
        quantity_returned,
        is_canceled,
        cancellation_reason_code,
        cancellation_reason,
        cancellation_date,
        quantity_canceled
    FROM base_sales
),
aggregated_order_lines AS (
    -- Aggregate just in case there are duplicate joins per order/SKU
    SELECT
        order_number,
        date_ordered,
        email,
        customer_name,
        location_code,
        sku,
        short_description,
        is_class_cancelled,
        class_start_date,
        SUM(quantity)               AS quantity,
        SUM(quantity_shipped)       AS quantity_shipped,
        SUM(sub_total)              AS sub_total,
        MAX(is_return)              AS is_return,
        MAX(return_reason_code)     AS return_reason_code,
        MAX(return_reason)          AS return_reason,
        MAX(return_date)            AS return_date,
        SUM(quantity_returned)      AS quantity_returned,
        MAX(is_canceled)            AS is_canceled,
        MAX(cancellation_reason_code) AS cancellation_reason_code,
        MAX(cancellation_reason)    AS cancellation_reason,
        MAX(cancellation_date)      AS cancellation_date,
        SUM(quantity_canceled)      AS quantity_canceled
    FROM customer_resolved
    GROUP BY
        order_number,
        date_ordered,
        email,
        customer_name,
        location_code,
        sku,
        short_description,
        is_class_cancelled,
        class_start_date
),
enriched_metrics AS (
    SELECT
        a.*,
        CASE 
            WHEN SUM(quantity) OVER (PARTITION BY order_number, sku) = 0
                THEN 0
            ELSE sub_total / NULLIF(quantity, 0)
        END   AS seat_price,
        ( quantity 
          - quantity_returned 
          - quantity_canceled )  AS qty_net,
        ( quantity 
          - quantity_returned 
          - quantity_canceled )
          * CASE 
                WHEN quantity = 0 THEN 0
                ELSE sub_total / NULLIF(quantity, 0)
            END   AS ext_price,
        -- Class-level totals (per location / class / SKU)
        SUM(sub_total) OVER (
            PARTITION BY location_code, class_start_date, sku
        )  AS class_total_revenue,
        SUM(
            quantity - quantity_returned - quantity_canceled
        ) OVER (
            PARTITION BY location_code, class_start_date, sku
        )  AS class_total_net_qty,
        -- Rank orders within each class by revenue impact
        RANK() OVER (
            PARTITION BY location_code, class_start_date
            ORDER BY sub_total DESC
        )  AS order_rank_in_class
    FROM aggregated_order_lines AS a
)
SELECT
    order_number,
    date_ordered,
    email,
    customer_name,
    location_code,
    sku,
    short_description,
    is_class_cancelled,
    class_start_date,
    quantity,
    quantity_shipped,
    sub_total,
    is_return,
    return_reason_code,
    return_reason,
    return_date,
    quantity_returned,
    is_canceled,
    cancellation_reason_code,
    cancellation_reason,
    cancellation_date,
    quantity_canceled,
    seat_price,
    qty_net          AS qty,
    ext_price,
    class_total_revenue,
    class_total_net_qty,
    order_rank_in_class
FROM enriched_metrics
ORDER BY
    class_start_date,
    location_code,
    order_number,
    sku;
