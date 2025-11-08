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

select peep.sales_line.order_number
, peep.sales_line.date_ordered 
, peep.sales_header.email 
, (CASE WHEN peep.sfcc_order.customer_name = 'Cozymeal Chef' THEN (peep.sfcc_billing_address.first_name || ' ' || peep.sfcc_billing_address.last_name) ELSE peep.sfcc_order.customer_name END) customer_name 
, peep.culinary_products.location_code
, peep.sales_line.sku
, peep.products.short_description
, peep.culinary_products.is_class_cancelled 
, peep.culinary_products.start_date class_start_date 
, sum(peep.sales_line.quantity) quantity
, sum(peep.sales_line.quantity_shipped) quantity_shipped
, sum(peep.sales_line.sub_total) sub_total
, peep.sales_line.is_return 
, peep.written_returns.reason_code 
, peep.written_returns.reason 
, peep.written_returns.dt return_date
, sum(peep.sales_line.quantity_returned) quantity_returned
, peep.sales_line.is_canceled 
, peep.sales_line.cancellation_reason_code 
, peep.sales_line.cancellation_reason 
, peep.canceled_sales.dt cancellation_date
, sum(peep.sales_line.quantity_canceled) quantity_canceled
, (sum(peep.sales_line.sub_total) / sum(peep.sales_line.quantity)) seat_price
, (sum(peep.sales_line.quantity) - sum(peep.sales_line.quantity_returned) - sum(peep.sales_line.quantity_canceled) ) qty
, ((sum(peep.sales_line.quantity) - sum(peep.sales_line.quantity_returned) - sum(peep.sales_line.quantity_canceled) ) * (sum(peep.sales_line.sub_total) / sum(peep.sales_line.quantity)) ) ext_price
from peep.sales_line
left join peep.culinary_products
on peep.culinary_products.sku = peep.sales_line.sku 
left join peep.products
on peep.products.sku = peep.sales_line.sku
left join peep.sales_header
on peep.sales_header.order_number  = peep.sales_line.order_number 
left join peep.written_returns
on ((peep.written_returns.order_number  = peep.sales_line.order_number) and (peep.written_returns.order_line_analytics_id = peep.sales_line.order_line_analytics_id))
left join peep.canceled_sales
on ((peep.canceled_sales.order_number  = peep.sales_line.order_number) and (peep.canceled_sales.order_line_analytics_id = peep.sales_line.order_line_analytics_id))
left join peep.sfcc_order
on peep.sfcc_order.sfcc_order_number = peep.sales_header.ecom_order_number 
left join peep.sfcc_billing_address
on peep.sfcc_billing_address.sfcc_order_number = peep.sales_header.ecom_order_number 
where peep.culinary_products.is_class_cancelled = 'TRUE'
and peep.culinary_products.start_date >= '2023-01-29 00:00:00'
group by peep.sales_line.order_number
, peep.sales_header.email 
, peep.sfcc_order.customer_name 
, peep.culinary_products.location_code
, peep.sales_line.date_ordered 
, peep.sales_line.sku
, peep.products.short_description
, peep.culinary_products.is_class_cancelled 
, peep.culinary_products.start_date
, peep.sales_line.is_return 
, peep.written_returns.reason_code 
, peep.written_returns.reason 
, peep.sales_line.is_canceled 
, peep.written_returns.dt 
, peep.sales_line.cancellation_reason_code 
, peep.sales_line.cancellation_reason 
, peep.canceled_sales.dt
, peep.culinary_products.is_class_cancelled
, peep.sfcc_billing_address.first_name
, peep.sfcc_billing_address.last_name;