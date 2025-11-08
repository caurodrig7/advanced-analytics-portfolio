/* 
--------------------------------------------------------------------------------
SQL Script: Amex Culinary Returns & Cancellations
--------------------------------------------------------------------------------
Objective:
    Identify Culinary class orders paid with American Express (Amex) cards 
    that were returned, canceled, or had their classes canceled, to support 
    financial reconciliation and operational reporting.

Definition:
    - Amex Culinary Orders:
        • Culinary orders placed through Payment instrument type = 'Amex'
    - Return or Cancellation:
        • Line item flagged as returned or canceled in OMS
        • Culinary class flagged as canceled in the course catalog

Scope:
    - Includes Culinary product types only.
    - Aggregated at the order-line level for detailed refund tracking.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

select sfcc_order.sfcc_order_number sfcc_order_number
  , oms.order_number oms_order_number
  , sfcc_order.order_date date_ordered
  , oms.source
  , sfcc_order.customer_name customer_name
  , p.card_type
  , p.last_4 last_4_digits_CC
  , sales.line_id
  , sales.sku
  , min(line.product_name) product_name
  , min(line.product_type) product_type
  , culinary.class_type
  , culinary.start_date
  , culinary.end_date
  , sales.is_return
  , sales.is_canceled
  , culinary.is_class_cancelled
  , sales.quantity
  , sales.unit_price
  , sales.sub_total
from sfcc.sfcc_order
left join analytics.sales_header oms
  on oms.ecom_order_number = sfcc_order.sfcc_order_number
left join analytics.sales_line sales
  on sales.order_number = oms.order_number
left join sfcc.sfcc_order_item line 
  on (line.sfcc_order_number = sfcc_order.sfcc_order_number and line.sku = sales.sku)
left join sfcc.sfcc_payment_instrument p
  on p.sfcc_order_number = sfcc_order.sfcc_order_number
left join analytics.culinary_products culinary
  on sales.sku = culinary.sku
where p.card_type = 'Amex'
and line.product_type = 'Culinary'
and sfcc_order.order_date >= '2022-04-01'
and sfcc_order.order_date <= '2022-06-30'
group by sfcc_order.sfcc_order_number
  , oms.order_number
  , sfcc_order.order_date
  , oms.source
  , sfcc_order.customer_name
  , p.card_type
  , p.last_4
  , sales.line_id
  , sales.sku
  , culinary.class_type
  , culinary.start_date
  , culinary.end_date
  , sales.is_return
  , sales.is_canceled
  , culinary.is_class_cancelled
  , sales.quantity
  , sales.unit_price
  , sales.sub_total;
