/*
--------------------------------------------------------------------------------
SQL Script: New Customers – Rolling 52 Weeks
--------------------------------------------------------------------------------
Objective:
    Report new customers based on delivered sales over the Rolling 52 weeks,
    summarized at the fiscal week level with no channel duplication.

Definition:
    - New Customers: Customers whose first delivered sale occurs within the 
      R52 fiscal week window.

Scope:
    - Uses the corporate fiscal calendar.
    - Excludes Amazon channel transactions.
    - Excludes non-merchandise sales such as Gift Cards (GC) and Warranties.
    - New customers are counted without duplication across sales channels.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH R52_weeks AS (
-- get R52 weeks excluding current week
	SELECT DISTINCT common.calendar.fiscal_week_id
	FROM common.calendar
	WHERE common.calendar.fiscal_week_id < (
		SELECT common.calendar.fiscal_week_id 
		FROM common.calendar 
		WHERE common.calendar.gregorian_date = CURRENT_DATE
	)
	ORDER BY common.calendar.fiscal_week_id DESC
	LIMIT 52
)
SELECT count(email) as new_customers
FROM (
	SELECT peep.sales_header.email
	, min(peep.sales_header.date_ordered_analytics_id) as first_order_date_id
	FROM peep.written_sales
	LEFT JOIN peep.sales_header
		ON peep.sales_header.order_analytics_id = peep.written_sales.order_analytics_id  
	LEFT JOIN peep.sales_line
		ON peep.sales_line.order_line_analytics_id = peep.written_sales.order_line_analytics_id
	LEFT JOIN peep.product_to_merchandising_taxonomy
		ON peep.written_sales.product_analytics_id = peep.product_to_merchandising_taxonomy.product_analytics_id 
	WHERE peep.sales_header.email IS NOT NULL
	-- exclude gc & warranty
	AND peep.product_to_merchandising_taxonomy.level_4_name NOT LIKE 'GIFT CERTIFICATES'
	-- exclude amazon orders
	AND peep.sales_header.order_analytics_id NOT IN (
        SELECT DISTINCT peep.sales_line.order_analytics_id 
        FROM peep.sales_line 
		WHERE peep.sales_line.sales_channel LIKE '%amazon%'
	)
	GROUP BY peep.sales_header.email
) AS first_orders
LEFT JOIN common.calendar
	ON common.calendar.date_analytics_id = first_orders.first_order_date_id
WHERE (common.calendar.fiscal_week_id) IN (
    SELECT fiscal_week_id 
    FROM R52_weeks
);