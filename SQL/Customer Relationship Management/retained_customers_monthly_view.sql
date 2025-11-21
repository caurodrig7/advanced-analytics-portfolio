/*
--------------------------------------------------------------------------------
SQL Script: Retained Customers – Monthly View
--------------------------------------------------------------------------------
Objective:
    Report retained customers based on delivered sales,
    summarized at the monthly level with no channel duplication.

Definition:
    - Retained Customers: Purchases in both the current and prior fiscal years.

Scope:
    - Excludes Amazon channel transactions.
    - Excludes non-merchandise sales such as Gift Cards and Warranties.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH monthly_customers AS (
    -- Get distinct customers who purchased in a given month and year
    SELECT DISTINCT peep.sales_header.email, 
           YEAR(peep.sales_header.date_ordered) AS order_year, 
           MONTH(peep.sales_header.date_ordered) AS order_month
    FROM peep.delivered_sales
	LEFT JOIN peep.sales_header
		ON peep.sales_header.order_analytics_id = peep.delivered_sales.order_analytics_id  
	LEFT JOIN peep.sales_line
		ON ((peep.sales_line.order_analytics_id = peep.delivered_sales.order_analytics_id) AND (peep.sales_line.order_line_analytics_id = peep.delivered_sales.order_line_analytics_id))
	WHERE peep.sales_header.email IS NOT NULL
		AND (
				(peep.sales_line.'source' = 'xcenter' AND 
					(peep.sales_line.order_line_type = 'in_store_sale')
				) OR
				(peep.sales_line.'source' = 'oroms' AND 
    				( 
         				(peep.sales_line.sales_channel = 'web' AND peep.sales_line.order_line_type = 'ecommerce') OR 
         				(peep.sales_line.sales_channel = 'culinary_orders' AND peep.sales_line.order_line_type = 'ecommerce') OR
         				(peep.sales_line.sales_channel = 'slt_bopis' AND peep.sales_line.order_line_type = 'bopis') OR 
         				(peep.sales_line.sales_channel = 'walmart_go_local' AND peep.sales_line.order_line_type = 'bopis') OR
         				(peep.sales_line.sales_channel = 'customer_service' AND peep.sales_line.order_line_type = 'phone') OR
         				(peep.sales_line.sales_channel = 'culinary_orders' AND peep.sales_line.order_line_type = 'phone')
    				)
				) 
			)
),
yearly_customers AS (
    -- Get distinct customers who made a purchase in a given year
    SELECT DISTINCT peep.sales_header.email, 
    YEAR(peep.sales_header.date_ordered) AS order_year
    FROM peep.delivered_sales
	LEFT JOIN peep.sales_header
		ON peep.sales_header.order_analytics_id = peep.delivered_sales.order_analytics_id  
	LEFT JOIN peep.sales_line
		ON ((peep.sales_line.order_analytics_id = peep.delivered_sales.order_analytics_id) AND (peep.sales_line.order_line_analytics_id = peep.delivered_sales.order_line_analytics_id))
	WHERE peep.sales_header.email IS NOT NULL
		AND (
				(peep.sales_line.'source' = 'xcenter' AND 
					(peep.sales_line.order_line_type = 'in_store_sale')
				) OR
				(peep.sales_line.'source' = 'oroms' AND 
    				( 
         				(peep.sales_line.sales_channel = 'web' AND peep.sales_line.order_line_type = 'ecommerce') OR 
         				(peep.sales_line.sales_channel = 'culinary_orders' AND peep.sales_line.order_line_type = 'ecommerce') OR
         				(peep.sales_line.sales_channel = 'slt_bopis' AND peep.sales_line.order_line_type = 'bopis') OR 
         				(peep.sales_line.sales_channel = 'walmart_go_local' AND peep.sales_line.order_line_type = 'bopis') OR
         				(peep.sales_line.sales_channel = 'customer_service' AND peep.sales_line.order_line_type = 'phone') OR
         				(peep.sales_line.sales_channel = 'culinary_orders' AND peep.sales_line.order_line_type = 'phone')
    				)
				) 
			)
),
retained_customers AS (
    -- Find customers who purchased in month M of year Y and also in any month of year Y-1
    SELECT 
        mc.order_year AS year, 
        mc.order_month AS month, 
        COUNT(DISTINCT mc.email) AS retained_customers
    FROM monthly_customers mc
    JOIN yearly_customers yc 
        ON mc.email = yc.email  -- Same customer
        AND mc.order_year = yc.order_year + 1  -- Current year and previous year match
    GROUP BY mc.order_year, mc.order_month
)
SELECT * 
FROM retained_customers
ORDER BY year, 
	month;