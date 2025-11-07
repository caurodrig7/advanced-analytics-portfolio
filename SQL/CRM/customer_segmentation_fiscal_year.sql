/*
SQL: Customer Segmentation Query
Objective: Classify customers by engagement status to measure customer lifecycle behavior across the total business
Definition: This query categorizes each customer as one of the following:
    - New: First purchase within the selected fiscal year.
    - Retained: Has made purchases in both the current and prior fiscal years.
    - Reactivated: Has returned after at least one fiscal year of inactivity.
    - Anonymous: Orders with no identifiable customer email or ID.
Scope:
    - Includes all channels and lines of business (total business view).
    - Aggregated by Fiscal Year.
Author: Cesar Rodriguez
*/

-- email & order year
WITH customers_by_year AS (
    SELECT DISTINCT peep.sales_header.email,
        common.calendar.fiscal_year AS fiscal_year
    FROM peep.written_sales
    JOIN peep.sales_header
    	ON peep.sales_header.order_analytics_id = peep.written_sales.order_analytics_id
    JOIN peep.sales_line
    	ON peep.sales_line.order_line_analytics_id = peep.written_sales.order_line_analytics_id 
    JOIN peep.product_to_merchandising_taxonomy
		ON peep.product_to_merchandising_taxonomy.product_analytics_id = peep.written_sales.product_analytics_id
	JOIN common.calendar
        ON common.calendar.date_analytics_id = peep.written_sales.date_ordered_analytics_id
    WHERE peep.sales_header.email IS NOT NULL
    	-- exclude amazon customers
		AND peep.sales_line.order_analytics_id NOT IN (
        	SELECT DISTINCT peep.sales_line.order_analytics_id 
        	FROM peep.sales_line 
			WHERE ((peep.sales_line.sales_channel LIKE '%amazon%') OR (peep.sales_line.sales_channel LIKE '%amzbopis%') OR (peep.sales_line.order_line_type LIKE '%amazon%'))
		)
		-- exclude gc & warranty only customers
		AND peep.product_to_merchandising_taxonomy.level_4_name NOT LIKE 'GIFT CERTIFICATES'
),
-- years with orders
all_years AS (
    SELECT DISTINCT customers_by_year.fiscal_year
    FROM customers_by_year
),
-- link first order year
customer_first_order AS (
    SELECT DISTINCT customers_by_year.email, 
        MIN(customers_by_year.fiscal_year) AS first_order_year
    FROM customers_by_year
    GROUP BY customers_by_year.email
),
-- link order year X & order year X+1
orders_prev_year AS (
    SELECT DISTINCT customers_by_year.email, 
    customers_by_year.fiscal_year + 1 AS target_year
    FROM customers_by_year
),
-- link order year X & order year > X+1
orders_before_prev_year AS (
    SELECT DISTINCT customers_by_year.email, 
    all_years.fiscal_year AS target_year
    FROM customers_by_year
    JOIN all_years
    	ON customers_by_year.fiscal_year < all_years.fiscal_year - 1
)
-- classification per customer per year
, classified_customers AS (
    SELECT DISTINCT customers_by_year.email,
        customers_by_year.fiscal_year AS fiscal_year,
        CASE
            WHEN (customer_first_order.first_order_year = customers_by_year.fiscal_year) THEN 'New customer'
            WHEN (orders_prev_year.email IS NOT NULL) THEN 'Retained customer'
            WHEN (orders_before_prev_year.email IS NOT NULL AND orders_prev_year.email IS NULL) THEN 'Reactivated customer'
            ELSE 'Unclassified customer'
        END AS customer_type
    FROM customers_by_year
    JOIN customer_first_order 
        ON customers_by_year.email = customer_first_order.email
    LEFT JOIN orders_prev_year
        ON ((customers_by_year.email = orders_prev_year.email) AND (customers_by_year.fiscal_year = orders_prev_year.target_year))
    LEFT JOIN orders_before_prev_year
        ON ((customers_by_year.email = orders_before_prev_year.email) AND (customers_by_year.fiscal_year = orders_before_prev_year.target_year))
),
-- join customer classification to order data
orders_with_classification AS (
    SELECT classified_customers.fiscal_year,
        classified_customers.customer_type,
        peep.written_sales.order_analytics_id,
        peep.written_sales.merchandise,
        peep.written_sales.quantity,
        peep.sales_header.email
    FROM peep.written_sales
    JOIN peep.sales_header
    	ON peep.written_sales.order_analytics_id = peep.sales_header.order_analytics_id
    JOIN peep.sales_line
    	ON peep.sales_line.order_line_analytics_id = peep.written_sales.order_line_analytics_id
    JOIN peep.product_to_merchandising_taxonomy
		ON peep.product_to_merchandising_taxonomy.product_analytics_id = peep.written_sales.product_analytics_id
    JOIN common.calendar
        ON common.calendar.date_analytics_id = peep.written_sales.date_ordered_analytics_id
    JOIN classified_customers
        --ON ((classified_customers.email = peep.sales_header.email) AND (classified_customers.year = YEAR(peep.sales_header.date_ordered)))
    	ON ((classified_customers.email = peep.sales_header.email) AND (classified_customers.fiscal_year = common.calendar.fiscal_year))  
    WHERE peep.sales_header.email IS NOT NULL
    	-- exclude amazon orders
    	AND peep.sales_line.order_analytics_id NOT IN (
        	SELECT DISTINCT peep.sales_line.order_analytics_id 
        	FROM peep.sales_line 
			WHERE ((peep.sales_line.sales_channel LIKE '%amazon%') OR (peep.sales_line.sales_channel LIKE '%amzbopis%') OR (peep.sales_line.order_line_type LIKE '%amazon%'))
		)  
		-- exclude gc & warranty orders
		AND peep.product_to_merchandising_taxonomy.level_4_name NOT LIKE 'GIFT CERTIFICATES'
),
-- orders without email
anonymous_orders AS (
    SELECT common.calendar.fiscal_year,
        'Anonymous customer' AS customer_type,
        peep.written_sales.order_analytics_id,
        peep.written_sales.merchandise,
        peep.written_sales.quantity,
        NULL AS email
    FROM peep.written_sales
    JOIN peep.sales_header
        ON peep.written_sales.order_analytics_id = peep.sales_header.order_analytics_id
    JOIN peep.sales_line
        ON peep.sales_line.order_line_analytics_id = peep.written_sales.order_line_analytics_id
    JOIN peep.product_to_merchandising_taxonomy
        ON peep.product_to_merchandising_taxonomy.product_analytics_id = peep.written_sales.product_analytics_id
    JOIN common.calendar
        ON common.calendar.date_analytics_id = peep.written_sales.date_ordered_analytics_id
    WHERE peep.sales_header.email IS NULL
        -- exclude amazon orders
        AND peep.sales_line.order_analytics_id NOT IN (
            SELECT DISTINCT peep.sales_line.order_analytics_id 
            FROM peep.sales_line 
            WHERE peep.sales_line.sales_channel LIKE '%amazon%' 
               OR peep.sales_line.sales_channel LIKE '%amzbopis%' 
               OR peep.sales_line.order_line_type LIKE '%amazon%'
        )  
        -- exclude gc & warranty only customers
        AND peep.product_to_merchandising_taxonomy.level_4_name NOT LIKE 'GIFT CERTIFICATES'
)
-- aggregate count of customers, count of orders, count of unit, order value per fiscal year per customer type
SELECT combined.fiscal_year,
    combined.customer_type,
    COUNT(DISTINCT COALESCE(email, CAST(order_analytics_id AS VARCHAR))) AS customer_count,
    COUNT(DISTINCT combined.order_analytics_id) AS order_count,
    SUM(combined.merchandise) AS total_order_value,
    SUM(combined.quantity) AS total_units
FROM (
    SELECT * 
    FROM orders_with_classification
    UNION ALL
    SELECT * 
    FROM anonymous_orders
) AS combined
GROUP BY combined.fiscal_year, 
	combined.customer_type
ORDER BY combined.fiscal_year, 
	combined.customer_type;