/*
--------------------------------------------------------------------------------
SQL Script: Customer Segmentation by Sales Channel, Product type and Fiscal Quarter
--------------------------------------------------------------------------------
Objective:
    Classify customers by engagement status to measure lifecycle behavior 
    across Retail and Online channels, HardGood and Cooking classes product types.

Definition:
    This query assigns each customer to one of the following categories:
        - New: First purchase within the selected fiscal year.
        - Retained: Purchases in both the current and prior fiscal years.
        - Reactivated: Returned after at least one fiscal year of inactivity.
        - Anonymous: Orders with no identifiable customer email or ID.

Scope:
    - Includes Retail and Online channel views.
    - Grouped by Product types HardGoods and Cooking classes.
    - Aggregated at the Fiscal Quarter level.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

-- fiscal calendar mapping
WITH fiscal_calendar AS (
    SELECT common.calendar.fiscal_year,
        common.calendar.fiscal_quarter_id,
        common.calendar.quarter_analytics_id,
        common.calendar.fiscal_month_id,
        common.calendar.month_analytics_id,
        common.calendar.fiscal_week_id,
        common.calendar.week_analytics_id,
        MIN(common.calendar.fiscal_date_id) AS start_date_fiscal_week,
        MIN(common.calendar.date_analytics_id) AS start_date_id_fiscal_week,
        MAX(common.calendar.fiscal_date_id) AS end_date_fiscal_week,
        MAX(common.calendar.date_analytics_id) AS end_date_id_fiscal_week
    FROM common.calendar
    GROUP BY 
        common.calendar.fiscal_year,
        common.calendar.fiscal_quarter_id,
        common.calendar.quarter_analytics_id,
        common.calendar.fiscal_month_id,
        common.calendar.month_analytics_id,
        common.calendar.fiscal_week_id,
        common.calendar.week_analytics_id
),
-- assign an ID to not null Emails
email_identifier AS (                                                      
    SELECT distinct_emails.email,
        DENSE_RANK() OVER (ORDER BY distinct_emails.email) AS email_id
    FROM (
        SELECT DISTINCT peep.sales_header.email
        FROM peep.sales_header
        WHERE peep.sales_header.email IS NOT NULL
    ) AS distinct_emails
),
-- enrich xcenter bopis orders with oroms bopis email
sales_header_with_enriched_email AS (
    SELECT peep.sales_header.order_analytics_id,
        peep.sales_header.source,
        peep.sales_header.order_type,
        COALESCE(peep.sales_header.email, online.email) AS enriched_email
    FROM peep.sales_header
    LEFT JOIN peep.sales_header AS online
        ON peep.sales_header.alt_order_number_1 = online.order_number
        AND (
            (peep.sales_header.source = 'xcenter' AND peep.sales_header.order_type = 'bopis' AND online.source = 'oroms' AND online.order_type LIKE '%bopis%')
            --OR
            --(peep.sales_header.source = 'xcenter' AND peep.sales_header.order_type = 'ship_from_store' AND online.source = 'oroms' AND online.order_type = 'ecommerce order')    
        )
),
-- orders with email linked to calendar by channel
customers_by_quarter AS (
    SELECT DISTINCT 
        sales_header_with_enriched_email.enriched_email AS email,
        common.calendar.fiscal_quarter_id,
        common.calendar.quarter_analytics_id,
        CASE 
            WHEN sales_header_with_enriched_email.source = 'oroms'
              OR sales_header_with_enriched_email.order_type LIKE '%bopis%'
              OR sales_header_with_enriched_email.order_type = 'ship_from_store' THEN 'Direct'
            WHEN sales_header_with_enriched_email.source = 'xcenter' THEN 'Retail'
        END AS sales_channel_type
    FROM peep.delivered_sales
    JOIN sales_header_with_enriched_email
        ON sales_header_with_enriched_email.order_analytics_id = peep.delivered_sales.order_analytics_id
    JOIN peep.sales_line
        ON peep.sales_line.order_line_analytics_id = peep.delivered_sales.order_line_analytics_id
    JOIN peep.product_to_merchandising_taxonomy
        ON peep.product_to_merchandising_taxonomy.product_analytics_id = peep.delivered_sales.product_analytics_id
    JOIN common.calendar
        ON common.calendar.date_analytics_id = peep.delivered_sales.date_ordered_analytics_id
    WHERE sales_header_with_enriched_email.enriched_email IS NOT NULL
      -- exclude non-customer emails
      AND sales_header_with_enriched_email.enriched_email NOT IN (
          SELECT peep.non_customer_emails.email
          FROM peep.non_customer_emails
      )
        -- exclude amazon customers
        AND peep.sales_line.order_analytics_id NOT IN (
            SELECT DISTINCT peep.sales_line.order_analytics_id 
            FROM peep.sales_line 
            WHERE peep.sales_line.sales_channel LIKE '%amazon%' 
               OR peep.sales_line.sales_channel LIKE '%amzbopis%' 
               OR peep.sales_line.order_line_type LIKE '%amazon%'
        )
        -- exclude gc & warranty only customers
        -- AND peep.product_to_merchandising_taxonomy.level_4_name NOT LIKE 'GIFT CERTIFICATES'
),
-- link first order quarter
customer_first_quarter AS (
    SELECT 
        customers_by_quarter.email, 
        customers_by_quarter.sales_channel_type,
        MIN(customers_by_quarter.quarter_analytics_id) AS first_quarter_analytics_id
    FROM customers_by_quarter
    GROUP BY customers_by_quarter.email, customers_by_quarter.sales_channel_type
),
-- retention window: customers active in previous 4 quarters 
orders_last_4_quarters AS (
    SELECT a.email, 
           a.sales_channel_type,
           b.quarter_analytics_id AS target_quarter
    FROM customers_by_quarter a
    JOIN customers_by_quarter b
        ON a.quarter_analytics_id BETWEEN b.quarter_analytics_id - 4 AND b.quarter_analytics_id - 1
        AND a.email = b.email
        AND a.sales_channel_type = b.sales_channel_type
),
-- reactivation logic: prior to previous 4 quarters 
orders_before_last_4_quarters AS (
    SELECT a.email, 
           a.sales_channel_type,
           b.quarter_analytics_id AS target_quarter
    FROM customers_by_quarter a
    JOIN customers_by_quarter b
        ON a.quarter_analytics_id < b.quarter_analytics_id - 4
        AND a.email = b.email
        AND a.sales_channel_type = b.sales_channel_type
),
-- classification per customer per quarter 
classified_customers AS (
    SELECT DISTINCT 
        customers_by_quarter.email,
        customers_by_quarter.fiscal_quarter_id,
        customers_by_quarter.quarter_analytics_id,
        customers_by_quarter.sales_channel_type,
        CASE
            WHEN customers_by_quarter.quarter_analytics_id = customer_first_quarter.first_quarter_analytics_id THEN 'New'
            WHEN orders_last_4_quarters.email IS NOT NULL THEN 'Retained'
            WHEN orders_before_last_4_quarters.email IS NOT NULL AND orders_last_4_quarters.email IS NULL THEN 'Reactivated'
            ELSE 'Unclassified'
        END AS customer_type
    FROM customers_by_quarter
    JOIN customer_first_quarter 
        ON customers_by_quarter.email = customer_first_quarter.email
       AND customers_by_quarter.sales_channel_type = customer_first_quarter.sales_channel_type
    LEFT JOIN orders_last_4_quarters
        ON ((customers_by_quarter.email = orders_last_4_quarters.email) 
        AND (customers_by_quarter.sales_channel_type = orders_last_4_quarters.sales_channel_type)
        AND (customers_by_quarter.quarter_analytics_id = orders_last_4_quarters.target_quarter))
    LEFT JOIN orders_before_last_4_quarters
        ON ((customers_by_quarter.email = orders_before_last_4_quarters.email) 
        AND (customers_by_quarter.sales_channel_type = orders_before_last_4_quarters.sales_channel_type)
        AND (customers_by_quarter.quarter_analytics_id = orders_before_last_4_quarters.target_quarter))
),
-- join customer classification to order data (PROPAGATE CHANNEL)
orders_with_classification AS (
    SELECT classified_customers.fiscal_quarter_id,
        classified_customers.sales_channel_type,
        classified_customers.customer_type,
        peep.delivered_sales.order_analytics_id,
        CASE                                                                                                        
    		WHEN peep.product_to_merchandising_taxonomy.level_5_name = 'WARRANTY' THEN 'Warranty'                   
    		WHEN peep.product_to_merchandising_taxonomy.level_3_name = 'GIFT CERTIFICATES' THEN 'GiftCard'          
    		WHEN peep.product_to_merchandising_taxonomy.level_3_name = 'COOKING SCHOOL' THEN 'CulinaryClass'       
    		ELSE 'HardGood'													
		END AS product_category_flag,                                                                               
        peep.delivered_sales.merchandise,
        peep.delivered_sales.quantity,
        classified_customers.email
    FROM peep.delivered_sales
    JOIN sales_header_with_enriched_email
        ON peep.delivered_sales.order_analytics_id = sales_header_with_enriched_email.order_analytics_id
    JOIN peep.sales_line
        ON peep.sales_line.order_line_analytics_id = peep.delivered_sales.order_line_analytics_id
    JOIN peep.product_to_merchandising_taxonomy
        ON peep.product_to_merchandising_taxonomy.product_analytics_id = peep.delivered_sales.product_analytics_id
    JOIN common.calendar
        ON common.calendar.date_analytics_id = peep.delivered_sales.date_ordered_analytics_id
    JOIN classified_customers
        ON (classified_customers.email = sales_header_with_enriched_email.enriched_email) 
       AND (classified_customers.fiscal_quarter_id = common.calendar.fiscal_quarter_id)
       AND (classified_customers.sales_channel_type = CASE 
            WHEN sales_header_with_enriched_email.source = 'oroms'
              OR sales_header_with_enriched_email.order_type LIKE '%bopis%'
              OR sales_header_with_enriched_email.order_type = 'ship_from_store' THEN 'Direct'
            WHEN sales_header_with_enriched_email.source = 'xcenter' THEN 'Retail'
        END)
    WHERE sales_header_with_enriched_email.enriched_email IS NOT NULL
    	-- exclude amazon orders
        AND peep.sales_line.order_analytics_id NOT IN (
            SELECT DISTINCT peep.sales_line.order_analytics_id 
            FROM peep.sales_line 
            WHERE peep.sales_line.sales_channel LIKE '%amazon%' 
               OR peep.sales_line.sales_channel LIKE '%amzbopis%' 
               OR peep.sales_line.order_line_type LIKE '%amazon%'
        )
        -- exclude gc & warranty orders
        -- AND peep.product_to_merchandising_taxonomy.level_4_name NOT LIKE 'GIFT CERTIFICATES'
),
-- orders without email (PROPAGATE CHANNEL)
anonymous_orders AS (
    SELECT 
        common.calendar.fiscal_quarter_id,
        CASE 
            WHEN sales_header_with_enriched_email.source = 'oroms'
              OR sales_header_with_enriched_email.order_type LIKE '%bopis%'
              OR sales_header_with_enriched_email.order_type = 'ship_from_store' THEN 'Direct'
            WHEN sales_header_with_enriched_email.source = 'xcenter' THEN 'Retail'
        END AS sales_channel_type,
        'Anonymous' AS customer_type,
        peep.delivered_sales.order_analytics_id,
        CASE                                                                                                         
    		WHEN peep.product_to_merchandising_taxonomy.level_5_name = 'WARRANTY' THEN 'Warranty'                   
    		WHEN peep.product_to_merchandising_taxonomy.level_3_name = 'GIFT CERTIFICATES' THEN 'GiftCard'           
    		WHEN peep.product_to_merchandising_taxonomy.level_3_name = 'COOKING SCHOOL' THEN 'CulinaryClass'        
    		ELSE 'HardGood'																						     
		END AS product_category_flag, 
        peep.delivered_sales.merchandise,
        peep.delivered_sales.quantity,
        NULL AS email
    FROM peep.delivered_sales
    JOIN sales_header_with_enriched_email
        ON peep.delivered_sales.order_analytics_id = sales_header_with_enriched_email.order_analytics_id
    JOIN peep.sales_line
        ON peep.sales_line.order_line_analytics_id = peep.delivered_sales.order_line_analytics_id
    JOIN peep.product_to_merchandising_taxonomy
        ON peep.product_to_merchandising_taxonomy.product_analytics_id = peep.delivered_sales.product_analytics_id
    JOIN common.calendar
        ON common.calendar.date_analytics_id = peep.delivered_sales.date_ordered_analytics_id
    WHERE (
		sales_header_with_enriched_email.enriched_email IS NULL
		-- include non-customer emails
		OR sales_header_with_enriched_email.enriched_email IN (
			SELECT peep.non_customer_emails.email
            FROM peep.non_customer_emails
            )
        )
    	-- exclude amazon orders
        AND peep.sales_line.order_analytics_id NOT IN (
            SELECT DISTINCT peep.sales_line.order_analytics_id 
            FROM peep.sales_line 
            WHERE peep.sales_line.sales_channel LIKE '%amazon%' 
               OR peep.sales_line.sales_channel LIKE '%amzbopis%' 
               OR peep.sales_line.order_line_type LIKE '%amazon%'
        )
        -- exclude gc & warranty only customers
        -- AND peep.product_to_merchandising_taxonomy.level_4_name NOT LIKE 'GIFT CERTIFICATES'
)
, reporting_date AS (
SELECT 
fw.fiscal_year
,fw.fiscal_quarter_id
,fw.quarter_analytics_id
,max(c.date_analytics_id) date_analytics_id
,max(c.gregorian_date) gregorian_date
from common.fiscal_weeks fw
join common.calendar c
on fw.fiscal_week_id = c.fiscal_week_id
where fw.fiscal_year between 2019 and 2026
and fw.week_in_quarter = 2
group by 1,2,3
order by 2
)
-- aggregate (now BY CHANNEL as well)
SELECT 
    combined.fiscal_quarter_id,
    rd.date_analytics_id,
    combined.sales_channel_type,
    combined.customer_type,
    -- COUNT(DISTINCT COALESCE(combined.email, CAST(combined.order_analytics_id AS VARCHAR))) AS customer_count_by_month,
    COALESCE(CAST(email_identifier.email_id AS VARCHAR), CAST(combined.order_analytics_id AS VARCHAR)) AS customer_identifier, 
    combined.order_analytics_id,                                      
    combined.product_category_flag,										
    --COUNT(DISTINCT combined.order_analytics_id) AS order_count_by_month,
    SUM(combined.merchandise) AS total_order_value_by_quarter,
    SUM(combined.quantity) AS total_units_by_quarter
FROM (
    SELECT * 
    FROM orders_with_classification
    UNION ALL
    SELECT * 
    FROM anonymous_orders
) AS combined
JOIN reporting_date rd
ON rd.fiscal_quarter_id = combined.fiscal_quarter_id
LEFT JOIN email_identifier                                   
	ON combined.email = email_identifier.email               
-- WHERE combined.fiscal_quarter_id BETWEEN '20251' AND '20251'  
GROUP BY 1,2,3,4,5,6,7;