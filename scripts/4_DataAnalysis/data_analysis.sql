/*
=============
DATA ANALYSIS
=============
Script Purpose:
This script performs analytical queries on the gold layer to extract 
business insights from the data warehouse.

The analysis includes:
	- Customer segmentation based on sales and activity (VIP, Silver, New)
	- Identification of top-performing product categories
	- Time-based analysis of order volume trends

The queries leverage aggregated metrics, window functions, and CTEs 
to support business decision-making.
*/



USE DataAnalytics;

/*
Customer Segmentation:
- Classifies customers based on total sales and activity duration
- Identifies VIP, Silver, and New customers using business rules
- Enriches data with customer demographics and favourite product categories
*/
WITH Customer_favourite AS (
/*
Determine customer's favourite product:
- Identifies the product with highest sales per customer
- Uses ROW_NUMBER() to rank products by sales
- Links product attributes for category analysis
*/
SELECT 
	t1.Customer_key,
	t1.Sales,
	t2.ID_CAT,
	t2.Category,
	t2.Subcategory,
	t2.Line
FROM (
SELECT 
	Customer_key,
	Product_key,
	SUM(Sales) AS Sales,
	ROW_NUMBER() OVER (PARTITION BY Customer_key ORDER BY SUM(Sales) DESC) as RN
FROM gold.fact_sales
GROUP BY Customer_key, Product_key
) as t1
LEFT JOIN gold.dim_products as t2
ON t1.Product_key = t2.Product_key
WHERE t1.RN = 1
),
Customer_details AS (
/*
Aggregate customer metrics:
- Calculates total sales and number of orders
- Computes years of activity
- Applies rules for customer classification (VIP, Silver or New_customer )
*/
SELECT 
	fs.Customer_key,
	CONCAT(dc.First_name, ' ', dc.Last_name) AS Customer_name,
	dc.Gender,
	dc.Country,
	SUM(fs.Sales) AS Total_sales,
	COUNT(DISTINCT fs.Order_number) AS Total_orders,
	DATEDIFF(YEAR,MIN(Order_date), MAX(Order_date)) AS Years_of_activity,
	CASE WHEN SUM(fs.Sales) >= 5000 AND DATEDIFF(YEAR,MIN(Order_date), MAX(Order_date)) >= 2 THEN 'VIP'
		 WHEN SUM(fs.Sales) >= 5000 AND DATEDIFF(YEAR,MIN(Order_date), MAX(Order_date)) < 2 THEN 'Silver'
		 ELSE 'New_customer' END AS Customer_status
	
FROM gold.fact_sales as fs
LEFT JOIN gold.dim_customers as dc
ON fs.Customer_key = dc.Customer_key
GROUP BY fs.Customer_key, dc.First_name, dc.Gender, dc.Last_name, dc.Country
),
table_customer_data AS (
SELECT 
	cd.Customer_key,
	cd.Customer_name,
	cd.Gender,
	cd.Country,
	cd.Total_sales,
	cd.Total_orders,
	cd.Years_of_activity,
	cd.Customer_status,
	cf.ID_CAT AS Favourite_category,
	cf.Line AS Favourite_line
FROM Customer_details AS cd
LEFT JOIN Customer_favourite as cf
ON cd.Customer_key = cf.Customer_key
)

/*SELECT 
	Favourite_line,
	CAST(100.0*COUNT(*)/SUM(COUNT(*)) OVER () AS DECIMAL(10,2))
FROM table_customer_data
GROUP BY Favourite_line
*/
SELECT 
	Customer_status,
	CAST(100.0*COUNT(*)/SUM(COUNT(*)) OVER () AS DECIMAL(10,2)) AS Percentage_total
FROM table_customer_data
GROUP BY Customer_status;
GO





/*
Product Performance Analysis:
- Aggregates total sales and quantity by product.
- Supports identification of top-performing categories
*/

WITH product_table AS (
SELECT
	fs.Product_key,
	dp.Product_name,
	dp.ID_CAT,
	dp.Category,
	dp.Subcategory,
	SUM(fs.Quantity) AS Total_quantity,
	SUM(fs.Sales) AS Total_sales
FROM gold.fact_sales as fs
LEFT JOIN gold.dim_products as dp
ON fs.Product_key = dp.Product_key
GROUP BY fs.Product_key, dp.Product_name,
	dp.Product_name,
	dp.ID_CAT,
	dp.Category,
	dp.Subcategory
)
SELECT
	ID_CAT,
	Category,
	Subcategory,
	CAST(100.0*SUM(Total_sales)/SUM(SUM(Total_sales)) OVER () AS DECIMAL(10,2)) AS Pctg_sales
FROM product_table
GROUP BY ID_CAT,
	Category,
	Subcategory;
GO



/*
Time Series Analysis:
- Uses window functions to analyze order trends over time
*/
SELECT DISTINCT
	DATETRUNC(MONTH,Order_date) AS Month_date,
	COUNT(*) OVER (ORDER BY DATETRUNC(MONTH,Order_date)) as Num_Orders
FROM gold.fact_sales
ORDER BY DATETRUNC(MONTH,Order_date)
