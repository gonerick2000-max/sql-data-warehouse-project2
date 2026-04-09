/*
==========
GOLD LAYER
==========
Script Purpose:
This script creates the gold layer views following a dimensional model (star schema)
for analytical consumption.

The following objects are defined:
    - gold.dim_customers: customer dimension
    - gold.dim_products: product dimension
    - gold.fact_sales: sales fact table

Key features:
    - Integration of multiple data sources (CRM and ERP)
    - Creation of surrogate keys for dimensions
    - Business-friendly column naming
    - Handling of temporal product data (validity ranges)
*/




/*
Customer Dimension:
- Integrates customer data from CRM and ERP sources
- Resolves conflicting gender information using source priority
- Generates a surrogate key (Customer_key)
- Provides business-friendly column names
*/
CREATE OR ALTER VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY t1.cst_key) as Customer_key,
	t1.cst_id AS Customer_ID,
	t1.cst_firstname AS First_name,
	t1.cst_lastname AS Last_name,
	t3.CNTRY AS Country,
	t2.BDATE AS Birthdate,
	t1.cst_marital_status AS Marital_status,
	CASE WHEN t1.cst_gndr != 'n/a' THEN t1.cst_gndr
		 ELSE COALESCE(t2.GEN, 'n/a') END AS Gender,
	t1.cst_create_date AS Creation_date
FROM silver.crm_cust_info as t1
LEFT JOIN (
SELECT *
FROM silver.erp_CUST_AZ12
) as t2
ON t1.cst_key = t2.CID
LEFT JOIN (
SELECT *
FROM silver.erp_LOC_A101
) as t3
ON t1.cst_key = t3.CID;

GO

/*
Product Dimension:
- Combines product data from CRM and ERP sources
- Generates a surrogate key (Product_key)
- Provides business-friendly column names
*/
CREATE OR ALTER VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY t1.prd_start_dt, t1.prd_key) AS Product_key,
	t1.prd_key AS Product_number,
	t1.prd_nm AS Product_name,
	t1.ID_CAT,
	t2.CAT AS Category,
	t2.SUBCAT AS Subcategory,
	t1.prd_line AS Line,
	t2.MAINTENANCE AS Maintenance,
	t1.prd_cost AS Cost,
	t1.prd_start_dt AS Start_date,
	t1.prd_end_dt AS End_date
FROM silver.crm_prd_info as t1
LEFT JOIN (
SELECT *
FROM silver.erp_PX_CAT_G1V2
) as t2
ON t1.ID_CAT = t2.ID_CAT;

GO

/*
Sales Fact Table:
- Links transactional sales data with customer and product dimensions 
  using the surrogate keys
- Applies temporal joins to match products based on validity periods
- Handles missing product matches using the oldest record
- Provides business-friendly column names
*/
CREATE OR ALTER VIEW gold.fact_sales AS
WITH oldest_costs AS (
SELECT *
FROM (
SELECT *,
	ROW_NUMBER() OVER (PARTITION BY Product_number ORDER BY Start_date) as FLAG
FROM gold.dim_products
) AUX
WHERE AUX.FLAG = 1
)
SELECT 
	sls.sls_ord_num as Order_number,
	CASE WHEN pr.Product_key IS NOT NULL THEN pr.Product_key
		 ELSE oc.Product_key END AS Product_key,
	cu.Customer_key,
	sls.sls_order_dt as Order_date,
	sls.sls_ship_dt Ship_date,
	sls.sls_due_dt as Due_date,
	sls.sls_quantity as Quantity,
	sls.sls_price as Price,
	sls.sls_sales as Sales
FROM silver.crm_sales_details as sls
LEFT JOIN gold.dim_customers as cu
on sls_cust_id = cu.Customer_ID
LEFT JOIN gold.dim_products as pr
ON sls.sls_prd_key = pr.Product_number AND 
   (sls.sls_order_dt >= pr.Start_date AND (sls.sls_order_dt <= pr.End_date OR pr.End_date IS NULL))
LEFT JOIN oldest_costs as oc
on sls.sls_prd_key = oc.Product_number;
