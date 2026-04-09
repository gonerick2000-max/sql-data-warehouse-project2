/*
==================================
DATA VALIDATION (SILVER LAYER)
==================================
Script Purpose:
This script performs data quality checks on the silver layer to ensure that transformations 
from the bronze layer were applied correctly.
*/


USE DataWareHouse;
-- EXEC bronze.load_val;
-- EXEC silver.load_val;

/*
Customer data validation:
- Detect NULL or duplicate customer IDs
- Verify standardization of marital status and gender values
*/
SELECT 
	cst_id,
	count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id is null

SELECT 
	cst_marital_status
FROM silver.crm_cust_info
GROUP BY cst_marital_status

SELECT 
	cst_gndr
FROM silver.crm_cust_info
GROUP BY cst_gndr

--
/*
Product data validation:
- Verify standardization of product line categories
- Ensure start date is always earlier than end date
*/
SELECT DISTINCT
	prd_line
FROM silver.crm_prd_info

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--
/*
Sales data validation:
- Ensure sales amount equals quantity * price
- Validate average shipping time (expected ~7 days)
*/

SELECT *
FROM silver.crm_sales_details
WHERE sls_quantity*sls_price != sls_sales

SELECT 
	AVG(DATEDIFF(DAY, sls_order_dt, sls_ship_dt)) AS avg_days_to_ship
FROM silver.crm_sales_details

--
/*
ERP customer validation:
- Ensure ID prefixes have been removed
- Validate birth dates are earlier than the current date.
- Verify gender standardization
*/

SELECT DISTINCT
	SUBSTRING(CID, 1, 3)
FROM silver.erp_CUST_AZ12

SELECT *
FROM silver.erp_CUST_AZ12
WHERE BDATE > GETDATE()

SELECT DISTINCT 
	GEN
FROM silver.erp_CUST_AZ12

--
/*
ERP location validation:
- Ensure customer IDs are cleaned
- Verify country values are standardized
*/

SELECT DISTINCT
	SUBSTRING(CID, 1, 3)
FROM silver.erp_LOC_A101

SELECT DISTINCT
	CNTRY
FROM SILVER.erp_LOC_A101
