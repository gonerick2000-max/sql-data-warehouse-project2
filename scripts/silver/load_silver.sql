/*
===================================
TRANSFORM AND LOAD DATA INTO SILVER
===================================
Script Purpose:
This script transforms and loads data from the bronze layer into the silver layer.

- Data cleansing (TRIM, NULL handling, invalid value filtering)
- Data deduplication using window functions (ROW_NUMBER)
- Standardization of categorical values (e.g., gender, status)
- Data type corrections and formatting (e.g., date conversion)
- Basic business logic and enrichment (e.g., price correction, derived sales)

Both CRM and ERP datasets are processed and loaded into structured, analysis-ready 
tables in the silver layer.
*/


CREATE OR ALTER PROCEDURE silver.load_val AS
BEGIN
BEGIN TRY
	PRINT 'LOADING SILVER TABLES'
	DECLARE @start_time DATETIME, @end_time DATETIME
	SET @start_time = GETDATE()

/* -------- CRM TABLES -------- */
/* 
Clean and deduplicate customer data (bronze.crm_cust_info table):
- Removes duplicates using ROW_NUMBER (keeps most recent record)
- Standardizes gender and marital status values
- Trims string fields
- Filters out invalid customer IDs
*/
	TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN cst_marital_status = 'M' THEN 'Married'
			 WHEN cst_marital_status = 'S' THEN 'Single'
			 ELSE 'n/a' END AS cst_marital_status,
		CASE WHEN cst_gndr = 'M' THEN 'Male'
			 WHEN cst_gndr = 'F' THEN 'Female'
			 ELSE 'n/a' END AS cst_gndr,
		cst_create_date
	FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
	FROM bronze.crm_cust_info
	) AS t1
	WHERE t1.flag = 1 AND t1.cst_id is not null

	PRINT 'Table silver.crm_cust_info loaded';

/* 
Transform product data (bronze.crm_prd_info table):
- Extracts category ID and product key from composite key
- Standardizes product line categories
- Trims text fields
- Generates end date using LEAD() for slowly changing structure
*/
	TRUNCATE TABLE silver.crm_prd_info
	INSERT INTO silver.crm_prd_info (
		ID_CAT,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT 
		REPLACE(TRIM(SUBSTRING(prd_key, 1, 5)), '-', '_') as ID_CAT,
		TRIM(SUBSTRING(prd_key, 7, len(prd_key))) as prd_key,
		TRIM(prd_nm) AS prd_nm,
		prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring' 
			 ELSE 'n/a' END AS prd_line,
			 prd_start_dt,
			 DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY TRIM(SUBSTRING(prd_key, 7, len(prd_key))) ORDER BY prd_start_dt)) as prd_end_dt
	FROM bronze.crm_prd_info;
	PRINT 'Table silver.crm_prd_info loaded';

/* 
Clean and enrich sales data (bronze.crm_sales_details table):
- Validates and converts date fields from raw format
- Handles missing or invalid dates
- Imputes missing prices using previous values (LAG)
- Recalculates sales amount when needed
- Ensures data consistency for analytical use
*/
	TRUNCATE TABLE silver.crm_sales_details;
	DECLARE @avg_days INT = -7;
	WITH clean AS (
		SELECT
			TRIM(sls_ord_num) AS sls_ord_num,
			TRIM(sls_prd_key) AS sls_prd_key,
			sls_cust_id,
			CASE WHEN LEN(CAST(sls_order_dt AS NVARCHAR)) != 8 THEN NULL
				 ELSE CONVERT(DATE, CAST(sls_order_dt AS CHAR(8))) END AS sls_order_dt,
			CASE WHEN LEN(CAST(sls_ship_dt AS NVARCHAR)) != 8 THEN NULL
				 ELSE CONVERT(DATE, CAST(sls_ship_dt AS CHAR(8))) END AS sls_ship_dt,
			CASE WHEN LEN(CAST(sls_due_dt AS NVARCHAR)) != 8 THEN NULL
				 ELSE CONVERT(DATE, CAST(sls_due_dt AS CHAR(8))) END AS sls_due_dt,
			sls_quantity,
			sls_price,
			CASE WHEN sls_sales < 1 THEN NULL
				 ELSE sls_sales END as sls_sales
		FROM bronze.crm_sales_details
	),
	lagged AS (
		SELECT *,
			LAG(sls_price) OVER (PARTITION BY sls_prd_key ORDER BY sls_order_dt) AS prev_price
		FROM clean
	)
	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_quantity,
		sls_price,
		sls_sales
	)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt IS NOT NULL THEN sls_order_dt
			 ELSE DATEADD(DAY, @avg_days, sls_ship_dt) END as sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 1 THEN prev_price
			 ELSE sls_price END AS sls_price,
		CASE WHEN sls_price IS NULL OR sls_price <= 1 THEN prev_price*sls_quantity
			 ELSE sls_price*sls_quantity END AS sls_sales
	FROM lagged

	PRINT 'Table silver.crm_sales_details loaded';

/* -------- ERP TABLES -------- */
/* 
Standardize ERP customer data (bronze.erp_CUST_AZ12 table):
- Cleans customer IDs (removes prefixes)
- Validates birth dates
- Normalizes gender values
*/
	TRUNCATE TABLE silver.erp_CUST_AZ12;
	INSERT INTO silver.erp_CUST_AZ12 (
		CID,
		BDATE,
		GEN
	)
	SELECT 
		CASE WHEN SUBSTRING(CID,1,3) = 'NAS' THEN SUBSTRING(CID,4,len(CID))
			 ELSE CID END AS CID,
			 CASE WHEN BDATE > GETDATE() THEN NULL
				  ELSE BDATE END AS BDATE,
			 CASE WHEN TRIM(GEN) = 'F' THEN 'Female'
				  WHEN TRIM(GEN) = 'M' THEN 'Male'
				  WHEN TRIM(GEN) = '' OR GEN IS NULL THEN 'n/a'
				  ELSE TRIM(GEN) END AS GEN
	FROM bronze.erp_CUST_AZ12;

	PRINT 'Table silver.erp_CUST_AZ12 loaded';

/* 
Standardize location data (bronze.erp_LOC_A101 table):
- Cleans customer IDs
- Expands country codes into full names
- Handles missing or invalid values
*/
	TRUNCATE TABLE silver.erp_LOC_A101;
	INSERT INTO silver.erp_LOC_A101 (
		CID,
		CNTRY
	)
	SELECT
		REPLACE(CID, '-', '') AS CID,
		CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			 WHEN TRIM(CNTRY) = 'USA' OR TRIM(CNTRY) = 'US' THEN 'United States'
			 WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
			 ELSE TRIM(CNTRY) END AS CNTRY 
	FROM bronze.erp_LOC_A101;

	PRINT 'Table silver.erp_LOC_A101 loaded';

/* 
Clean product category data (bronze.erp_PX_CAT_G1V2 table):
- Trims all text fields
*/
	TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
	INSERT INTO silver.erp_PX_CAT_G1V2(
		ID_CAT,
		CAT,
		SUBCAT,
		MAINTENANCE
	)
	SELECT
		TRIM(ID) AS ID_CAT,
		TRIM(CAT) as CAT,
		TRIM(SUBCAT) as SUBCAT,
		TRIM(MAINTENANCE) AS MAINTENANCE
	FROM bronze.erp_PX_CAT_G1V2;

	PRINT 'Table silver.erp_PX_CAT_G1V2 loaded';
	PRINT '';
	SET @end_time = GETDATE()
	PRINT 'TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's'
	PRINT 'END OF LOADING DATA'
	PRINT ''
	PRINT ''
END TRY
BEGIN CATCH
	PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE()
	PRINT 'ERROR MESSAGE: ' + CAST(ERROR_NUMBER() AS NVARCHAR)
	PRINT 'ERROR MESSAGE: ' + CAST(ERROR_STATE() AS NVARCHAR)
END CATCH
END
