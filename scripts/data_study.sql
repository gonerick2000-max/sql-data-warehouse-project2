/*
=================
STUDYING THE DATA
=================
Script Purpose:
The purpose of this script is to study the sctructure of the data obtained with the crm source and erase repeated values. It also standardized values.
*/


USE DataWareHouse;
SELECT *
	FROM (
		SELECT *,
			count(*) OVER (PARTITION BY cst_id) as cnt,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as RN
		FROM bronze.crm_cust_info
		) t
WHERE cnt > 1 OR cst_id IS NULL



USE DataWareHouse;
WITH new_table AS (
	SELECT *,
			count(*) OVER (PARTITION BY cst_id) as cnt,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as RN
		FROM bronze.crm_cust_info
	)
DELETE FROM new_table
WHERE (cnt > 1 AND RN > 1) OR (cst_id IS NULL)



SELECT *
FROM bronze.crm_cust_info
WHERE cst_id IN (29433, 29449, 29466, 29466, 29473, 29483)


SELECT 
	cst_marital_status
FROM bronze.crm_cust_info
GROUP BY cst_marital_status

SELECT 
	cst_gndr
FROM bronze.crm_cust_info
GROUP BY cst_gndr
/* --------------------------------------- */

SELECT 
	prd_id,
	count(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*) != 1

SELECT *
FROM bronze.crm_prd_info
WHERE prd_line IS NULL


SELECT *
FROM bronze.crm_prd_info
WHERE prd_nm IN  (
SELECT 
	prd_nm
FROM bronze.crm_prd_info
GROUP BY prd_nm
HAVING count(*) != 1 )

SELECT 
	prd_line
FROM bronze.crm_prd_info
GROUP BY prd_line


/* -------------------------------- */

SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num IN (
SELECT
	sls_ord_num
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING count(*) != 1
)


SELECT 
	YEAR(sls_order_dt) ord_year,
	COUNT(*)
FROM bronze.crm_sales_details
GROUP BY YEAR(sls_order_dt)
ORDER BY ord_year


SELECT *
FROM bronze.crm_sales_details
WHERE sls_ship_dt IS NULL OR sls_due_dt IS NULL

/* -------------------------------- */

SELECT
	CID,
	COUNT(*)
FROM bronze.erp_CUST_AZ12
GROUP BY CID
HAVING COUNT(*) != 1


SELECT 
	GEN,
	LEN(GEN) AS LENGHT,
	COUNT(*) AS counts
FROM bronze.erp_CUST_AZ12
GROUP BY GEN

UPDATE bronze.erp_CUST_AZ12
SET GEN = CASE WHEN GEN = 'Male' THEN 'M'
		 WHEN GEN = 'Female' THEN 'F'
		 ELSE NULLIF(GEN, '') END
