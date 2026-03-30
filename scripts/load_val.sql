/*
========================
LOADING VALUES TO TABLES
========================
Script Purpose:
This script loads data from crm and erp sources and converts the integer values from the 4th column of 
bronze.crm_sales_details table to date values.
*/

TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\erick\Desktop\machine_learning\SQL\project1\sql-data-WH-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);


TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\erick\Desktop\machine_learning\SQL\project1\sql-data-WH-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);



TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\erick\Desktop\machine_learning\SQL\project1\sql-data-WH-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

UPDATE bronze.crm_sales_details
SET sls_order_dt = NULL
WHERE ISDATE(sls_order_dt) = 0

ALTER TABLE bronze.crm_sales_details
ALTER COLUMN sls_order_dt DATE

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt IS NOT NULL


TRUNCATE TABLE bronze.erp_CUST_AZ12;
BULK INSERT bronze.erp_CUST_AZ12
FROM 'C:\Users\erick\Desktop\machine_learning\SQL\project1\sql-data-WH-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);


TRUNCATE TABLE bronze.erp_LOC_A101;
BULK INSERT bronze.erp_LOC_A101
FROM 'C:\Users\erick\Desktop\machine_learning\SQL\project1\sql-data-WH-project\datasets\source_erp\LOC_A101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);


TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
BULK INSERT bronze.erp_PX_CAT_G1V2
FROM 'C:\Users\erick\Desktop\machine_learning\SQL\project1\sql-data-WH-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
