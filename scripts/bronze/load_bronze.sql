/*
===============================
LOAD DATA INTO BRONZE TABLES
===============================
Script Purpose:
This script performs a full load of raw data into the bronze layer 
from CRM and ERP source files.

- Data is loaded using BULK INSERT from CSV files
- Existing data is removed using TRUNCATE
- No transformations are applied
*/


CREATE OR ALTER PROCEDURE bronze.load_val AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	PRINT 'LOADING BRONZE TABLES'
	BEGIN TRY
		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\erick\Desktop\SQL\PROJECT\DataWareHouse\Sources\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT 'Table bronze.crm_cust_info loaded'


		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\erick\Desktop\SQL\PROJECT\DataWareHouse\Sources\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT 'Table bronze.crm_prd_info loaded'



		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\erick\Desktop\SQL\PROJECT\DataWareHouse\Sources\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT 'Table bronze.crm_sales_details loaded'

		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\erick\Desktop\SQL\PROJECT\DataWareHouse\Sources\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT 'Table bronze.erp_CUST_AZ12 loaded'


		TRUNCATE TABLE bronze.erp_LOC_A101;
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\erick\Desktop\SQL\PROJECT\DataWareHouse\Sources\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT 'Table bronze.erp_LOC_A101 loaded'


		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\erick\Desktop\SQL\PROJECT\DataWareHouse\Sources\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT 'Table bronze.erp_PX_CAT_G1V2 loaded'
		PRINT ''
		PRINT 'END OF LOADING DATA'
		SET @end_time = GETDATE()
		PRINT 'TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's'
		PRINT ''
		PRINT ''
	END TRY
	BEGIN CATCH
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE()
		PRINT 'ERROR MESSAGE: ' + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'ERROR MESSAGE: ' + CAST(ERROR_STATE() AS NVARCHAR)
	END CATCH
END
