/*
=============
CREATE TABLES
=============
Script Purpose:
This script creates a table for each csv data file extrated from our sources: source_crm and source_erp.
*/

USE DataWareHouse


DROP TABLE IF EXISTS bronze.crm_cust_info;
DROP TABLE IF EXISTS bronze.crm_prd_info;
DROP TABLE IF EXISTS bronze.crm_sales_details;
DROP TABLE IF EXISTS bronze.erp_CUST_AZ12;
DROP TABLE IF EXISTS bronze.erp_LOC_A101;
DROP TABLE IF EXISTS bronze.PX_CAT_G1V2;

/* ----------------------------- */

CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(10),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status CHAR,
	cst_gndr CHAR,
	cst_create_date DATE
);

/* ----------------------------- */

CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost NVARCHAR(50),
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);

/* ----------------------------- */

CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

/* ----------------------------- */

CREATE TABLE bronze.erp_CUST_AZ12 (
	CID NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(50)
);

/* ----------------------------- */

CREATE TABLE bronze.erp_LOC_A101 (
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50)
);

/* ----------------------------- */

CREATE TABLE bronze.PX_CAT_G1V2 (
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(10)
);

