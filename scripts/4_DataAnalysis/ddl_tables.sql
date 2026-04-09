/*
=============
CREATE TABLES
=============
Script Purpose:
This script creates a database named DataAnalytics, a schema named gold
and all staging tables for data analysis.

Each table corresponds to a CSV file extracted from the gold layer.
*/


USE master;
GO

CREATE DATABASE DataAnalytics;
GO

USE DataAnalytics;
GO
CREATE SCHEMA gold;
GO


DROP TABLE IF EXISTS gold.dim_customers;
GO
CREATE TABLE gold.dim_customers (
    Customer_key BIGINT,
    Customer_ID NVARCHAR(10),
    First_name NVARCHAR(50),
    Last_name NVARCHAR(50),
    Country NVARCHAR(50),
    Birthdate DATE,
    Marital_status NVARCHAR(10),
    Gender NVARCHAR(50),
    Creation_date DATE
);
GO

TRUNCATE TABLE gold.dim_customers;
BULK INSERT gold.dim_customers
FROM 'C:\Users\erick\Desktop\machine_learning\SQL\PROJECT\DataAnalysis\gold.dim_customers.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
PRINT 'Table gold.dim_customers loaded'




DROP TABLE IF EXISTS gold.dim_products;
GO
CREATE TABLE gold.dim_products (
       [Product_key] BIGINT
      ,[Product_number] NVARCHAR(50)
      ,[Product_name] NVARCHAR(50)
      ,[ID_CAT] NVARCHAR(10)
      ,[Category] NVARCHAR(50)
      ,[Subcategory] NVARCHAR(50)
      ,[Line] NVARCHAR(50)
      ,[Maintenance] NVARCHAR(10)
      ,[Cost] INT
      ,[Start_date] DATE
      ,[End_date] DATE
);
GO

TRUNCATE TABLE gold.dim_products;
BULK INSERT gold.dim_products
FROM 'C:\Users\erick\Desktop\machine_learning\SQL\PROJECT\DataAnalysis\gold.dim_products.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
PRINT 'Table gold.dim_products loaded'





DROP TABLE IF EXISTS gold.fact_sales;
GO
CREATE TABLE gold.fact_sales (
       [Order_number] NVARCHAR(50)
      ,[Product_key] BIGINT
      ,[Customer_key] BIGINT
      ,[Order_date] DATE
      ,[Ship_date] DATE
      ,[Due_date] DATE
      ,[Quantity] INT
      ,[Price] INT
      ,[Sales] INT
);
GO

TRUNCATE TABLE gold.fact_sales;
BULK INSERT gold.fact_sales
FROM 'C:\Users\erick\Desktop\machine_learning\SQL\PROJECT\DataAnalysis\gold.fact_sales.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
PRINT 'Table gold.fact_sales loaded'
