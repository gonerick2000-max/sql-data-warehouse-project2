/*
===========================
CREATE DATABASE AND SCHEMAS
===========================
Script Purpose: 
	The script creates the DataWareHouse database and defines the core schemas (bronze, silver and gold) following the 
	Medallion architecture pattern.
*/


USE master;
GO

CREATE DATABASE DataWareHouse;
GO

USE DataWareHouse;
GO

/* Create Schemas: bronze, silver, gold */
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO