/*
===========================
CREATE DATABASE AND SCHEMAS
===========================
Script Purpose: 
	The script creates a database names DataWareHouse and three schemas: bronze, silver and gold.
*/


USE master;
GO

CREATE DATABASE DataWareHouse;
GO

USE DataWareHouse;
GO

/* Create Scheamas: Bronze, Silver, Gold */
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
