/* 

==================================================================================
Create Database 'DataWarehouse'and Schema
==================================================================================

Script Purpose:
	This script creates a new database after checking if it already exists. If the
	the database exists, it is deleted and recreated. The script also sets up 
	three (3) schemas within the database: 'bronze', 'silver' and 'gold'.

WARNING:
	Proceed with caution as the entire DataWarehouse database will be permanently
	deleted when the script is ran. Be sure to save and have backups before running
	the script.

*/

USE master;
GO

-- Check if the 'DataWarehouse' database exist. Drop and recreate if needed
IF EXISTS (
SELECT 1 
FROM sys.databases 
WHERE name = 'DataWarehouse')

BEGIN
	ALTER DATABASE DataWarehouse
	SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO

-- Create 3 Schemas
CREATE SCHEMA bronze;
GO


CREATE SCHEMA silver;
GO



CREATE SCHEMA gold;
GO
