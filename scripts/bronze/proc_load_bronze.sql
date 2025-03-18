/*

=====================================================================================================================
Stored Procedure: Load Bronze Layer
=====================================================================================================================

Script Purpose:
	1. This stored procedure loads data into the 'bronze' schema from external .csv files.
	2. It performs the following actions:
		- Truncates the bronze tables before loading the data
		- Uses the 'BULK INSERT' command to load data from .csv files to bronze tables

Parameters
	1. None.
	2. This stored procedure does not accept any parameters or return any values.

Notion Project Plan Steps:
	- Code Ingestion
	- Data Validation (Data Completeness and Schema Checks)

Raw Notes to Self:

Load Method: Truncate and Bulk insert

The Data Engineering Pipeline Considerations:
	- Speed of loading the data
	- Identify and monitor for errors
	- Print each step in the ETL process
	- Organize and make clear the documentation in the output: helps with debugging and optimizing performance easier

Actions:
-- load the souces tables from the crm and erp files into the database
-- reminder: specify the source location with the file name in the path name (FROM pathname)
-- reminder: since these files and script are used frequently, just create stored procedures from these scripts
-- reminder: debug potential errors with TRY ... CATCH in the stored procedure 
			(TRY ... CATCH ensures errors handling,, data integrity, and issue logging for easier debugging)
			- SQL runs the TRY block, and if it fails, it runs the CATCH block to handle the errors -- STEP 6
--reminder: track the ETL duration as we want to know how it talks to load tables and which tables taking 
			a lot of time to run - possible sign of issues
			- this helps to identify bottlenecks, optimize performance, monitor trends, and detects issues -- STEP 7

Usage Example: 
EXEC bronze.load_bronze;
=========================================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS -- STEP 4
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; -- STEP 7
	BEGIN TRY -- STEP 6
		PRINT '=====================================================================';
		PRINT 'Loading Bronze Layer'; -- STEP 5
		PRINT '=====================================================================';

		PRINT '---------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------------------------------';

		-- crm sources
		SET @batch_start_time = GETDATE(); -- STEP 7
		SET @start_time = GETDATE(); -- STEP 7
		PRINT ' >> Truncate Table: bronze.crm_cust_info'; 


-- Truncate and Insert: quickly delete all rows from the table and reset it to an empty table before loading/inserting data
-- Reminder: If new data is added to the table or if altered at the source, delete table rows to empty then re-insert in database
		TRUNCATE TABLE bronze.crm_cust_info; -- STEP 3

		PRINT '>> Insert Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info -- STEP 1
		FROM 'C:\Users\karlc\OneDrive\Attachments\Documents\SQL Server Management Studio\SQL\SQL Data Warehouse\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- first row is the second row
			FIELDTERMINATOR = ',', -- the delimitor/separator is a comma
			TABLOCK -- OPTIONAL: lock the table during the loading
			); -- overall, we define how to handle the file as we load the files in the database (18000+ rows)
		
		-- let us test the quality of our bronze table: data in each column, data is the correct columns (it wasn't shifted), 
		-- count the number of rows [COUNT(*)], etc.

		--SELECT -- STEP 2
		--*
		--FROM bronze.crm_cust_info
		
		SET @end_time = GETDATE(); 
		PRINT '>> bronze.crm_cust_info Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load this table and print its duration)


		-- repeat procedure for the other 5 source files
		SET @start_time = GETDATE(); -- STEP 7
		PRINT ' >> Truncate Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info; 

		PRINT '>> Insert Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\karlc\OneDrive\Attachments\Documents\SQL Server Management Studio\SQL\SQL Data Warehouse\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, -- first row is the second row
			FIELDTERMINATOR = ',', -- the delimitor/separator is a comma
			TABLOCK -- OPTIONAL: lock the table during the loading
			); -- overall, we define how to handle the file as we load the files in the database
		SET @end_time = GETDATE(); 
		PRINT '>> bronze.crm_prd_info Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load this table and print its duration)

		SET @start_time = GETDATE(); -- STEP 7
		PRINT ' >> Truncate Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details; 

		PRINT '>> Insert Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\karlc\OneDrive\Attachments\Documents\SQL Server Management Studio\SQL\SQL Data Warehouse\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, -- first row is the second row
			FIELDTERMINATOR = ',', -- the delimitor/separator is a comma
			TABLOCK -- OPTIONAL: lock the table during the loading
			); -- overall, we define how to handle the file as we load the files in the database
		SET @end_time = GETDATE(); 
		PRINT '>> bronze.crm_sales_details Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load this table and print its duration)

		PRINT '---------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------------------------------';
		-- erp sources

		SET @start_time = GETDATE(); -- STEP 7
		PRINT ' >> Truncate Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12; 

		PRINT '>> Insert Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\karlc\OneDrive\Attachments\Documents\SQL Server Management Studio\SQL\SQL Data Warehouse\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2, -- first row is the second row
			FIELDTERMINATOR = ',', -- the delimitor/separator is a comma
			TABLOCK -- OPTIONAL: lock the table during the loading
			); -- overall, we define how to handle the file as we load the files in the database
		SET @end_time = GETDATE(); 
		PRINT '>> bronze.erp_cust_az12 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load this table and print its duration)

		SET @start_time = GETDATE(); -- STEP 7
		PRINT '>> Truncate Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101; 

		PRINT '>> Insert Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\karlc\OneDrive\Attachments\Documents\SQL Server Management Studio\SQL\SQL Data Warehouse\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2, -- first row is the second row
			FIELDTERMINATOR = ',', -- the delimitor/separator is a comma
			TABLOCK -- OPTIONAL: lock the table during the loading
			); -- overall, we define how to handle the file as we load the files in the database
		SET @end_time = GETDATE(); 
		PRINT '>> bronze.erp_loc_a101 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load this table and print its duration)

		SET @start_time = GETDATE(); -- STEP 7
		PRINT ' >> Truncate Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; 

		PRINT ' >> Insert Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\karlc\OneDrive\Attachments\Documents\SQL Server Management Studio\SQL\SQL Data Warehouse\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2, -- first row is the second row
			FIELDTERMINATOR = ',', -- the delimitor/separator is a comma
			TABLOCK -- OPTIONAL: lock the table during the loading
			); -- overall, we define how to handle the file as we load the files in the database
		SET @end_time = GETDATE(); 
		PRINT '>> bronze.erp_px_cat_g1v2 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load this table and print its duration)
		
		SET @batch_end_time = GETDATE(); 
		PRINT '>> Loading Bronze Layer is completed.

		Total Bronze Layer Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load the whole bronze layer and print its duration)

	END TRY -- STEP 6
	BEGIN CATCH -- our custom error capture message
		PRINT '=====================================================================';
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================================================';
	END CATCH

END


/*
-- let us test the quality of our bronze tables for each table:

SELECT 
*
FROM bronze.crm_cust_info

SELECT 
*
FROM bronze.crm_sales_details

SELECT 
*
FROM bronze.erp_px_cat_g1v2

SELECT 
*
FROM bronze.crm_prd_info

SELECT 
*
FROM bronze.erp_cust_az12

SELECT 
*
FROM bronze.erp_loc_a101
*/
