/*
=====================================================================================================================
Stored Procedure: Load Silver Layer
=====================================================================================================================

Script Purpose:
	1. This stored procedure performs the Extract, Transform, and Load (ETL) process to populate the 'silver' schema 
		tables from the 'bronze' schema.
	2. It performs the following actions:
		- Truncates the silver tables before loading the data
		- Inserts transformed, standardized, and cleaned data from the 'bronze' tables into the 'silver' tables
		- Prepares data for analysis

Parameters
	1. None.
	2. This stored procedure does not accept any parameters or return any values.

Notion Project Plan Steps:
	- Code Ingestion
	- Data Validation (Data Completeness and Schema Checks)

Raw Notes to Self:

The Data Engineering Pipeline Considerations:
	- Speed of loading the data
	- Identify and monitor for errors
	- Print each step in the ETL process
	- Organize and make clear the documentation in the output: helps with debugging and optimizing performance easier

The following data transformations were performed:
	- Data cleaning
	- Data standardization
	- Data normalization
	- Derived data and columns
	- Data enrichment

Other Actions:
	- load the souces tables from the bronze tables
	- reminder: since these files and script are used frequently, a stored procedure was created
	- reminder: debug potential errors with TRY ... CATCH in the stored procedure 
			(TRY ... CATCH ensures errors handling,, data integrity, and issue logging for easier debugging)
			- SQL runs the TRY block, and if it fails, it runs the CATCH block to handle the errors -- STEP 6
	-reminder: track the ETL duration as we want to know how it takes to load the tables and which tables take
			a lot of time to run - possible sign of issues
			- this helps to identify bottlenecks, optimize performance, monitor trends, and detects issues -- STEP 7

Usage Example: 
EXEC silver.load_silver;
=========================================================================================================================
*/

 CREATE OR ALTER PROCEDURE silver.load_silver AS -- stored procedure for load script
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; -- STEP 7
	BEGIN TRY -- STEP 6
		PRINT '=====================================================================';
		PRINT 'Loading Silver Layer'; -- STEP 5
		PRINT '=====================================================================';

		PRINT '---------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------------------------------';

		-- crm sources
		SET @batch_start_time = GETDATE(); -- STEP 7
		SET @start_time = GETDATE(); -- STEP 7
		
--------------------------------------------------------------------------------------------------------------------------------------------
		PRINT '>> Truncate Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>> Insert Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
	
		SELECT 
		cst_id,
		cst_key,

		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname, -- Data transformation: Remove unwanted spaces

			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'Unknown'
			END cst_marital_status, -- Data normalization: User-friendly readable format and handling missing values
									-- Turn marital status values into readable format - lower to UPPER case. 
									-- Remove extra spaces as a precaution. 
									-- Rename data values for clarity
									-- Use default value "Unknown" for missing information

			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'Unknown'
			END cst_gndr,
		cst_create_date

		FROM (
			SELECT
				*,
				-- Step 2: Use window function to rank id by date to show most recent first. We are only interested in this recent date
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS recent 
				FROM bronze.crm_cust_info
				-- WHERE cst_id = 29483 -- Step 1: id exist two times. There is a newer creation date so this date is the most recent
				WHERE cst_id IS NOT NULL 
			) t 
		WHERE recent = 1 -- Data transformation: Remove duplicates
						 -- Filter data

		SET @end_time = GETDATE(); 
		PRINT '>> silver.crm_cust_info Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------';

--------------------------------------------------------------------------------------------------------------------------------------------

		SET @start_time = GETDATE(); -- STEP 7
		PRINT '>> Truncate Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>> Insert Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			) -- Step 8: Place in silver product info table

		-- According to the data integration mapping, the crm_prd_info file is connected to the erp_px_cat_g1v2 and the crm_sales_details 
		-- via the prd_key. So, break up "prd_key" in "crm_prd_info" to retrieve the category ID and product key
		SELECT
		prd_id,
		-- prd_key, -- not needed anymore
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')  AS cat_id, -- Step 1: Starting from the left position (1), extract 5 characters
			REPLACE(SUBSTRING(prd_key, 7, LEN(prd_key)), '-', '_')  AS prod_key, -- Step 3: Starting from the left position (7), extract remaining characters

		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost, -- Step 5: replace NULLs with 0

			CASE UPPER(TRIM(prd_line))
				WHEN 'S' THEN 'Other Sales'
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'
			END prd_line, -- Step 6: Data normalization in user-friendly readable format and handling missing values
						  -- Turn values into readable format - lower to UPPER case. 
						  -- Remove extra spaces as a precaution. 
						  -- Rename data values for clarity
						  -- Use default value "Unknown" for missing information

		CAST(prd_start_dt AS DATE) AS prd_start_dt, -- Data typecasting: Convert from datetime to date
		CAST(
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 
				AS DATE) 
			AS prd_end_dt -- Step 7: Data Enrichment - Address overlapping dates and remove timestamp by calculating end date one
						  -- day before the next start date

		FROM bronze.crm_prd_info

		SET @end_time = GETDATE(); 
		PRINT '>> silver.crm_prd_info Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load this table and print its duration)

--------------------------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE(); -- STEP 7

		PRINT '>> Truncate Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>> Insert Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
			)

		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,

		-- 	sls_order_dt, -- No longer needed
			CASE
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL -- Invalid data: if the date is 0 or string lenght is not 8, make NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- Data type casting: then cast the date first as varchar then re-cast as order date
			END AS sls_order_dt,

		--	sls_ship_dt, -- no longer needed
				CASE
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL -- if the date is 0 or string lenght is not 8, make NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) -- then cast the date first as varchar then re-cast as ship date
			END AS sls_ship_dt,

		--	sls_due_dt, -- no longer needed
				CASE
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL -- if the date is 0 or string lenght is not 8, make NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) -- then cast the date first as varchar then re-cast as due date
			END AS sls_due_dt,

		-- after checking with data owner, we will apply three business rules, as values must not be NULL, negative or zero:
		--		1. if sales is negative, zero or NULL, calculate sales from formula: quantity * price
		--		2. if price is zero or null, calculate price from formula: sales / quantity
		--		3. if price is negative, convert price to a positive value

			CASE 
			-- Invalid data, Missing data, Derive data from existing data and calculations
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales -- otherwise, keep the exisiting sales
			END AS sls_sales,
	
			sls_quantity,
	
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0) -- if quantity is 0, replace with a NULL value
				ELSE sls_price -- otherwise, keep the exisiting prices
			END AS sls_price

		FROM bronze.crm_sales_details

		SET @end_time = GETDATE(); 
		PRINT '>> silver.crm_sales_details Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load this table and print its duration)

--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

		PRINT '---------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------------------------------';
		-- erp sources

--------------------------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE(); -- STEP 7

		PRINT '>> Truncate Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>> Insert Data Into: silver.erp_cust_az12';

		INSERT INTO silver.erp_cust_az12 (
			cid, 
			bdate,
			gen
			)

		SELECT 
		-- Handle invalid values: Remove NAS prefix if present
		-- CID, -- not needed anymore
		CASE 
			WHEN CID LIKE 'NAS%' THEN SUBSTRING (CID, 4, LEN(CID)) -- search for the characters NAS, extract from the 4th position to the length of CID
			ELSE CID -- otherwise, print CID
		END AS cid, -- This CID matches the cst_key format

		-- Handle invalid values: Set future birthdates to NULL
		-- BDATE, -- not needed anymore
		CASE 
			WHEN BDATE > GETDATE() THEN NULL -- we can be certain the date cannot be greater the current date so make them NULL
			ELSE BDATE -- otherwise, print the recorded birthdate
		END AS bdate,

		-- Normalize gender values for consistency, and handle unknown values
		--GEN, -- not needed anymore
		CASE 
			WHEN UPPER(TRIM(GEN)) IN ('F', 'Female') THEN 'Female' --find and make F. female
			WHEN UPPER(TRIM(GEN)) IN ('M', 'Male') THEN 'Male' --find and make M, Male
			ELSE 'n/a' -- otherwise, n'a for any NULL, empty spaces or unknowns
		END AS gen

		FROM bronze.erp_cust_az12

		SET @end_time = GETDATE(); 
		PRINT '>> silver.erp_cust_az12 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load this table and print its duration)

--------------------------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE(); -- STEP 7

		PRINT '>> Truncate Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>> Insert Data Into: silver.erp_loc_a101';

		INSERT INTO silver.erp_loc_a101 (
			CID ,
			CNTRY
			)

		SELECT

		-- Handle invalid values: Remove NAS prefix if present
		REPLACE(CID, '-', '') AS cid,

		-- Normalize and handle unknown country codes for consistency
		CASE 
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry)IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101

		SET @end_time = GETDATE(); 
		PRINT '>> silver.erp_loc_a101 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load this table and print its duration)

--------------------------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE(); -- STEP 7

		PRINT '>> Truncate Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>> Insert Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
			)

		SELECT
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE

		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE(); 
		PRINT '>> silver.erp_px_cat_g1v2 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load this table and print its duration)

--------------------------------------------------------------------------------------------------------------------------------------------

		
		SET @batch_end_time = GETDATE(); 
		PRINT '>> Loading Bronze Layer is completed.

		Total Bronze Layer Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT ' >> ---------------------------------
		';	-- STEP 7 (find how long it takes to load the whole bronze layer and print its duration)

--------------------------------------------------------------------------------------------------------------------------------------------
	END TRY -- STEP 6
	BEGIN CATCH -- our custom error capture message
		PRINT '=====================================================================';
		PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================================================';
	END CATCH

--------------------------------------------------------------------------------------------------------------------------------------------
END
