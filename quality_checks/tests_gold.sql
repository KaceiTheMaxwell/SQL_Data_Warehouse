/*
=======================================================================================
Quality Checks
=======================================================================================

Script Purpose:
	This script performs various quality checks for data standardization, consistency 
	and accuracy across the 'silver' schema. It includes checks for:
		- Null or duplicate values and/or primary keys
		- Unwanted spaces in string fields
		- Data standardization and consistency
		- Invalid date ranges and orders
		- Data consistency between related fields and tables

Usage Notes
	- Run these checks after data loading in Silver layer
	- Investigate and resolve any discrepancies found during the checks
========================================================================================

*/

-- ======================================================================================
-- Checking 'silver.crm_cust_info'
-- ======================================================================================

-- Check for Duplicates in Primary Key
-- Expectation: No Results. There should be no duplicates after joining the tables based 
-- on the primary key

SELECT cst_id, COUNT(*) FROM (
	SELECT 
	-- info from 'ci' master table
		ci.cst_id, 
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status, 
		ci.cst_gndr,
		ci.cst_create_date,

	-- info from 'ca' table
		ca.BDATE,
		ca.GEN,

	-- info from 'cl' table
		cl.CNTRY

	FROM silver.crm_cust_info AS ci -- our master table: customer info
	LEFT JOIN silver.erp_cust_az12 AS ca
		ON ci.cst_key = ca.CID -- primary key

	LEFT JOIN silver.erp_loc_a101 AS cl
		ON ci.cst_key = cl.CID -- primary key
	) t
GROUP BY cst_id
HAVING COUNT(*) > 1

-- Data integration
-- Remove the duplicated gender column, integrate gender data from both columns, and handle NULL values by replacing with 'n/a'

SELECT 
-- info from 'ci' master table

--	ci.cst_gndr, -- no longer needed due to data integration
	ci.cst_create_date,
--	ca.GEN, -- no longer needed due to data integration
	CASE 
		WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- CRM has the master data so use its gender info in places w/o 'Unknown'
		ELSE COALESCE(ca.GEN, 'n/a') -- otherwise, wherever the data is 'Unknown', use gender data from other table. And make NULL 'n/a'
	END AS new_gen,

-- info from 'cl' table
	cl.CNTRY

FROM silver.crm_cust_info AS ci -- our master table: customer info
LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.CID -- primary key

LEFT JOIN silver.erp_loc_a101 AS cl
	ON ci.cst_key = cl.CID -- primary key

-- Check the quality of this new dimension, specifically the gender column
SELECT
-- *
DISTINCT gender
FROM gold.dim_customers

-- ======================================================================================
-- Checking 'silver.crm_prd_info'
-- ======================================================================================

-- Check for Duplicates in Primary Key
-- Expectation: No Results. There should be no duplicates after joining the tables based 
-- on the primary key

SELECT prd_key, COUNT(*) FROM (

SELECT
pd.prd_id,
pd.cat_id,
pd.prd_key,
pd.prd_nm,
pd.prd_cost,
pd.prd_line,
pd.prd_start_dt,
-- pd.prd_end_dt -- column no longer needed: the data owner only wants current data so focus attention on 
			     -- end dates with NULL values b/c non-NULL end date values represent past information

ca.CAT,
ca.SUBCAT,
ca.MAINTENANCE

FROM silver.crm_prd_info AS pd
LEFT JOIN silver.erp_px_cat_g1v2 AS ca
	ON pd.cat_id = ca.ID -- tables share relationship based on the cat_ID and ID

WHERE prd_end_dt IS NULL -- this filters out historical data and returns only current sales
	) t
GROUP BY prd_key
HAVING COUNT(*) > 1

-- Check the quality of this new dimension
SELECT
*
FROM gold.dim_products

-- ======================================================================================
-- Checking 'gold.fact_sales'
-- ======================================================================================

-- Check for Foreign Key Integrity (Dimensions)
-- Expectation: No Results. There should be no results after joining the tables

SELECT 
*
FROM gold.fact_sales k
LEFT JOIN gold.dim_customers c
ON c.customer_key = k.customer_key

LEFT JOIN gold.dim_products p
ON p.product_key = k.customer_key

WHERE p.product_key IS NULL

