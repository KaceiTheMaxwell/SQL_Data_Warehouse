/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

-- descriptive information about the customers
CREATE VIEW gold.dim_customers AS

SELECT 

-- let us generate a surrogate key to connect the data model, using a window function, to create this 
-- dimension table

	ROW_NUMBER () OVER(ORDER BY cst_id) AS customer_key,

	ci.cst_id AS customer_id, -- business rule: rename columns to user-friendly, understandable names
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,

	cl.CNTRY AS country, -- business rule: sort column order into logical groups to improve readability

	ci.cst_marital_status AS marital_status, 
--	ci.cst_gndr, -- no longer needed due to data integration
--	ca.GEN, -- no longer needed due to data integration
	CASE 
		WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- CRM has the master data so use its gender info in places w/o 'Unknown'
		ELSE COALESCE(ca.GEN, 'n/a') -- otherwise, wherever the data is 'Unknown', use gender data from other table. And make NULL 'n/a'
	END AS gender,

	ca.BDATE AS birth_date,

	ci.cst_create_date AS create_date

FROM silver.crm_cust_info AS ci -- our master table: customer info
LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.CID -- primary key

LEFT JOIN silver.erp_loc_a101 AS cl
	ON ci.cst_key = cl.CID -- primary key

GO

--------------------------------------------------------------------------------------------------------------------

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

-- descriptive information about the products
CREATE VIEW gold.dim_products AS

SELECT

-- let us generate a surrogate key to connect the data model, using a window function, to create this 
-- dimension table
	ROW_NUMBER() OVER(ORDER BY prd_start_dt, prd_key) AS product_key,

	pd.prd_id AS product_id, -- business rule: rename columns to user-friendly, understandable names
	pd.prd_key AS product_number, -- business rule: sort column order into logical groups to improve readability
	pd.prd_nm AS product_name,
	pd.cat_id AS category_id,
	ca.CAT AS category,
	ca.SUBCAT AS sub_category,
	pd.prd_line AS product_line,
	pd.prd_cost AS product_cost,
	pd.prd_start_dt AS start_date,
	ca.MAINTENANCE AS maintenance

-- pd.prd_end_dt -- column no longer needed: the data owner only wants current data so focus attention on 
			     -- end dates with NULL values b/c non-NULL end date values represent past information

FROM silver.crm_prd_info AS pd
LEFT JOIN silver.erp_px_cat_g1v2 AS ca
	ON pd.cat_id = ca.ID -- tables share relationship based on the cat_ID and ID

WHERE prd_end_dt IS NULL -- this filters out historical data and returns only current sales

GO

--------------------------------------------------------------------------------------------------------------------


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
/*

No data integration required

Note to self:
-- summative information about the products and customers with keys, dates and measures
-- facts connecting multiple dimensions so surrogate keys comes from the dimension 
-- primary keys: prd_key and cust_id
*/

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS

SELECT 

sd.sls_ord_num AS order_number, -- business rule: rename columns to user-friendly, understandable names

-- data lookup
--sd.sls_prd_key,-- no longer needed as we replace with the surrogate key that we generaated in our database, not the source system
--sd.sls_cust_id,-- no longer needed as we replace with the surrogate key that we generaated in our database, not the source system
pd.product_key,
cu.customer_key,

sd.sls_order_dt AS order_date, -- business rule: sort column order into logical groups to improve readability: dimension keys, dates then measures
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,

sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price

FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pd
ON sd.sls_prd_key = pd.product_number

LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id

GO
