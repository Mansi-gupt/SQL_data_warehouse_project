--checkinfg quality of silver layer tables data 

/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
/*
steps -
create ddl
do data cleaning
load data 
check data quality of each tables
*/
-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
select cst_id from silver.crm_cust_info where cst_id is null
select count(*) as count,cst_id from silver.crm_cust_info group by cst_id having count(*)>1

-- Check for Unwanted Spaces
-- Expectation: No Results
select * from  silver.crm_cust_info where cst_firstname != trim(cst_firstname) or cst_lastname != trim(cst_lastname)

-- Data Standardization & Consistency
select distinct cst_gndr from  silver.crm_cust_info
select distinct cst_marital_status from  silver.crm_cust_info

select * from silver.crm_cust_info

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
select * from silver.crm_prd_info
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
select count(*), prd_id from silver.crm_prd_info group by prd_id having prd_id is null or count(*)>1

-- Check for Unwanted Spaces
-- Expectation: No Results
select * from silver.crm_prd_info where prd_nm != trim(prd_nm)

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
select * from silver.crm_prd_info where prd_cost<0 or prd_cost is Null 

-- Data Standardization & Consistency
select distinct prd_line from silver.crm_prd_info 

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
select * from silver.crm_prd_info where prd_start_dt > prd_end_dt

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
select * from silver.crm_sales_details where sls_order_dt > sls_due_dt or sls_order_dt > sls_ship_dt

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
select * from  silver.crm_sales_details where sls_Price <=0 or sls_Price is null or sls_Price != sls_sales / nullif(sls_Quantity,0)
select * from  silver.crm_sales_details where sls_Quantity <=0 or sls_Quantity is null
select * from  silver.crm_sales_details where sls_sales != sls_Quantity * sls_Price or sls_sales <=0 or sls_sales is null

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
select * from silver.erp_cust_az12 where bdate > getdate() or bdate < cast('1924-01-01' as date)
-- Data Standardization & Consistency
select distinct gen FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
select distinct cntry from silver.erp_loc_a101 

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
select * from silver.erp_px_cat_g1v2 where id != trim(id) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
select distinct MAINTENANCE from silver.erp_px_cat_g1v2
