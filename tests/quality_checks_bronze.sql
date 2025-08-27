--checking data quality of bronze layer to load data in silver layer.
/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'bronze' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.
	-bronze layer contains row data from sourses so after running this script you will find out data quality issue that we will fix in silver layer.
Usage Notes:
    - Run these checks after data loading bronze Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

/*
====================================================
--checking 1st table - crm_cust_info
====================================================
*/
SELECT * FROM bronze.crm_cust_info;

--quality check 1.- primary key should be unique + not null
--make sure keys are able to join keys in other table
	--expectation - no result
select cst_id,count(*) from bronze.crm_cust_info group by cst_id having count(*)>1 or cst_id is null
--choosing latest record in case of duplicate primary key by using cst_create_date column
select * from (SELECT ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as rank_by_creation_date,* FROM bronze.crm_cust_info)t where rank_by_creation_date=1;

--quality check 2.- in string values check for unwanted spaces
	--expectation - no result
select cst_firstname from bronze.crm_cust_info where cst_firstname != trim(cst_firstname);
select cst_lastname from bronze.crm_cust_info where cst_lastname != trim(cst_lastname);
select cst_gndr from bronze.crm_cust_info where cst_gndr != trim(cst_gndr);

--quality check 3.- checking consistancy of low cardinality values in cst_marital_status and cst_gndr
select distinct cst_marital_status from bronze.crm_cust_info
select distinct cst_gndr from bronze.crm_cust_info
--Granularity of this columns is low and in DWH we want meaning ful dataonly rather than abbrebiation
--so we will map S->Single M->Married null->N/A for cst_marital_status
select 
cst_marital_status,
case when upper(trim(cst_marital_status))='M' then 'Married'
when upper(trim(cst_marital_status))='S' then 'Single'
else 'N/A' end as cst_marital_status
from bronze.crm_cust_info
--so we will map F->Female M->Male null->N/A for cst_gndr
select 
cst_gndr,
case when upper(trim(cst_gndr))='M' then 'Male'
when upper(trim(cst_gndr))='F' then 'Female'
else 'N/A' end as cst_gndr
from bronze.crm_cust_info

/*
====================================================
--checking table - crm_prd_info
====================================================
*/
--quality check 1.- primary key should be unique + not null
	--expectation - no result
SELECT * FROM bronze.crm_prd_info;
select count(*) as count,prd_id from bronze.crm_prd_info group by prd_id having count(*)>1
select * from bronze.crm_prd_info where prd_id is null

--distinct values
select distinct prd_nm from bronze.crm_prd_info
select distinct prd_line from bronze.crm_prd_info

--nulls or negative cost
select * from bronze.crm_prd_info where prd_cost<0 or prd_cost is Null 

--checking able to join tables using keys or not
select replace(substring(prd_key,1,5) ,'-','_') as cat_id from bronze.crm_prd_info where replace(substring(prd_key,1,5) ,'-','_') not in (select distinct id FROM bronze.erp_px_cat_g1v2)
select substring(prd_key,7, len(prd_key)) as prd_key,* from bronze.crm_prd_info where substring(prd_key,7, len(prd_key)) not in (select distinct sls_prd_key FROM bronze.crm_sales_details)

--in string values check for unwanted spaces
	--expectation - no result
select * from bronze.crm_prd_info where prd_nm != trim(prd_nm)


--start date greater than end date
select * from bronze.crm_prd_info where prd_start_dt > prd_end_dt
select prd_id,prd_key,prd_cost,prd_start_dt,dateadd(day,-1,lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt  from bronze.crm_prd_info 
select cast(prd_start_dt as date) from bronze.crm_prd_info

select distinct prd_line from bronze.crm_prd_info

/*
====================================================
--checking table - crm_sales_details
====================================================
*/
SELECT * FROM bronze.crm_sales_details;

--white spaces
SELECT * FROM bronze.crm_sales_details where sls_ord_num != trim(sls_ord_num)

-- checking by joining keys
SELECT * FROM bronze.crm_sales_details where sls_prd_key not in (select distinct sls_prd_key from bronze.crm_prd_info)
SELECT * FROM bronze.crm_sales_details where sls_cust_id not in (select distinct sls_cust_id from bronze.crm_prd_info)

--nulls or negative sls_sales and sls_price
select * FROM bronze.crm_sales_details where sls_sales <0 or sls_sales is null
select * FROM bronze.crm_sales_details where sls_price <0 or sls_price is null

--not date or negative or zero dates dates with less length
SELECT sls_order_dt FROM bronze.crm_sales_details where isdate(sls_order_dt)=0  or sls_order_dt <= 0 or len(sls_order_dt)!=8
--checking boundary of dates
SELECT sls_order_dt FROM bronze.crm_sales_details where  sls_order_dt > 20500101 or sls_order_dt< 19000101
SELECT sls_due_dt FROM bronze.crm_sales_details where isdate(sls_due_dt)=0 or sls_due_dt is null
SELECT sls_ship_dt FROM bronze.crm_sales_details where isdate(sls_ship_dt)=0 or sls_ship_dt is null

select case when isdate(sls_order_dt)=1 then cast(cast(sls_order_dt as varchar) as date)
	else Null end sls_order_dt FROM bronze.crm_sales_details where sls_order_dt is null or isdate(sls_order_dt) = 0
 
--check for invalid order date - order date should be less than ship date and due date
select * from bronze.crm_sales_details where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

select * FROM bronze.crm_sales_details where sls_price <=0 or sls_price is null OR sls_sales != sls_quantity * ABS(sls_price) 
select * FROM bronze.crm_sales_details where sls_quantity <= 0 or sls_quantity is null
select * FROM bronze.crm_sales_details where sls_sales<=0 or sls_sales is null

select sls_sales,case when sls_sales <= 0 OR sls_sales != ABS(sls_quantity) * ABS(sls_price)  then ABS(sls_quantity) * ABS(sls_price) 
	else sls_sales end as sls_sales
 FROM bronze.crm_sales_details

 select sls_quantity,case 
	when sls_quantity <= 0 OR sls_quantity != ABS(sls_sales) / nullif(sls_price,0)  then ABS(sls_quantity) * ABS(sls_price) 
	else sls_quantity end as sls_quantity
 FROM bronze.crm_sales_details

  select sls_price,case 
	when sls_price <= 0 OR sls_price != ABS(sls_sales) / nullif(sls_quantity,0)  then ABS(sls_quantity) * ABS(sls_price) 
	else sls_price end as sls_price
 FROM bronze.crm_sales_details
/*
====================================================
--checking table - erp_CUST_AZ12
====================================================
*/
SELECT * FROM bronze.erp_CUST_AZ12;
SELECT * FROM bronze.erp_CUST_AZ12 where cid != trim(cid);
SELECT * FROM bronze.erp_CUST_AZ12 where bdate is null isdate(BDATE)=0;

select cast(bdate as varchar),case when isdate(cast(bdate as varchar)) = 0 then Null 
else bdate end as bdate
	FROM bronze.erp_CUST_AZ12  where isdate(cast(bdate as varchar)) = 0 

SELECT distinct gen FROM bronze.erp_CUST_AZ12;
select case when upper(gen) = 'F' OR upper(GEN) ='FEMALE' THEN 'Female'
	when  upper(gen) = 'M' OR upper(GEN) ='MALE' THEN 'Male'
	else 'N/A' end as gen
	FROM bronze.erp_CUST_AZ12;

/*
====================================================
--checking table - erp_loc_a101
====================================================
*/

SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM bronze.erp_loc_a101 where cid !=trim(cid);
select distinct cntry FROM bronze.erp_loc_a101

select *,case when upper(cntry) in('DE','GERMANY') then 'Germany'
	when upper(cntry) in('USA','UNITED STATES','US') then 'United States'
	else cntry end as cntry
FROM bronze.erp_loc_a101 

/*
====================================================
--checking table - erp_px_cat_g1v2
====================================================
*/
SELECT  * FROM bronze.erp_px_cat_g1v2;

SELECT  * FROM bronze.erp_px_cat_g1v2 where id not in (select distinct cat_id from silver.crm_prd_info)

SELECT  * FROM bronze.erp_px_cat_g1v2 where cat != trim(cat) or subcat != trim(subcat)

select distinct maintenance from bronze.erp_px_cat_g1v2;
