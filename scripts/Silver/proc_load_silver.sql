--loading silver.crm_cust_info
truncate table silver.crm_cust_info
insert into silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
select 
cst_id,
cst_key,
--data cleaning - removed spaces
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
--data normalization and standerdization
case when upper(trim(cst_marital_status))='M' then 'Married'
when upper(trim(cst_marital_status))='S' then 'Single'
else 'N/A' --handling missing value
end as cst_marital_status,
case when upper(trim(cst_gndr))='M' then 'Male'
when upper(trim(cst_gndr))='F' then 'Female'
else 'N/A' end as cst_gndr,
cst_create_date 
from (
SELECT 
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as rank_by_creation_date,
* --removed duplicate and filtering
FROM bronze.crm_cust_info
where cst_id is not null
)t 
where rank_by_creation_date=1;

truncate table silver.crm_prd_info;
insert into silver.crm_prd_info(
prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
)
SELECT prd_id,
	replace(substring(prd_key,1,5) ,'-','_') as cat_id,  -- data extraction - derived columns
	substring(prd_key,7, len(prd_key)) as prd_key,
	trim(prd_nm),
	isnull(prd_cost,0),		--replacaing nulls with 0 - handling nulls
	case when upper(trim(prd_line)) = 'M' then 'Mountain'
	when upper(trim(prd_line)) = 'S' then 'Other Sales'
	when upper(trim(prd_line)) = 'T' then 'Touring'
	when upper(trim(prd_line)) = 'R' then 'Road'
	else 'N/A' end as prd_line,
	cast(prd_start_dt as date), -- data type conversion
	cast(dateadd(day,-1,lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as date)as prd_end_dt  --data enrichment - add new data to enhance data for analsys
	FROM bronze.crm_prd_info;

truncate table silver.crm_sales_details
insert into silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
	select
	trim(sls_ord_num),
	trim(sls_prd_key),
	sls_cust_id,
	case when isdate(sls_order_dt)=1 then cast(cast(sls_order_dt as varchar) as date)
	else Null end sls_order_dt,
	case when isdate(sls_ship_dt)=1 then cast(cast(sls_ship_dt as varchar) as date)
	else Null end sls_ship_dt,
	case when isdate(sls_due_dt)=1 then cast(cast(sls_due_dt as varchar) as date)
	else Null end sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	FROM bronze.crm_sales_details;
