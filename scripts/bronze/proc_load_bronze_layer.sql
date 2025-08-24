--bulk insert in bronze layer tables

/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

--Full Load (refreshing table) -truncate and insert into bronze layer
/*
adding customized message for each step for better understanding and readability
adding try and catch, handled error with error message
tracking etl duration -optimize performance issue and tracking
*/

--to execute 
-- exec bronze.load_bronze;
CREATE PROCEDURE bronze.load_bronze
as
begin
declare @start_time datetime, @end_time datetime, @start_bronze_load datetime, @end_bronze_load datetime;
begin try
set @start_bronze_load = getdate();
print('========================================');
print('Loading bronze Layer');
print('========================================');

print('------------------------------------------');
print('Loading CRM Tables');
print('------------------------------------------');
set @start_time = getdate();
--Full Load (refreshing table) -truncate and insert into bronze.crm_cust_info 
PRINT('>> Truncating: bronze.crm_cust_info');
TRUNCATE TABLE bronze.crm_cust_info ;
PRINT('>> inserting data into: bronze.crm_cust_info');
BULK INSERT bronze.crm_cust_info 
FROM 'C:\Users\SNEHA\Desktop\Data WareHouse Project\sql-data-warehouse-project\datasets\source_crm\CUST_INFO.CSV'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
)
set @end_time = getdate();

print('time to load bronze.crm_cust_info: ' + cast(datediff(second,@start_time,@end_time) as varchar)+ ' sec');
print(' ');
--Full Load (refreshing table) -truncate and insert into bronze.crm_prd_info 
PRINT('>> Truncating: bronze.crm_prd_info');
set @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_prd_info ;
PRINT('>> inserting data into: bronze.crm_prd_info');
BULK INSERT bronze.crm_prd_info 
FROM 'C:\Users\SNEHA\Desktop\Data WareHouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
)
set @end_time = getdate();

print('time to load bronze.crm_prd_info: ' + cast(datediff(second,@start_time,@end_time) as varchar)+ ' sec');
print(' ');

--Full Load (refreshing table) -truncate and insert into bronze.crm_sales_details 
PRINT('>> Truncating: bronze.crm_sales_details');
set @start_time = getdate();
TRUNCATE TABLE bronze.crm_sales_details ;
PRINT('>> inserting data into: bronze.crm_sales_details');
BULK INSERT bronze.crm_sales_details 
FROM 'C:\Users\SNEHA\Desktop\Data WareHouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
)
set @end_time = getdate();

print('time to load bronze.crm_sales_details: ' + cast(datediff(second,@start_time,@end_time) as varchar)+ ' sec');
print(' ');

print('------------------------------------------');
print('Loading ERP Tables');
print('------------------------------------------');

--Full Load (refreshing table) -truncate and insert into bronze.erp_CUST_AZ12 
PRINT('>> Truncating: bronze.erp_CUST_AZ12');
set @start_time = getdate();
TRUNCATE TABLE bronze.erp_CUST_AZ12 ;
PRINT('>> inserting data into: bronze.erp_CUST_AZ12');
BULK INSERT bronze.erp_CUST_AZ12 
FROM 'C:\Users\SNEHA\Desktop\Data WareHouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
)
set @end_time = getdate();

print('time to load bronze.erp_CUST_AZ12: ' + cast(datediff(second,@start_time,@end_time) as varchar)+ ' sec');
print(' ');

--Full Load (refreshing table) -truncate and insert into bronze.erp_loc_a101 
PRINT('>> Truncating: bronze.erp_loc_a101');
set @start_time = getdate();
TRUNCATE TABLE bronze.erp_loc_a101 ;
PRINT('>> inserting data into: bronze.erp_loc_a101');
BULK INSERT bronze.erp_loc_a101 
FROM 'C:\Users\SNEHA\Desktop\Data WareHouse Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.CSV'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
)
set @end_time = getdate();

print('time to load bronze.erp_loc_a101: ' + cast(datediff(second,@start_time,@end_time) as varchar)+ ' sec');
print(' ');


--Full Load (refreshing table) -truncate and insert into bronze.erp_px_cat_g1v2 
PRINT('>> Truncating: bronze.erp_px_cat_g1v2');
set @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_px_cat_g1v2 ;
PRINT('>> inserting data into: bronze.erp_px_cat_g1v2');
BULK INSERT bronze.erp_px_cat_g1v2 
FROM 'C:\Users\SNEHA\Desktop\Data WareHouse Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.CSV'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
)
set @end_time = getdate();

print('time to load bronze.erp_px_cat_g1v2: ' + cast(datediff(second,@start_time,@end_time) as varchar)+ ' sec');
print(' ');
set @end_bronze_load = getdate();
print('========================================');
print('Loading Bronze Layer is Completed');
print('Total Load Duration: ' + cast(datediff(second,@start_bronze_load,@end_bronze_load) as varchar)+ ' sec');
print('========================================');
end try
begin catch
print('========================================');
print('Error while Loading bronze Layer');
print('========================================');
print('Error Message: ' + error_message());
print('Error number: ' + cast(error_number() as varchar));
print('Error state: ' + cast(error_state() as varchar));
end catch
end;
