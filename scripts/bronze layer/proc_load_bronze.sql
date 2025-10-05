exec bronze.load_bronze
create or alter proc bronze.load_bronze as
 begin
 DECLARE @start_time DATETIME, @end_time DATETIME
	begin try
	print '*******************************'
	print 'table bronze.crm_cust_info'
	print '*******************************'

	SET @start_time = GETDATE();
	truncate table bronze.crm_cust_info
	bulk insert bronze.crm_cust_info 
	from 'C:\Users\AIS\Downloads\SQL WITH BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with
	(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
------------------------------------------------------
print '*******************************'
print 'table bronze.crm_prd_info'
print '*******************************'
		
SET @start_time = GETDATE();
truncate table bronze.crm_prd_info
bulk insert bronze.crm_prd_info 
from 'C:\Users\AIS\Downloads\SQL WITH BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
---------------------------------------------------------
SET @start_time = GETDATE();
truncate table bronze.crm_sales_details
bulk insert bronze.crm_sales_details 
from 'C:\Users\AIS\Downloads\SQL WITH BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
-------------------------------------------------
	print '*******************************'
	print 'table bronze.erp_cust_az12'
	print '*******************************'
SET @start_time = GETDATE();
truncate table bronze.erp_cust_az12

bulk insert bronze.erp_cust_az12 
from 'C:\Users\AIS\Downloads\SQL WITH BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
----------------------------------------------------------
	print '*******************************'
	print 'table bronze.erp_loc_a101'
	print '*******************************'
SET @start_time = GETDATE();
truncate table bronze.erp_loc_a101

bulk insert bronze.erp_loc_a101 
from 'C:\Users\AIS\Downloads\SQL WITH BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
----------------------------------------------------------------
	print '*******************************'
	print 'table bronze.erp_px_cat_g1v2'
	print '*******************************'
SET @start_time = GETDATE();
truncate table bronze.erp_px_cat_g1v2

bulk insert bronze.erp_px_cat_g1v2 
from 'C:\Users\AIS\Downloads\SQL WITH BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		end try
		begin catch
		PRINT '___________________________________________-'
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '____________________________________________'
		end catch
end
