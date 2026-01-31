CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @start_bronze DATETIME, @end_bronze DATETIME;
	SET @start_bronze = GETDATE()
	BEGIN TRY
		PRINT '=============================';
		PRINT 'Loading Bronze layer';
		PRINT '=============================';

		PRINT '-----------------------------';
		PRINT 'Loading CRM tables';
		PRINT '-----------------------------';
		
		SET @start_time = GETDATE()
		-- Truncating the table crm_cust_info
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT'>> TABLE cust_info.csv Truncated'
		-- Loading the table crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\DataWarehouse\Sources\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT'>> TABLE cust_info.csv inserted'
		SET @end_time = GETDATE()
		PRINT 'Time taken to insert cust_info.csv = ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'Seconds'
		PRINT '-----------------------------------'
		
		
		-- Truncating the table bronze.crm_prd_info
		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT'>> TABLE crm_prd_info.csv truncated'
		-- Loading the table bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\DataWarehouse\Sources\source_crm\prd_info.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT 'Time taken to insert prd_info.csv = ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'Seconds'
		PRINT '-----------------------------------'
		
		
		PRINT'>> TABLE crm_prd_info.csv inserted'
		-- Truncating the table crm_sales_details
		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT'>> TABLE crm_prd_info.csv truncated'
		-- Loading the table crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\DataWarehouse\Sources\source_crm\sales_details.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT'>> TABLE crm_prd_info.csv inserted'
		SET @end_time = GETDATE()
		PRINT 'Time taken to insert prd_info.csv = ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'Seconds'
		PRINT '-----------------------------------'

		PRINT '-----------------------------'
		PRINT 'Loading ERP tables'
		PRINT '-----------------------------'
		
		
		SET @start_time = GETDATE()
		--Truncate the table bronze.erp_cust_az12
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT'>> TABLE cust_az12.csv Truncated'
		-- Loading the table
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\DataWarehouse\Sources\source_erp\CUST_AZ12.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT'>> TABLE cust_az12.csv inserted'
		SET @end_time = GETDATE()
		PRINT 'Time taken to insert cust_az12.csv = ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'Seconds'
		PRINT '-----------------------------------'
		
		--Truncate the table bronze.erp_loc_a101
		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT'>> TABLE loc_a101.csv truncated'
		--Loading the table
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\DataWarehouse\Sources\source_erp\LOC_A101.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT'>> TABLE loc_a101.csv inserted'
		SET @end_time = GETDATE()
		PRINT 'Time taken to insert loc_a101.csv = ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'Seconds'
		PRINT '-----------------------------------'

		--Truncate the table bronze.erp_px_cat_g1v2
		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT'>> TABLE px_cat_g1v2.csv truncated'
		--Loading the table
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\DataWarehouse\Sources\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT'>> TABLE px_cat_g1v2.csv inserted'
		SET @end_time = GETDATE()
		PRINT 'Time taken to insert px_cat_g1v2.csv = ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'Seconds'
		PRINT '-----------------------------------'
		SET @end_bronze = GETDATE()
		PRINT 'Time taken to load bronze layer = ' + CAST(DATEDIFF(second, @start_bronze, @end_bronze) AS NVARCHAR) +'Seconds'
		PRINT '-----------------------------------'
	END TRY
	
	BEGIN CATCH
		PRINT '================================================' 
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER' 
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR(50))
		PRINT '================================================'
	END CATCH
END

