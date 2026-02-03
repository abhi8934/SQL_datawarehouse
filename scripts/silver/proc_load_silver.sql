CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @start_silver DATETIME, @end_silver DATETIME
	BEGIN TRY
		PRINT '---------------------------------------------------------'
		PRINT '--------------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------------------------------'
		PRINT '---------------------------------------------------------'
		SET @start_silver = GETDATE()
		SET @start_time = GETDATE()
		PRINT '>> Truncating table silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting crm_cust_info to silver layer'
		INSERT INTO silver.crm_cust_info
		-- how does this work 
		(
			cst_id,
			cst_key, 
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		SELECT 
		-- Here star is replaced by other conditions which clean the data further
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			ELSE 'N/A' 
			END AS cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'N/A' 
			END AS cst_gndr,
			cst_create_date
		FROM ( 
			SELECT *, 
				ROW_NUMBER() OVER(
					PARTITION BY cst_id 
					ORDER BY cst_create_date DESC
				) AS flag
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			) t
		WHERE flag = 1
		SET @end_time = GETDATE()
		PRINT '>> Time taken to load crm_cust_info = ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating table silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting data in silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info
		(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') AS cat_id,
			SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
			TRIM(prd_nm) AS prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE
				WHEN UPPER(TRIM(prd_line)) = 'R' then 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' then 'Touring'
				WHEN UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
				ELSE 'N/A'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt)OVER(
				PARTITION BY prd_key
				ORDER BY prd_start_dt)-1
				AS DATE
				) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE()
		PRINT '>> Time taken to load crm_prd_info = ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------'
		
		SET @start_time = GETDATE()
		PRINT '>> Truncating table silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting data into silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details
		(
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
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
				-- when sls_price is faulted.
				CASE 
					WHEN sls_price <= 0 THEN ABS(sls_price) 
					WHEN sls_price IS NULL THEN ABS(sls_sales) / sls_quantity
					ELSE sls_price
				END AS sls_price,
				sls_quantity,
				-- when the problem is in sls_sales
				CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales
			FROM bronze.crm_sales_details
			SET @end_time = GETDATE()
		PRINT '>> Time taken to load crm_sales_details = ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------'
		
		PRINT '---------------------------------------------------------'
		PRINT '---------------------------------------------------------'
		PRINT ' Loading ERP tables '
		PRINT '---------------------------------------------------------'
		PRINT '---------------------------------------------------------'
		
		SET @start_time = GETDATE()
		PRINT '>> Truncating table erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>> Inserting data in erp_cust_az12'
		INSERT INTO silver.erp_cust_az12
		(
			CID,
			BDATE,
			GEN
		)

		SELECT  
		CASE 
			WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
			ELSE CID 
		END AS CID,
		CAST(BDATE AS DATE) AS BDATE,
		CASE 
			WHEN GEN = 'F' OR GEN = 'Female' THEN 'Female'
			WHEN GEN = 'M' OR GEN = 'Male' THEN 'Male'
			ELSE 'N/A'
		END AS GEN
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE()
		PRINT '>> Time taken to load erp_cust_az12 = ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating table silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> Inserting table silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101
		(
			CID,
			CNTRY
		)
		SELECT
			REPLACE(CID, '-','') AS CID,
			CASE 
				WHEN TRIM(CNTRY) = 'DE' OR CNTRY = 'Germany' THEN 'Germany'
				WHEN TRIM(CNTRY) = 'United States' OR CNTRY = 'US' THEN 'United States'
				WHEN TRIM(CNTRY) = NULL or TRIM(CNTRY) = ' '  THEN NULL
				ELSE TRIM(CNTRY)
			END AS CNTRY
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE()
		PRINT '>> Time taken to load erp_loc_a101 = ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating table silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>> Inserting values in silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2
		(
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
		SET @end_time =  GETDATE()
		PRINT '>> Time taken to load erp_px_cat_g1v2 = ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------'
		SET @end_silver = GETDATE()
		PRINT '>> Time taken to load silver layer = ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + ' seconds'
	END TRY
	BEGIN CATCH
		PRINT '================================================' 
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER' 
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR(50))
		PRINT '================================================'
	END CATCH
END