-- For CRM files
-- crm_prd_info
SELECT * FROM silver.crm_prd_info

--I) Checking for NULLS & duplicates in prd_id
SELECT prd_id, count(*) AS duplicates
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id IS NULL
--II) Checking FOR NULLS in prd_cost
SELECT prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost IS NULL
--III) Proper mapping of values in low cardinality column (prd_line).
	--a) Viewing the original entries.
SELECT 
	prd_line, 
	COUNT(*) AS number
FROM bronze.crm_prd_info
GROUP BY prd_line 
	--b) Comparing with the silver layer 
SELECT prd_line, COUNT(*) AS number
FROM silver.crm_prd_info
GROUP BY prd_line 
--IV) prd_start_dt & prd_end_dt
SELECT * 
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt

-- crm_sales_details
SELECT * FROM bronze.crm_sales_details
SELECT * FROM silver.crm_prd_info
SELECT * FROM silver.crm_cust_info
-- II) prd_key exists in silver.prd_info
SELECT 
	sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN 
(SELECT prd_key FROM silver.crm_prd_info)
-- III) cust_id exists in silver.cust_id
SELECT 
	sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN 
(SELECT cst_id FROM silver.crm_cust_info)
-- IV) sls_order_dt , sls_ship_dt, sls_due_dt
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR 
LEN(sls_order_dt) !=8 OR 
sls_order_dt > 20500101
OR sls_order_dt < 19000101

--For erp_cust_az12
SELECT DISTINCT GEN FROM silver.erp_cust_az12
-- Find out which CIDs are in table silver.crm_cust_info
SELECT  
CID 
FROM silver.erp_cust_az12
WHERE CID NOT IN 
(SELECT cst_key FROM silver.crm_cust_info)

--for erp_loc_a101
-- checking for inconsistent values in CID
SELECT CID 
FROM silver.erp_loc_a101
WHERE CID NOT LIKE 'AW%'
-- checking distinct values in CNTRY column
SELECT DISTINCT CNTRY
FROM silver.erp_loc_a101
	