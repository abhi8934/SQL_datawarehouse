/*
Script purpose:- This script creates the coresponding tables
in the bronze layer (bronze schema).

Working:- It checks whether the table with the same name exists in the system,
if yes then it drops that, and creates a brand new table in its place with the name
as per case, if not it just creates a new table.
*/ 
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
--creating a table for crm_cust_info
CREATE TABLE bronze.crm_cust_info
(
	cst_id	INT,
	cst_key	NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),	
	cst_gndr NVARCHAR(50),	
	cst_create_date DATE
)

--checking if the table exists
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
--creating a table for crm_prd_info
CREATE TABLE bronze.crm_prd_info
(
	prd_id INT,
	prd_key	NVARCHAR(50),
	prd_nm	NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),	
	prd_start_dt DATETIME,	
	prd_end_dt DATETIME
)

--checking if the table exists
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
--creating a table for crm_sales_details
CREATE TABLE bronze.crm_sales_details
(
	sls_ord_num	NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id	INT,
	sls_order_dt INT,
	sls_ship_dt	INT,
	sls_due_dt	INT,
	sls_sales	INT,
	sls_quantity INT,	
	sls_price INT
)


--checking if the table exists
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
--creating a table for erp_cust_az12
CREATE TABLE bronze.erp_cust_az12
(
	CID NVARCHAR(50),
	BDATE DATETIME,
	GEN NVARCHAR(50)
)

--checking if the table exist
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
--Creating a table for erp_loc_a101
CREATE TABLE bronze.erp_loc_a101
(
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50)
)


--creating a table erp_px_cat_g1v2
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
--creating a table for erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2
(
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50)
)

