-- Creating the Views for the Fact table and the dimension table
/*
Script purpose:- This script creates the coresponding tables
from bronze layer in the silver layer (silver schema).

Working:- It checks whether the table with the same name exists in the system,
if yes then it drops that, and creates a brand new table in its place with the name
as per case, if not it just creates a new table.
*/


-- Join erp_cust_az12 and erp_loc_a101
CREATE VIEW gold.dim_customers AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key , -- Surrogate key
	cr.cst_id AS customer_id,
	cr.cst_key AS customer_number,
	cr.cst_firstname AS firstname,
	cr.cst_lastname AS lastname,
	cr.cst_marital_status AS marital_status,
	CASE WHEN cr.cst_gndr IS NOT NULL THEN cr.cst_gndr
		ELSE COALESCE(az.GEN, 'N/A')
	END AS Gender,
	CAST(az.BDATE AS DATE) AS birthdate,
	cr.cst_create_date AS creation_date,
	lc.CNTRY AS country
FROM silver.crm_cust_info AS cr
LEFT JOIN silver.erp_cust_az12 AS az
ON		cr.cst_key = az.CID
LEFT JOIN silver.erp_loc_a101 AS lc
ON		cr.cst_key = lc.CID


-- create dimension products
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key,
	pr.prd_id AS product_id,
	pr.prd_key AS product_number,
	pr.prd_nm AS product_name,
	pr.prd_cost AS product_cost,
	pr.prd_line AS product_line,
	CAST(pr.prd_start_dt AS DATE) AS start_dt,
	pr.cat_id AS catagory_id,
	pcat.CAT AS catagory,
	pcat.SUBCAT AS 'sub-catagory',
	pcat.MAINTENANCE AS maintenance
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 AS pcat
ON		pr.cat_id = pcat.ID 
WHERE pr.prd_end_dt IS NULL

-- Creating the Fact Table
CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	gp.product_number,
	gc.customer_id,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers gc
ON		sd.sls_cust_id = gc.customer_id
LEFT JOIN gold.dim_products gp
ON		sd.sls_prd_key = gp.product_number
