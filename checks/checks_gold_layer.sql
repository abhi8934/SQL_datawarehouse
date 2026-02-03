
-- Checking foreign key integrity customer 
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_id = f.customer_id
WHERE c.customer_id IS NULL

-- Checking foreign key integrity products
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON		f.product_number = p.product_number
WHERE p.product_number IS NULL

SELECT * FROM gold.dim_products