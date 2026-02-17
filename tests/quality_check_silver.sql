/*
===================================================================================
Quality Checks

Script Purpose:
	This script performs various quality checks for data consistency, accuracy, and standardization across the ‘silver’ schema.  It includes checks for:
	-Null or Duplicate primary keys.
	-Unwanted spaces in string fields.
	-Data standardization and consistency.
	-Invalid data ranges and orders.
	-Data consistency between related fields.

Usage Notes:
	-Run these checks after data loading Silver Layer.
	-Investigate and resolve any discrepancies found during the checks.
==================================================================================
*/

--Check for Nulls or Duplicates in the Primary Key
--Expected Output: No Result
SELECT 
	prd_id, 
	COUNT(*)
	FROM silver.crm_prd_info
	GROUP BY prd_id
	HAVING COUNT(*) > 1 OR prd_id IS NULL

	----Check for unwanted spaces
	----Expected Output: No Result

	SELECT prd_nm
	FROM silver.crm_prd_info
	WHERE prd_nm != LTRIM(RTRIM(prd_nm))

	---Check for NULLS or Negative Numbers
	---Expected Output: No Result

	SELECT prd_cost
	FROM silver.crm_prd_info
	WHERE prd_cost < 0 OR prd_cost IS NULL


	---Data Standardization & Consistency Check
	SELECT DISTINCT gen
	FROM bronze.erp_cust_az12

	---Check for Invalid Date Orders
	SELECT *
	FROM silver.crm_prd_info
	WHERE prd_end_date < prd_star_dt

	SELECT 
	NULLIF(sls_order_dt, 0) AS sls_order_dt
	FROM silver.crm_sales_details
	WHERE sls_order_dt <=0
	OR LEN(sls_order_dt) != 8
	OR sls_order_dt > 20500101
	OR sls_order_dt < 19000101


--Check Data Consistency: Between Sales, Quantity and Price
---->Sales = Quantity * Price
---->Values must not be NULL,Zero or Negative

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


---Identify Out-Of-Range Dates

SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()
