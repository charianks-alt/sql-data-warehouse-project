/*

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

--==========================================================================
--Checking ‘gold.dim_customers’
--==========================================================================
--Check for Uniqueness of Customer Key in gold.dim_customers
--Expectations: No Results

SELECT 
	ci.cst_gndr,
	ca.gen,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --- CRM is the master for Gender----
ELSE COALESCE(ca.gen, 'n/a')
END AS new_gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key= ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
on ci.cst_key= la.cid
ORDER BY 1,2

--Foreign Key Integrity ( Dimensions)----
SELECT
* FROM gold.facts_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
on p.product_key = f.product_key
WHERE p.product_key IS NULL

