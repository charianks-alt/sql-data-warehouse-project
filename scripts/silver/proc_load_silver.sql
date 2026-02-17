/*

Stored Procedure: Load Silver Layer ( Bronze -> Silver)


Script Purpose:
	This Stored procedure performs the ETL ( Extract, Transform, Load) process to populate the 
	‘silver’ schema tables from the ‘bronze’ schema.
Actions Performed:
	-Truncates silver tables.
	-Inserts transformed and cleaned data from Bronze to silver tables.
Parameters:
	None.
	This stored procedure doesn’t accept any parameters or return any values.
Usage Example:
EXEC Silver.load_silver;

==================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	
	BEGIN TRY
	SET @batch_start_time= GETDATE();
		PRINT'=================================';
		PRINT'Loading Silver Layer';
		PRINT'=================================';

		PRINT'---------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'---------------------------------';
	
		SET @start_time= GETDATE();
	PRINT '>>Truncating silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>>Inserting data into silver.crm_cust_info';

	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status, 
		cst_gndr,
		cst_create_date
	)
	SELECT
		cst_id,
		cst_key,
		LTRIM(RTRIM(cst_firstname)) AS cst_firstname,
		LTRIM(RTRIM(cst_lastname)) AS cst_lastname,
		CASE WHEN UPPER(LTRIM(RTRIM(cst_marital_status))) = 'M' THEN 'Married'
			 WHEN UPPER(LTRIM(RTRIM(cst_marital_status))) = 'S' THEN 'Single'
			 ELSE 'Unknown'
		END AS cst_material_status,
		CASE WHEN UPPER(LTRIM(RTRIM(cst_gndr))) = 'M' THEN 'Male'
			 WHEN UPPER(LTRIM(RTRIM(cst_gndr))) = 'F' THEN 'Female'
			 ELSE 'Unknown'
		END AS cst_gndr,
		cst_create_date
	FROM (
		SELECT
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	) AS t
	WHERE flag_last = 1;
	SET @end_time= GETDATE();
		PRINT'>>Load Duration :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT '----------'

	SET @start_time= GETDATE();

	PRINT '>>Truncating silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>>Inserting data into silver.crm_prd_info';
	IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
		DROP TABLE silver.crm_prd_info
	CREATE TABLE silver.crm_prd_info(
		prd_id INT,
		cat_id NVARCHAR(50),
		prd_key NVARCHAR(50),
		prd_nm NVARCHAR(50),
		prd_cost INT,
		prd_line NVARCHAR(50),
		prd_star_dt DATE,
		prd_end_date DATE,
		dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
	INSERT INTO silver.crm_prd_info (
		prd_id, 
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_star_dt,
		prd_end_date
	)
	SELECT 
		prd_id, 
		REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(LTRIM(RTRIM(prd_line)))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'N/A'
		 END AS prd_line,
		CAST(prd_start_dt AS DATE),
		CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_time= GETDATE();
		PRINT'>>Load Duration :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT '----------'

		SET @start_time= GETDATE();
		PRINT '>>Truncating silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>>Inserting data into silver.crm_sales_details';
		IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
		DROP TABLE silver.crm_sales_details
		CREATE TABLE silver.crm_sales_details(
		sls_ord_num NVARCHAR(50),
		sls_prd_key NVARCHAR(50),
		sls_cust_id INT,
		sls_order_dt DATE,
		sls_ship_dt DATE,
		sls_due_dt DATE,
		sls_sales INT,
		sls_quantity INT,
		sls_price INT,
		dwh_create_date DATETIME2 DEFAULT GETDATE()
		)
	INSERT INTO silver.crm_sales_details (
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
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL or sls_price <=0
	 THEN sls_sales / NULLIF(sls_quantity, 0)
	 ELSE sls_price
	 END AS sls_price
	FROM bronze.crm_sales_details;
	SET @end_time= GETDATE();
		PRINT'>>Load Duration :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT '----------'

		PRINT'---------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'---------------------------------';

		SET @start_time= GETDATE();
	PRINT '>>Truncating silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>>Inserting data into silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 
	(
	cid, bdate, gen)
	SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING( cid, 4,LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
	 END AS bdate,
	CASE WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M', 'MALE') THEN 'Male'
		 WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F', 'FEMALE') THEN 'Female'
		 ELSE 'N/A'
		END AS gen
	FROM bronze.erp_cust_az12;
	SET @end_time= GETDATE();
		PRINT'>>Load Duration :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT '----------'

		SET @start_time= GETDATE();
	PRINT '>>Truncating silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>>Inserting data into silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101
	(
	cid, cntry)
	SELECT
	REPLACE ( cid, '-', '') AS cid,
	COALESCE(CASE 
		WHEN TRIM(cntry) ='' OR cntry IS NULL THEN 'N/A'
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM (cntry) IN ('US', 'USA') THEN 'United States'
		 ELSE TRIM(cntry)
	END,'N/A') AS cntry
	FROM bronze.erp_loc_a101;
	SET @end_time= GETDATE();
		PRINT'>>Load Duration :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT '----------'

	SET @end_time= GETDATE();
	PRINT '>>Truncating silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>>Inserting data into silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2
	(
	id, cat, subcat, maintenance)
	SELECT
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
	SET @end_time= GETDATE();
		PRINT'>>Load Duration :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT '----------'

		SET @batch_end_time= GETDATE();
		PRINT'>>Total Batch Load Duration :' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------'

		END TRY
	BEGIN CATCH
		PRINT'====================================================';
		PRINT'ERROR OCCURED WHILE LOADING BRONZE LAYER';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Message' + CAST( ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message' + CAST( ERROR_STATE() AS NVARCHAR);
		PRINT'====================================================';

	END CATCH
END
