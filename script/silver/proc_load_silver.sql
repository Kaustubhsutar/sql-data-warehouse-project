/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
  	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
  	BEGIN TRY
    		SET @batch_start_time = GETDATE()
    		PRINT'================================================';
    		PRINT'Loading Silver Layer';
    		PRINT'================================================';
    		
    		PRINT'Loading CRM Tables';
    		PRINT'------------------------------------------------';
    
    		SET @start_time = GETDATE()
    		PRINT '>>Truncating silver.crm_cust_info';
    		TRUNCATE TABLE silver.crm_cust_info;
    		PRINT '>>Inserting into silver.crm_cust_info';
    		INSERT INTO silver.crm_cust_info (
    			cst_id,
    			cst_key,
    			cst_firstname,
    			cst_lastname,
    			cst_marital_status,
    			cst_gndr,
    			cst_create_date
    		)
    		-- Data Cleaning and Transformation 
    		SELECT
    			cst_id,
    			cst_key,
    			TRIM(cst_firstname) AS cst_firstname,		-- Remove blank spaces
    			TRIM(cst_lastname) AS cst_lastname,			-- Remove blank spaces
    
    			CASE 
    				WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
    				WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
    				ELSE 'n/a'
    			END cst_marital_status, -- Map marital status code with descriptive values
    
    			CASE 
    				WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
    				WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
    				ELSE 'n/a'
    			END cst_gndr,	-- Map gender code with descriptive values
    			cst_create_date
    		FROM
    		(
    			SELECT
    			*,
    			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_last
    			FROM bronze.crm_cust_info
    		)t
    		WHERE Flag_last = 1 AND cst_id IS NOT NULL; -- Select rows with most recent order date 
    		SET @end_time = GETDATE()
    		PRINT'Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
    		PRINT'------------------------------------------------';
    		
    		SET	@start_time = GETDATE();
    		PRINT '>>Truncating silver.crm_prd_info';
    		TRUNCATE TABLE silver.crm_prd_info;
    		PRINT '>>Inserting into silver.crm_prd_info';
    		INSERT INTO silver.crm_prd_info ( 
    		prd_id,
    		cat_id,
    		prd_key,
    		prd_nm,
    		prd_cost,
    		prd_line,
    		prd_start_dt,
    		prd_end_dt
    		)
    		-- Data Cleaning and Transformation 
    		SELECT
    			prd_id,
    			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,				-- Extract category ID
    			REPLACE(SUBSTRING(prd_key,7,LEN(prd_key)),'-','_') AS prd_key,	-- Extraxct product key
    			TRIM(prd_nm) AS prd_nm,
    			COALESCE(prd_cost,0) AS prd_cost,								-- Replace NULL
    			CASE UPPER(TRIM(prd_line))										 
    				WHEN 'M' THEN 'Mountain'
    				WHEN 'R' THEN 'Road'
    				WHEN 'S' THEN 'Other Sales'
    				WHEN 'T' THEN 'Touring'
    				ELSE 'n/a'	
    			END AS prd_line, --Map product line code to descriptive values
    			CAST(prd_start_dt AS DATE) AS prd_start_dt,
    			CAST(
    				LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 
    				AS DATE
    				) AS prd_end_dt -- Calculate end date one day before start date of next order
    		FROM bronze.crm_prd_info;
    		SET @end_time = GETDATE()
    		PRINT'Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
    		PRINT'------------------------------------------------';
    		
    		SET @start_time = GETDATE();
    		PRINT '>>Truncating silver.crm_sales_details';
    		TRUNCATE TABLE silver.crm_sales_details;
    		PRINT '>>Inserting into silver.crm_sales_details';
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
    		-- Data Cleaning and Transformation 
    		SELECT 
    			sls_ord_num,
    			sls_prd_key,
    			sls_cust_id,
    			CASE 
    				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
    				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    			END AS sls_order_dt,
    			CASE 
    				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
    				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    			END AS sls_ship_dt,
    			CASE 
    				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
    				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    			END AS sls_due_dt,
    			CASE 
    				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
    				THEN sls_quantity * ABS(sls_price)
    				ELSE sls_sales
    			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
    			sls_quantity,
    			CASE 
    				WHEN sls_price IS NULL OR sls_price <= 0 
    				THEN sls_sales / NULLIF(sls_quantity, 0)
    				ELSE sls_price  -- Derive price if original value is invalid
    			END AS sls_price
    		FROM bronze.crm_sales_details;
    		SET @end_time = GETDATE()
    		PRINT'Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds'
    
    		PRINT'------------------------------------------------';
    		PRINT'Loading ERP Tables';
    		PRINT'------------------------------------------------';
    		SET @start_time = GETDATE();
    		PRINT '>>Truncating silver.erp_cust_az12';
    		TRUNCATE TABLE silver.erp_cust_az12;
    		PRINT'>>Inserting into silver.erp_cust_az12';
    		INSERT INTO silver.erp_cust_az12 (
    					cid,
    					bdate,
    					gen
    		)
    		-- Data Cleaning and Transformation 
    		SELECT
    		CASE
    			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
    			ELSE cid
    		END AS cid, 
    		CASE
    			WHEN bdate > GETDATE() THEN NULL
    			ELSE bdate
    		END AS bdate, -- Set future birthdates to NULL
    		CASE
    			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
    			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
    			ELSE 'n/a'
    		END AS gen -- Normalize gender values and handle unknown cases
    		FROM bronze.erp_cust_az12;
    		SET @end_time = GETDATE()
    		PRINT'Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
    		PRINT'------------------------------------------------';
    		
    		SET @start_time =  GETDATE();
    		PRINT '>>Truncating silver.erp_loc_a101';
    		TRUNCATE TABLE silver.erp_loc_a101;
    		PRINT'>>Inserting into silver.erp_loc_a101';
    		INSERT INTO silver.erp_loc_a101( 
    			cid,
    			cntry
    		)
    		-- Data Cleaning and Transformation 
    		SELECT
    			REPLACE(cid,'-','') cid, 
    			CASE 
    				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    				WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States'
    				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    				ELSE TRIM(cntry)
    			END AS cntry -- Normalize and handle missing or blank country codes
    		FROM bronze.erp_loc_a101;
    		SET @end_time = GETDATE()
    		PRINT'Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
    		PRINT'------------------------------------------------';
    
    		SET @start_time = GETDATE();
    		PRINT '>>Truncating erp_px_cat_g1v2';
    		TRUNCATE TABLE silver.erp_px_cat_g1v2;
    		PRINT'>>Inserting into erp_px_cat_g1v2';
    		INSERT INTO silver.erp_px_cat_g1v2(
    			id,
    			cat,
    			subcat,
    			maintenance
    		)
    		-- Data Cleaning and Transformation 
    		SELECT 
    			id,
    			cat,
    			subcat,
    			maintenance
    		FROM bronze.erp_px_cat_g1v2;
    		SET @end_time = GETDATE()
    		PRINT'Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
    		PRINT'------------------------------------------------';
    	SET @batch_end_time = GETDATE();
    	PRINT'Batch Load Duration: '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR)+' seconds'
    	PRINT'-----------------------------------------------------'
  	
  	END TRY	
  	BEGIN CATCH
    	PRINT'=====================================================';
    	PRINT'ERROR OCCURED DURIND LOADING SILVER LAYER';
    	PRINT'Error Message'+ERROR_MESSAGE();
    	PRINT'Error Number'+CAST(ERROR_NUMBER() AS NVARCHAR);
    	PRINT'Error State'+CAST(ERROR_STATE() AS NVARCHAR);
  	END CATCH
END
END;
