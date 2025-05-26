
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
	SET
	NOCOUNT ON;
	
	DECLARE 
        @batch_start_time DATETIME = GETDATE(),
	@batch_end_time DATETIME,
	@start_step DATETIME,
	@end_step DATETIME;
-- Clear log table
    TRUNCATE
	TABLE silver.silver_load_log;
--------------------------
-- Step 1: Truncate crm_cust_info
--------------------------
    SET
@start_step = GETDATE();

PRINT 'TRUNCATING silver.crm_cust_info';

TRUNCATE
	TABLE silver.crm_cust_info;

SET
@end_step = GETDATE();

INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'TRUNCATE and INSERT INTO silver.crm_cust_info',
        'TRUNCATING silver.crm_cust_info',
        'SUCCESS',
        'Truncate complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );
--------------------------
-- Step 2: Insert Transformed Data
--------------------------
    SET
@start_step = GETDATE();

PRINT 'INSERTING INTO silver.crm_cust_info';

INSERT
	INTO
	silver.crm_cust_info (
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
	TRIM(cst_firstname),
	TRIM(cst_lastname),
	CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'N/A'
	END,
	CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'N/A'
	END,
	cst_create_date
FROM
	(
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id
	ORDER BY
		cst_create_date DESC) AS row_num
	FROM
		bronze.crm_cust_info
    ) AS sub
WHERE
	row_num = 1
	AND cst_id IS NOT NULL;

SET
@end_step = GETDATE();

INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'TRUNCATE and INSERT INTO silver.crm_cust_info',
        'INSERTING INTO silver.crm_cust_info',
        'SUCCESS',
        'Insert complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );
--------------------------
-- Step 3: Truncate crm_prd_info
--------------------------
    SET
@start_step = GETDATE();

PRINT 'TRUNCATING silver.crm_prd_info';

TRUNCATE
	TABLE silver.crm_prd_info;

SET
@end_step = GETDATE();

INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'TRUNCATE and INSERT INTO silver.crm_prd_info',
        'TRUNCATING silver.crm_prd_info',
        'SUCCESS',
        'Truncate complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );
------Inserting into silver.crm_prd_info
    	
    
 set @start_step = GETDATE()

INSERT
	INTO
	silver.crm_prd_info(
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
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE
		WHEN TRIM(prd_line) = 'M' THEN 'Mountain'
		WHEN TRIM(prd_line) = 'R' THEN 'Road'
		WHEN TRIM(prd_line) = 'S' THEN 'Other Sales'
		WHEN TRIM(prd_line) = 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line,
	prd_start_dt,
	DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM
	DATAWAREHOUSE.BRONZE.crm_prd_info cpi ;
SET @end_step = GETDATE()
INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'TRUNCATE and INSERT INTO silver.crm_prd_info',
        'INSERTING INTO silver.crm_prd_info',
        'SUCCESS',
        'Insert complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );



	    SET
@start_step = GETDATE();

PRINT 'TRUNCATING silver.crm_sales_details';

TRUNCATE
	TABLE silver.crm_sales_details;

SET
@end_step = GETDATE();

INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'TRUNCATE and INSERT INTO silver.crm_sales_details',
        'TRUNCATING silver.crm_sales_details',
        'SUCCESS',
        'Truncate complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );
------Inserting into silver.crm_sales_details
    	
    
 set @start_step = GETDATE()

INSERT INTO silver.crm_sales_details(
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
    sls_cust_id,		 -- Safely cast sls_order_dt
    CAST(
        CASE
            WHEN LEN(sls_order_dt) < 8 THEN DATEADD(DAY,-1,sls_ship_dt)
            ELSE sls_order_dt
        END AS DATE
    ) AS sls_order_dt,	-- Safely cast sls_ship_dt
    CAST(
        CASE
            WHEN LEN(sls_ship_dt) < 8 THEN NULL
            ELSE sls_ship_dt
        END AS DATE
    ) AS sls_ship_dt,	-- Safely cast sls_due_dt
    CAST(
        CASE
            WHEN LEN(sls_due_dt) < 8 THEN NULL
            ELSE sls_due_dt
        END AS DATE
    ) AS sls_due_dt,
    CASE
	    WHEN (sls_sales IS NULL OR sls_sales < 1) THEN sls_price*sls_quantity
	    ELSE sls_sales
	END as sls_sales,
    sls_quantity,
    CASE
    	WHEN (sls_price IS NULL OR sls_price<1) THEN sls_sales/sls_quantity
    	ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

SET @end_step = GETDATE()
INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'TRUNCATE and INSERT INTO silver.crm_sales_details',
        'INSERTING INTO silver.crm_sales_details',
        'SUCCESS',
        'Insert complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );


		    SET
@start_step = GETDATE();

PRINT 'TRUNCATING silver.erp_cust_az12';

TRUNCATE
	TABLE silver.erp_cust_az12;

SET
@end_step = GETDATE();

INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'TRUNCATE and INSERT INTO silver.erp_cust_az12',
        'TRUNCATING silver.erp_cust_az12',
        'SUCCESS',
        'Truncate complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );
------Inserting into silver.erp_cust_az12
    
 set @start_step = GETDATE()

INSERT INTO DataWarehouse.silver.erp_cust_az12(
cid, bdate, gen
)
SELECT
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
  END AS cid,
  CASE
    WHEN bdate > GETDATE() THEN NULL
    ELSE bdate
  END AS bdate,
  CASE
    WHEN REPLACE(REPLACE(REPLACE(REPLACE(
           UPPER(
             TRIM(
               REPLACE(REPLACE(REPLACE(REPLACE(gen, CHAR(160), ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), '')
             )
           ), CHAR(160), ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), '') IN ('M', 'MALE') 
         THEN 'Male'
    WHEN REPLACE(REPLACE(REPLACE(REPLACE(
           UPPER(
             TRIM(
               REPLACE(REPLACE(REPLACE(REPLACE(gen, CHAR(160), ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), '')
             )
           ), CHAR(160), ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), '') IN ('F', 'FEMALE') 
         THEN 'Female'
    WHEN gen IS NULL OR TRIM(gen) = '' THEN NULL
    ELSE 'N/A'
  END AS gen
FROM Datawarehouse.bronze.erp_cust_az12;
 SET @end_step = GETDATE()
 
 
INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'Truncating and INSERT INTO silver.erp_cust_az12',
        'Inserting INTO silver.erp_cust_az12',
        'SUCCESS',
        'Insert complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );
-------------------------------------new table------------------------


		    SET
@start_step = GETDATE();

PRINT 'TRUNCATING silver.erp_loc_a101';

TRUNCATE
	TABLE silver.erp_loc_a101;

SET
@end_step = GETDATE();

INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'TRUNCATE and INSERT INTO silver.erp_loc_a101',
        'TRUNCATING silver.erp_loc_a101',
        'SUCCESS',
        'Truncate complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );
------Inserting into silver.erp_cust_az12
    
 set @start_step = GETDATE()
INSERT INTO DataWarehouse.silver.erp_loc_a101 (
cid, cntry
)
SELECT 
REPLACE(cid, '-', '_') as cid,
CASE
	WHEN TRIM(cntry) = 'US' THEN 'USA'
	WHEN TRIM(cntry) = 'DE' THEN 'Denmark'
	ELSE cntry
END AS cntry
FROM DataWarehouse.bronze.erp_loc_a101
WHERE cid IS NULL OR cntry IS NULL;


 SET @end_step = GETDATE()
 
 
INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'Truncating and INSERT INTO silver.erp_loc_a101',
        'Inserting INTO silver.erp_loc_a101',
        'SUCCESS',
        'Insert complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );

	

----------------------------------------NEW TABLE-----------------------------

		
		    SET
@start_step = GETDATE();

PRINT 'TRUNCATING silver.erp_px_cat_g1v2';

TRUNCATE
	TABLE silver.erp_px_cat_g1v2;

SET
@end_step = GETDATE();

INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'TRUNCATE and INSERT INTO silver.erp_px_cat_g1v2',
        'TRUNCATING silver.erp_px_cat_g1v2',
        'SUCCESS',
        'Truncate complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );
------Inserting into silver.erp_cust_az12
    
 set @start_step = GETDATE()
INSERT INTO datawarehouse.bronze.erp_px_cat_g1v2 (
	id, cat, subcat, maintenance
)
SELECT * FROM datawarehouse.bronze.erp_px_cat_g1v2 epcgv 

 SET @end_step = GETDATE()
 
 
INSERT
	INTO
	silver.silver_load_log (
        job_name,
	step_name,
	status,
	message,
	total_time
    )
VALUES (
        'Truncating and INSERT INTO silver.erp_px_cat_g1v2',
        'Inserting INTO silver.erp_px_cat_g1v2',
        'SUCCESS',
        'Insert complete',
        CAST(DATEDIFF(SECOND, @start_step, @end_step) AS DECIMAL(10, 2))
    );

	


-- Final log
    SET
@batch_end_time = GETDATE();

PRINT 'Procedure completed in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds';
END;



EXEC silver.load_silver


SELECT * FROM silver.silver_load_log
