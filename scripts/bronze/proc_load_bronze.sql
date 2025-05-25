
USE master;
USE Datawarehouse;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	
		SET NOCOUNT ON;
		DECLARE @start_step DATETIME, @end_step DATETIME; -- for individual jobs
		DECLARE @step_start DATETIME, @step_end DATETIME; -- for bronze load
	
	PRINT '===================================================='
	PRINT 'Loading Bronze Layer'
	PRINT '===================================================='
	
		
	BEGIN TRY
		TRUNCATE TABLE bronze.bronze_load_log
		
		
		SET @step_start = GETDATE() 
		
		
		
		
		
		-------------------------------------
		PRINT '======LOADING FROM CRM======'
		PRINT '>>Truncating bronze.crm_cust_info';
		SET @start_step = GETDATE() 
		TRUNCATE TABLE bronze.crm_cust_info;
		SET @end_step = GETDATE()
		
		 
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'TRUNCATE bronze.crm_cust_info', 'SUCCESS',@start_step,@end_step, 'Truncate completed')
		
		PRINT '>> Inserting data into bronze.crm_cust_info';
		SET @start_step = GETDATE()
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\HP\Desktop\SQL Project\sql-datawarehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK
		);
		SET @end_step = GETDATE() 
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'INSERT into bronze.crm_cust_info', 'SUCCESS',@start_step,@end_step, 'Insertion completed')
		-----------------------------------------
		
		PRINT '>>Truncating bronze.crm_prd_info';
	
		SET @start_step = GETDATE() 
		TRUNCATE TABLE bronze.crm_prd_info;
		SET @end_step = GETDATE()
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'TRUNCATE bronze.crm_prd_info', 'SUCCESS',@start_step,@end_step, 'Truncate completed')
		PRINT '>> Inserting data into bronze.crm_prd_info';
		
		SET @start_step = GETDATE() 
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\HP\Desktop\SQL Project\sql-datawarehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK
		)
		SET @end_step = GETDATE()
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'INSERT into bronze.crm_prd_info', 'SUCCESS',@start_step,@end_step, 'Insertion completed')
		----------------------------------------------
		
		PRINT '>>Truncating bronze.crm_sales_details';
		SET @start_step = GETDATE() 
		TRUNCATE TABLE bronze.crm_sales_details;
		SET @end_step = GETDATE()
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'TRUNCATE bronze.crm_sales_details', 'SUCCESS',@start_step,@end_step, 'Truncate completed')
		
		PRINT '>> Inserting data into bronze.crm_sales_details';
	
		SET @start_step = GETDATE()
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\HP\Desktop\SQL Project\sql-datawarehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK
		);
		SET @end_step = GETDATE()
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'INSERT into bronze.crm_sales_details', 'SUCCESS',@start_step,@end_step, 'Insertion completed')
		-------------------------------------------
		PRINT '======LOADING FROM ERP======'
		-------------------------------------------
		
		PRINT '>>Truncating bronze.erp_cust_az12';


	
		SET @start_step = GETDATE() 
		TRUNCATE TABLE bronze.erp_cust_az12;
		SET @end_step = GETDATE()
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'TRUNCATE bronze.erp_cust_az12', 'SUCCESS',@start_step,@end_step, 'Truncate completed')
		
		PRINT '>> Inserting data into bronze.erp_cust_az12';
		SET @start_step = GETDATE() 
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\HP\Desktop\SQL Project\sql-datawarehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK
		);
		SET @end_step = GETDATE()
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'INSERT into bronze.erp_cust_az12', 'SUCCESS',@start_step,@end_step, 'Insertion completed')
		
		-------------------------------------------
		
		PRINT '>>Truncating bronze.erp_loc_a101';
		SET @start_step = GETDATE() 
		TRUNCATE TABLE bronze.erp_loc_a101;
		SET @end_step = GETDATE()
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'TRUNCATE bronze.erp_loc_a101', 'SUCCESS',@start_step,@end_step, 'Truncate completed')
		
		PRINT '>> Inserting data into bronze.erp_loc_a101';
		SET @start_step = GETDATE() 
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\HP\Desktop\SQL Project\sql-datawarehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK
		);
		SET @end_step = GETDATE()
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'INSERT into bronze.erp_loc_a101', 'SUCCESS',@start_step,@end_step, 'Insertion completed')
		----------------------------------------------------
		
		PRINT '>>Truncating bronze.erp_px_cat_g1v2';
		SET @start_step = GETDATE()
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		SET @end_step = GETDATE()
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'TRUNCATE bronze.erp_px_cat_g1v2', 'SUCCESS',@start_step,@end_step, 'Truncate completed')
		
		PRINT '>> Inserting data into bronze.erp_px_cat_g1v2';
		SET @start_step = GETDATE() 
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\HP\Desktop\SQL Project\sql-datawarehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK
		);
		SET @end_step = GETDATE()
		INSERT INTO bronze.bronze_load_log(job_name, step_name, status, start_time, end_time, message) VALUES ('load_bronze', 'INSERT into bronze.erp_px_cat_g1v2', 'SUCCESS',@start_step,@end_step, 'Insertion completed')
		--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
		SET @step_end = GETDATE()
		INSERT INTO bronze.bronze_load_log (job_name, step_name, status, start_time, end_time, message)
		VALUES ('Bronze layer load', 'Layer Load', 'SUCCESS', @step_start, @step_end, 'Bronze layer load complete')
		
	END TRY
	BEGIN CATCH
		PRINT '===============================================';
		PRINT 'An error occured while loading the Bronze layer';
		PRINT '>>>ERROR MESSAGE '+ERROR_MESSAGE();
		PRINT '>>>ERROR NUMBER '+CAST(ERROR_NUMBER() AS VARCHAR(10));
		PRINT '>>>LINE NUMBER '+CAST(ERROR_LINE() AS VARCHAR(10));
		PRINT '===============================================';
		INSERT INTO bronze.bronze_load_log (job_name, step_name, status, start_time, end_time, message)
		VALUES ('Bronze layer load', 'Layer Load', 'FAILED', @step_start, @step_end, 'Bronze layer load Failed')
	END CATCH
	
END
;

EXEC bronze.load_bronze

SELECT * FROM bronze.bronze_load_log bll






