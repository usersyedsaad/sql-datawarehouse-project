



CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_first_name VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
);

CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost VARCHAR(50),
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
);


CREATE TABLE bronze.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id VARCHAR(50),
sls_order_dt VARCHAR(50),
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales_date DATE,
sls_sales DECIMAL(18,2),
sls_quantity INT,
sls_price DECIMAL(18,2)
);

CREATE TABLE bronze.erp_cust_az12(
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50)
);

CREATE TABLE bronze.erp_loc_a101(
cid VARCHAR(50),
cntry VARCHAR(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2(
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(20)
);

CREATE TABLE DataWarehouse.bronze.bronze_load_log (
	log_id int IDENTITY(1,1) NOT NULL,
	job_name nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	step_name nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	status nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	message nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	start_time datetime DEFAULT getdate() NULL,
	end_time datetime NULL,
	total_time datetime NULL,
	error_message nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	error_number int NULL,
	error_line int NULL,
	CONSTRAINT PK__bronze_l__9E2397E090F43607 PRIMARY KEY (log_id)
);



