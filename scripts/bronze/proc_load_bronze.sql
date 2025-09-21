/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR REPLACE PROCEDURE BRONZE.LOAD_BRONZE()
LANGUAGE plpgsql
as $$
DECLARE 
	START_TIME TIMESTAMP ;
	END_TIME TIMESTAMP ;
begin 

	RAISE NOTICE '=========================================';
	RAISE NOTICE 'LOADING BRONZE LAYER';
	RAISE NOTICE '=========================================';
	

	RAISE NOTICE '=========================================';
	RAISE NOTICE 'LOADING CRM TABLES';
	RAISE NOTICE '=========================================';

	RAISE NOTICE '-------------------------';
	START_TIME := CURRENT_TIMESTAMP ;
	RAISE NOTICE '>>TRUNCATING TABLE :BRONZE.CRM_BUST_INFO';
	TRUNCATE TABLE BRONZE.CRM_CUST_INFO;

	RAISE NOTICE '>>INSERTING DATA INTO :BRONZE.CRM_BUST_INFO';
	COPY BRONZE.CRM_CUST_INFO(CST_ID,CST_KEY,CST_FIRSTNAME,CST_LASTNAME,CST_MARTIAL_STATUS,CST_GNDR,CST_CREATE_DATE)
	FROM 'C:\Users\Dev\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	DELIMITER ','
	CSV HEADER ;
	
	END_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> LOAD DURATION % ' ,AGE(END_TIME,START_TIME) ;
	RAISE NOTICE '-------------------------';

	RAISE NOTICE '-------------------------';
	START_TIME := CURRENT_TIMESTAMP ;
	
	RAISE NOTICE '>>TRUNCATING TABLE :BRONZE.CRM_PRD_INFO';
	TRUNCATE TABLE BRONZE.CRM_PRD_INFO;
	
	RAISE NOTICE '>>INSERTING DATA INTO :BRONZE.CRM_BUST_INFO';
	COPY BRONZE.CRM_PRD_INFO(PRD_ID,PRD_KEY,PRD_NM,PRD_COST,PRD_LINE,PRD_START_DT,PRD_END_DT)
	FROM 'C:\Users\Dev\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	DELIMITER ','
	CSV HEADER ;

	END_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> LOAD DURATION % ' ,AGE(END_TIME,START_TIME) ;
	RAISE NOTICE '-------------------------';

	RAISE NOTICE '-------------------------';
	START_TIME := CURRENT_TIMESTAMP ;
	
	RAISE NOTICE '>>TRUNCATING TABLE :BRONZE.CRM_SALES_DETAILS';
	TRUNCATE TABLE BRONZE.CRM_SALES_DETAILS;

	RAISE NOTICE '>>INSERTING DATA INTO :BRONZE.CRM_SALES_DETAILS';
	COPY BRONZE.CRM_SALES_DETAILS(SLS_ORD_NUM,SLS_PRD_KEY,SLS_CUST_ID,SLS_ORDER_DT,SLS_SHIP_DT,SLS_DUE_DT,SLS_SALES,SLS_QUANTITY,SLS_PRICE)
	FROM 'C:\Users\Dev\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	DELIMITER ','
	CSV HEADER ;

	END_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> LOAD DURATION % ' ,AGE(END_TIME,START_TIME) ;
	RAISE NOTICE '-------------------------';

	RAISE NOTICE '=========================================';
	RAISE NOTICE 'LOADING ERP TABLES';
	RAISE NOTICE '=========================================';

	RAISE NOTICE '-------------------------';
	START_TIME := CURRENT_TIMESTAMP ;
	
	RAISE NOTICE '>>TRUNCATING TABLE :BRONZE.ERP_CUST_AZ12';
	TRUNCATE TABLE BRONZE.ERP_CUST_AZ12;
	
	RAISE NOTICE '>>INSERTING DATA INTO :BRONZE.ERP_CUST_AZ12';

	COPY BRONZE.ERP_CUST_AZ12(CID,BDATE,GEN)
	FROM 'C:\Users\Dev\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	DELIMITER ','
	CSV HEADER ;

	END_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> LOAD DURATION % ' ,AGE(END_TIME,START_TIME) ;
	RAISE NOTICE '-------------------------';

	RAISE NOTICE '-------------------------';
	START_TIME := CURRENT_TIMESTAMP ;

	RAISE NOTICE '>>TRUNCATING TABLE :BRONZE.ERP_LOC_A101';
	TRUNCATE TABLE BRONZE.ERP_LOC_A101;

	RAISE NOTICE '>>INSERTING DATA INTO :BRONZE.ERP_LOC_A101';
	COPY BRONZE.ERP_LOC_A101(CID,CNTRY)
	FROM 'C:\Users\Dev\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	DELIMITER ','
	CSV HEADER ;

	END_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> LOAD DURATION % ' ,AGE(END_TIME,START_TIME) ;
	RAISE NOTICE '-------------------------';

	RAISE NOTICE '-------------------------';
	START_TIME := CURRENT_TIMESTAMP ;

	RAISE NOTICE '>>TRUNCATING TABLE :BRONZE.ERP_PX_CAT_G1V2';
	TRUNCATE TABLE BRONZE.ERP_PX_CAT_G1V2;
	
	RAISE NOTICE '>>INSERTING DATA INTO :BRONZE.ERP_PX_CAT_G1V2';
	COPY BRONZE.ERP_PX_CAT_G1V2(ID,CAT,SUBCAT,MAINTENANCE)
	FROM 'C:\Users\Dev\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	DELIMITER ','
	CSV HEADER ;

	END_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> LOAD DURATION % ' ,AGE(END_TIME,START_TIME) ;
	RAISE NOTICE '-------------------------';

EXCEPTION 
	WHEN OTHERS THEN 
		RAISE NOTICE '==============================';
		RAISE NOTICE 'SOME UNEXPECTED ERROR OCCURED!';
		RAISE NOTICE '==============================';
	
end;
$$;

CALL bronze.load_bronze();


