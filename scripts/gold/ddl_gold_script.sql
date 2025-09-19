/*
=================================================
DDL Script: Create Gold Views
=================================================
Script Purpose:
  This script creates views for the Gold layer in the data warehouse. The Gold layer represents the final dimension and fact tables (Star Schema)
  Each viewforms transformations and combines data from the Silver layer to produce a clean, enriched, and business-ready dataset.

Usage:
  These views can be queried directly for analytics and reporting.
=====================================================
*/

/*
=================================================
Creating Dimension : GOLD.DIM_CUSTOMER
=================================================
*/

CREATE VIEW GOLD.DIM_CUSTOMER AS
	SELECT 
		ROW_NUMBER() OVER(ORDER BY CI.CST_ID) AS CUSTOMER_KEY, 
		CI.CST_ID AS CUSTOMER_ID,
		CI.CST_KEY AS CUSTOMER_NUMBER,
		CI.CST_FIRSTNAME AS FIRST_NAME,
		CI.CST_LASTNAME AS LAST_NAME,
		LA.CNTRY AS COUNTRY,
		CI.CST_MARTIAL_STATUS AS MARITAL_STATUS,
		CASE WHEN CI.CST_GNDR != 'N/A' THEN CI.CST_GNDR -- CRM IS THE MASTER FIR GENDER INFO
			ELSE COALESCE(CA.GEN,'N/A')
		END AS GENDER,
		CA.BDATE AS BIRTHDATE,
		CI.CST_CREATE_DATE AS CREATE_DATE
	FROM SILVER.CRM_CUST_INFO AS CI
	LEFT JOIN SILVER.ERP_CUST_AZ12 AS CA
	ON CI.CST_KEY = CA.CID
	LEFT JOIN SILVER.ERP_LOC_A101 AS LA
	ON CI.CST_KEY = LA.CID

  /*
=================================================
Creating Dimension : GOLD.DIM_PRODUCT
=================================================
*/

CREATE VIEW GOLD.DIM_PRODUCT AS
	SELECT 	
		ROW_NUMBER() OVER(ORDER BY PN.PRD_START_DT,PN.PRD_KEY) AS PRODUCT_KEY,
		PN.PRD_ID  AS PRODUCT_ID,
		PN.PRD_KEY AS PRODUCT_NUMBER,
		PN.PRD_NM AS PRODUCT_NAME,
		PN.CAT_ID AS CATEGORY_ID,
		PC.CAT AS CATEGORY,
		PC.SUBCAT AS SUBCATEGORY,
		PC.MAINTENANCE,
		PN.PRD_COST AS COST,
		PN.PRD_LINE AS PRODUCT_LINE,
		PN.PRD_START_DT	AS START_DATE
	FROM SILVER.CRM_PRD_INFO AS PN
	LEFT JOIN SILVER.ERP_PX_CAT_G1V2 AS PC
	ON PN.CAT_ID = PC.ID
	WHERE PRD_END_DT IS NULL -- FILTER OUT ALL HISTORICAL DATA

 /*
=================================================
Creating FACT : GOLD.FACT_SALES
=================================================
*/	
CREATE VIEW GOLD.FACT_SALES AS 
	SELECT 
		SD.SLS_ORD_NUM AS ORDER_NUMBER,
		PR.PRODUCT_KEY,
		CR.CUSTOMER_KEY,
		SD.SLS_ORDER_DT AS ORDER_DATE,
		SD.SLS_SHIP_DT AS SHIPPING_DATE,
		SD.SLS_DUE_DT AS DUE_DATE, 
		SD.SLS_SALES AS SALES_AMOUNT,
		SD.SLS_QUANTITY AS QUANTITY,
		SD.SLS_PRICE AS PRICE
	FROM SILVER.CRM_SALES_DETAILS AS SD
	LEFT JOIN GOLD.DIM_PRODUCT AS PR
	ON SD.SLS_PRD_KEY = PR.PRODUCT_NUMBER
	LEFT JOIN GOLD.DIM_CUSTOMER AS CR
	ON SD.SLS_PRD_KEY = CR.CUSTOMER_ID

