-- ============================================================
--  SCHEMA: bronze
--  PURPOSE: Base layer for CRM & ERP data ingestion (Bronze)
--  AUTHOR: Ashirwad Gupta
--  CREATED: CURRENT_DATE
-- ============================================================

-- 1️⃣ Create Schema
CREATE SCHEMA IF NOT EXISTS bronze;

-- ============================================================
-- 2️⃣ CRM TABLES
-- ============================================================

-- CRM Customer Info
CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
    cst_id            SERIAL PRIMARY KEY,               -- unique customer ID
    cst_key           VARCHAR(50) UNIQUE,               -- business/customer key
    cst_firstname     VARCHAR(100),
    cst_lastname      VARCHAR(100),
    cst_material_status VARCHAR(50),
    cst_gndr          VARCHAR(10),
    sct_create_date   DATE DEFAULT CURRENT_DATE
);

COMMENT ON TABLE bronze.crm_cust_info IS 'CRM Customer Information - basic details of customers';

-- ------------------------------------------------------------

-- CRM Product Info
CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
    prd_id         SERIAL PRIMARY KEY,                  -- internal product ID
    prd_key        VARCHAR(50) UNIQUE,                  -- product key (business)
    prd_nm         VARCHAR(255),
    prd_cost       NUMERIC(12,2),
    prd_line       VARCHAR(100),
    prd_start_dt   DATE,
    prd_end_dt     DATE
);

COMMENT ON TABLE bronze.crm_prd_info IS 'CRM Product Information - product master data';

-- ------------------------------------------------------------

-- CRM Sales Details
CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
    sls_ord_num    VARCHAR(50) PRIMARY KEY,             -- order number
    sls_prd_key    VARCHAR(50) REFERENCES bronze.crm_prd_info(prd_key),
    sls_cust_id    INT REFERENCES bronze.crm_cust_info(cst_id),
    sls_order_dt   DATE,
    sls_ship_dt    DATE,
    sls_due_dt     DATE,
    sls_sales      NUMERIC(12,2),
    sls_quantity   INT,
    sls_price      NUMERIC(12,2)
);

COMMENT ON TABLE bronze.crm_sales_details IS 'CRM Sales Transactions - line-level sales facts';

-- ============================================================
-- 3️⃣ ERP TABLES
-- ============================================================

-- ERP Customer AZ12
CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
    cid     INT PRIMARY KEY,
    bdate   DATE,
    gen     VARCHAR(10)
);

COMMENT ON TABLE bronze.erp_cust_az12 IS 'ERP Customer Data - customer demographics and basic info';

-- ------------------------------------------------------------

-- ERP Location A101
CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
    cid     INT REFERENCES bronze.erp_cust_az12(cid),
    cntry   VARCHAR(100),
    PRIMARY KEY (cid, cntry)
);

COMMENT ON TABLE bronze.erp_loc_a101 IS 'ERP Customer Location - maps customer to country/region';

-- ------------------------------------------------------------

-- ERP Product Category G1V2
CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
    id           INT PRIMARY KEY,
    cat          VARCHAR(100),
    subcat       VARCHAR(100),
    maintenance  VARCHAR(100)
);

COMMENT ON TABLE bronze.erp_px_cat_g1v2 IS 'ERP Product Category Mapping - category hierarchy';
