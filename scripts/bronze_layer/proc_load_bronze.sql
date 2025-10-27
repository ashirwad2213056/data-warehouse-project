-- ==============================================================================================
-- File Name   : bronze_load_procedure.sql
-- Description : Loads CSV data into Bronze Layer tables.
--               Includes TRUNCATE, COPY, error handling, and duration tracking for each table.
-- Schema      : bronze
-- Database    : PostgreSQL
-- Author      : Ashirwad Gupta
-- File Type   : .sql (Standard SQL Script)
-- Execute In  : pgAdmin 4 Query Tool or psql CLI
-- ==============================================================================================

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;

    -- Error tracking variables
    err_msg TEXT;
    err_detail TEXT;
    err_hint TEXT;
    err_sqlstate TEXT;
BEGIN
    RAISE NOTICE '===========================================';
    RAISE NOTICE 'Starting Bronze Layer Load';
    RAISE NOTICE '===========================================';

    ----------------------------------------------------------------
    -- CRM Customer Info
    ----------------------------------------------------------------
    RAISE NOTICE '>> CRM Customer Info';
    v_start_time := clock_timestamp();
    BEGIN
        TRUNCATE TABLE bronze.crm_cust_info;
        EXECUTE $sql$
            COPY bronze.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname,
                                      cst_material_status, cst_gndr, sct_create_date)
            FROM 'C:/postgres_data/cust_info.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '\')
        $sql$;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '✅ CRM Customer Info loaded successfully! Duration: %', v_duration;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            err_msg = MESSAGE_TEXT,
            err_detail = PG_EXCEPTION_DETAIL,
            err_hint = PG_EXCEPTION_HINT,
            err_sqlstate = RETURNED_SQLSTATE;
        RAISE NOTICE '❌ CRM Customer Info load failed!';
        RAISE NOTICE 'Error: % | SQLSTATE: %', err_msg, err_sqlstate;
    END;

    ----------------------------------------------------------------
    -- CRM Product Info
    ----------------------------------------------------------------
    RAISE NOTICE '>> CRM Product Info';
    v_start_time := clock_timestamp();
    BEGIN
        TRUNCATE TABLE bronze.crm_prd_info;
        EXECUTE $sql$
            COPY bronze.crm_prd_info(prd_id, prd_key, prd_nm, prd_cost, prd_line,
                                     prd_start_dt, prd_end_dt)
            FROM 'C:/postgres_data/prd_info.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '\')
        $sql$;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '✅ CRM Product Info loaded successfully! Duration: %', v_duration;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            err_msg = MESSAGE_TEXT,
            err_detail = PG_EXCEPTION_DETAIL,
            err_hint = PG_EXCEPTION_HINT,
            err_sqlstate = RETURNED_SQLSTATE;
        RAISE NOTICE '❌ CRM Product Info load failed!';
        RAISE NOTICE 'Error: % | SQLSTATE: %', err_msg, err_sqlstate;
    END;

    ----------------------------------------------------------------
    -- CRM Sales Details
    ----------------------------------------------------------------
    RAISE NOTICE '>> CRM Sales Details';
    v_start_time := clock_timestamp();
    BEGIN
        TRUNCATE TABLE bronze.crm_sales_details;
        EXECUTE $sql$
            COPY bronze.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id,
                                          sls_order_dt, sls_ship_dt, sls_due_dt,
                                          sls_sales, sls_quantity, sls_price)
            FROM 'C:/postgres_data/sales_details.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '\')
        $sql$;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '✅ CRM Sales Details loaded successfully! Duration: %', v_duration;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            err_msg = MESSAGE_TEXT,
            err_detail = PG_EXCEPTION_DETAIL,
            err_hint = PG_EXCEPTION_HINT,
            err_sqlstate = RETURNED_SQLSTATE;
        RAISE NOTICE '❌ CRM Sales Details load failed!';
        RAISE NOTICE 'Error: % | SQLSTATE: %', err_msg, err_sqlstate;
    END;

    ----------------------------------------------------------------
    -- ERP Customer AZ12
    ----------------------------------------------------------------
    RAISE NOTICE '>> ERP Customer AZ12';
    v_start_time := clock_timestamp();
    BEGIN
        TRUNCATE TABLE bronze.erp_cust_az12;
        EXECUTE $sql$
            COPY bronze.erp_cust_az12(cid, bdate, gen)
            FROM 'C:/postgres_data/CUST_AZ12.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '\')
        $sql$;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '✅ ERP Customer AZ12 loaded successfully! Duration: %', v_duration;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            err_msg = MESSAGE_TEXT,
            err_detail = PG_EXCEPTION_DETAIL,
            err_hint = PG_EXCEPTION_HINT,
            err_sqlstate = RETURNED_SQLSTATE;
        RAISE NOTICE '❌ ERP Customer AZ12 load failed!';
        RAISE NOTICE 'Error: % | SQLSTATE: %', err_msg, err_sqlstate;
    END;

    ----------------------------------------------------------------
    -- ERP Location A101
    ----------------------------------------------------------------
    RAISE NOTICE '>> ERP Location A101';
    v_start_time := clock_timestamp();
    BEGIN
        TRUNCATE TABLE bronze.erp_loc_a101;
        EXECUTE $sql$
            COPY bronze.erp_loc_a101(cid, cntry)
            FROM 'C:/postgres_data/LOC_A101.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '\')
        $sql$;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '✅ ERP Location A101 loaded successfully! Duration: %', v_duration;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            err_msg = MESSAGE_TEXT,
            err_detail = PG_EXCEPTION_DETAIL,
            err_hint = PG_EXCEPTION_HINT,
            err_sqlstate = RETURNED_SQLSTATE;
        RAISE NOTICE '❌ ERP Location A101 load failed!';
        RAISE NOTICE 'Error: % | SQLSTATE: %', err_msg, err_sqlstate;
    END;

    ----------------------------------------------------------------
    -- ERP Product Category G1V2
    ----------------------------------------------------------------
    RAISE NOTICE '>> ERP Product Category G1V2';
    v_start_time := clock_timestamp();
    BEGIN
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        EXECUTE $sql$
            COPY bronze.erp_px_cat_g1v2(id, cat, subcat, maintenance)
            FROM 'C:/postgres_data/PX_CAT_G1V2.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '\')
        $sql$;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '✅ ERP Product Category G1V2 loaded successfully! Duration: %', v_duration;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            err_msg = MESSAGE_TEXT,
            err_detail = PG_EXCEPTION_DETAIL,
            err_hint = PG_EXCEPTION_HINT,
            err_sqlstate = RETURNED_SQLSTATE;
        RAISE NOTICE '❌ ERP Product Category G1V2 load failed!';
        RAISE NOTICE 'Error: % | SQLSTATE: %', err_msg, err_sqlstate;
    END;

    ----------------------------------------------------------------
    -- Completion
    ----------------------------------------------------------------
    RAISE NOTICE '===========================================';
    RAISE NOTICE 'Bronze Layer load process completed.';
    RAISE NOTICE '===========================================';
END;
$$;
