CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
    row_count BIGINT;
BEGIN
    /*
    ==============================================================================
    Saklı Yordam: Silver Katmanı Yükle (Bronze → Silver)
    ==============================================================================
    Amaç:
        Bronze katmanındaki verileri dönüştürüp Silver şemasına yükler.
        - Silver tabloları truncate edilir.
        - Bronze verisi temizlenip dönüştürülerek Silver tablolarına yazılır.

    Parametre:
        Yok (Bu prosedür parametre almaz ve değer döndürmez)

    Kullanım:
        CALL silver.load_silver();
    ==============================================================================
    */

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Silver Katmanı Yükleniyor';
    RAISE NOTICE '==========================================';

    -- crm_cust_info
    BEGIN
        RAISE NOTICE '>> silver.crm_cust_info yükleniyor...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> crm_cust_info yüklendi. Süre: % sn', EXTRACT(SECOND FROM duration);
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: crm_cust_info → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    -- crm_prd_info
    BEGIN
        RAISE NOTICE '>> silver.crm_prd_info yükleniyor...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost,
            prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_'),
            SUBSTRING(prd_key FROM 7),
            prd_nm,
            COALESCE(prd_cost, 0),
            CASE
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE)
        FROM bronze.crm_prd_info;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> crm_prd_info yüklendi. Süre: % sn', EXTRACT(SECOND FROM duration);
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: crm_prd_info → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    -- crm_sales_details
    BEGIN
        RAISE NOTICE '>> silver.crm_sales_details yükleniyor...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR char_length(sls_order_dt::text) != 8 THEN NULL
                 ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD') END,
            CASE WHEN sls_ship_dt = 0 OR char_length(sls_ship_dt::text) != 8 THEN NULL
                 ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD') END,
            CASE WHEN sls_due_dt = 0 OR char_length(sls_due_dt::text) != 8 THEN NULL
                 ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD') END,
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0
                     OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0
                 THEN sls_sales / NULLIF(sls_quantity, 0)
                 ELSE sls_price END
        FROM bronze.crm_sales_details;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> crm_sales_details yüklendi. Süre: % sn', EXTRACT(SECOND FROM duration);
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: crm_sales_details → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    -- erp_cust_az12
    BEGIN
        RAISE NOTICE '>> silver.erp_cust_az12 yükleniyor...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4) ELSE cid END,
            CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> erp_cust_az12 yüklendi. Süre: % sn', EXTRACT(SECOND FROM duration);
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: erp_cust_az12 → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    -- erp_loc_a101
    BEGIN
        RAISE NOTICE '>> silver.erp_loc_a101 yükleniyor...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> erp_loc_a101 yüklendi. Süre: % sn', EXTRACT(SECOND FROM duration);
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: erp_loc_a101 → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    -- erp_px_cat_g1v2
    BEGIN
        RAISE NOTICE '>> silver.erp_px_cat_g1v2 yükleniyor...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> erp_px_cat_g1v2 yüklendi. Süre: % sn', EXTRACT(SECOND FROM duration);
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: erp_px_cat_g1v2 → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Silver Katmanı Yüklemesi Tamamlandı!';
    RAISE NOTICE '==========================================';

END;
$$;
