CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
    row_count BIGINT;
BEGIN
    /*
    ===============================================================================
    Saklı Yordam: Bronze Katmanı Yükle (CSV -> bronze şeması)
    Açıklama:
        - Bronze tablolardaki veriler silinir (truncate)
        - CSV dosyalarından COPY ile veri yüklenir
        - Her yükleme süresi ve satır sayısı loglanır
        - Hata durumları yakalanır ve bildirilir
    Kullanım:
        CALL bronze.load_bronze();
    ===============================================================================
    */

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Bronze Katmanı Yükleniyor';
    RAISE NOTICE '================================================';

    -- CRM: crm_cust_info
    BEGIN
        RAISE NOTICE '>> Tablo temizleniyor: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISE NOTICE '>> Veri yükleniyor: bronze.crm_cust_info';
        start_time := clock_timestamp();
        COPY bronze.crm_cust_info
        FROM '/Users/onurakyurek/Desktop/YTU_Proje/datasets/source_crm/cust_info.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');
        GET DIAGNOSTICS row_count = ROW_COUNT;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> Süre: % sn | Satır sayısı: %', EXTRACT(SECOND FROM duration), row_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: crm_cust_info yüklenemedi → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    -- CRM: crm_prd_info
    BEGIN
        RAISE NOTICE '>> Tablo temizleniyor: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISE NOTICE '>> Veri yükleniyor: bronze.crm_prd_info';
        start_time := clock_timestamp();
        COPY bronze.crm_prd_info
        FROM '/Users/onurakyurek/Desktop/YTU_Proje/datasets/source_crm/prd_info.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');
        GET DIAGNOSTICS row_count = ROW_COUNT;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> Süre: % sn | Satır sayısı: %', EXTRACT(SECOND FROM duration), row_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: crm_prd_info yüklenemedi → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    -- crm_sales_details
    BEGIN
        RAISE NOTICE '>> Tablo temizleniyor: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISE NOTICE '>> Veri yükleniyor: bronze.crm_sales_details';
        start_time := clock_timestamp();
        COPY bronze.crm_sales_details
        FROM '/Users/onurakyurek/Desktop/YTU_Proje/datasets/source_crm/sales_details.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');
        GET DIAGNOSTICS row_count = ROW_COUNT;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> Süre: % sn | Satır sayısı: %', EXTRACT(SECOND FROM duration), row_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: crm_sales_details yüklenemedi → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    -- erp_loc_a101
    BEGIN
        RAISE NOTICE '>> Tablo temizleniyor: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISE NOTICE '>> Veri yükleniyor: bronze.erp_loc_a101';
        start_time := clock_timestamp();
        COPY bronze.erp_loc_a101
        FROM '/Users/onurakyurek/Desktop/YTU_Proje/datasets/source_erp/loc_a101.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');
        GET DIAGNOSTICS row_count = ROW_COUNT;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> Süre: % sn | Satır sayısı: %', EXTRACT(SECOND FROM duration), row_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: erp_loc_a101 yüklenemedi → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    -- erp_cust_az12
    BEGIN
        RAISE NOTICE '>> Tablo temizleniyor: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISE NOTICE '>> Veri yükleniyor: bronze.erp_cust_az12';
        start_time := clock_timestamp();
        COPY bronze.erp_cust_az12
        FROM '/Users/onurakyurek/Desktop/YTU_Proje/datasets/source_erp/cust_az12.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');
        GET DIAGNOSTICS row_count = ROW_COUNT;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> Süre: % sn | Satır sayısı: %', EXTRACT(SECOND FROM duration), row_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: erp_cust_az12 yüklenemedi → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    -- erp_px_cat_g1v2
    BEGIN
        RAISE NOTICE '>> Tablo temizleniyor: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE '>> Veri yükleniyor: bronze.erp_px_cat_g1v2';
        start_time := clock_timestamp();
        COPY bronze.erp_px_cat_g1v2
        FROM '/Users/onurakyurek/Desktop/YTU_Proje/datasets/source_erp/px_cat_g1v2.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');
        GET DIAGNOSTICS row_count = ROW_COUNT;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE '>> Süre: % sn | Satır sayısı: %', EXTRACT(SECOND FROM duration), row_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Hata: erp_px_cat_g1v2 yüklenemedi → % (kod: %)', SQLERRM, SQLSTATE;
    END;

    RAISE NOTICE '===========================================';
    RAISE NOTICE 'Bronze Katmanı Yüklemesi Başarıyla Tamamlandı!';
    RAISE NOTICE '===========================================';
END;
$$;
