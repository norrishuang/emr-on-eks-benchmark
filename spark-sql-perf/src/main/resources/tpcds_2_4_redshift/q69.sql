--q69.sql--

 select
    cd_gender, cd_marital_status, cd_education_status, count(*) cnt1,
    cd_purchase_estimate, count(*) cnt2, cd_credit_rating, count(*) cnt3
 from
     dev.spectrum_iceberg_schema.store_returns.customer c,
     dev.spectrum_iceberg_schema.store_returns.customer_address ca,
     dev.spectrum_iceberg_schema.store_returns.customer_demographics
 where
    c.c_current_addr_sk = ca.ca_address_sk and
    ca_state in ('KY', 'GA', 'NM') and
    cd_demo_sk = c.c_current_cdemo_sk and
    exists (select * from dev.spectrum_iceberg_schema.store_returns.store_sales,
                          dev.spectrum_iceberg_schema.store_returns.date_dim
            where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2001 and
                d_moy between 4 and 4+2) and
   (not exists (select * from  dev.spectrum_iceberg_schema.store_returns.web_sales,
                               dev.spectrum_iceberg_schema.store_returns.date_dim
                where c.c_customer_sk = ws_bill_customer_sk and
                    ws_sold_date_sk = d_date_sk and
                    d_year = 2001 and
                    d_moy between 4 and 4+2) and
    not exists (select * from dev.spectrum_iceberg_schema.store_returns.catalog_sales,
                              dev.spectrum_iceberg_schema.store_returns.date_dim
                where c.c_customer_sk = cs_ship_customer_sk and
                    cs_sold_date_sk = d_date_sk and
                    d_year = 2001 and
                    d_moy between 4 and 4+2))
 group by cd_gender, cd_marital_status, cd_education_status,
          cd_purchase_estimate, cd_credit_rating
 order by cd_gender, cd_marital_status, cd_education_status,
          cd_purchase_estimate, cd_credit_rating
 limit 100
            
