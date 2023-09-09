--q69.sql--

 select
    cd_gender, cd_marital_status, cd_education_status, count(*) cnt1,
    cd_purchase_estimate, count(*) cnt2, cd_credit_rating, count(*) cnt3
 from
     dev.{0}.customer c,
     dev.{0}.customer_address ca,
     dev.{0}.customer_demographics
 where
    c.c_current_addr_sk = ca.ca_address_sk and
    ca_state in ('KY', 'GA', 'NM') and
    cd_demo_sk = c.c_current_cdemo_sk and
    exists (select * from dev.{0}.store_sales,
                          dev.{0}.date_dim
            where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2001 and
                d_moy between 4 and 4+2) and
   (not exists (select * from  dev.{0}.web_sales,
                               dev.{0}.date_dim
                where c.c_customer_sk = ws_bill_customer_sk and
                    ws_sold_date_sk = d_date_sk and
                    d_year = 2001 and
                    d_moy between 4 and 4+2) and
    not exists (select * from dev.{0}.catalog_sales,
                              dev.{0}.date_dim
                where c.c_customer_sk = cs_ship_customer_sk and
                    cs_sold_date_sk = d_date_sk and
                    d_year = 2001 and
                    d_moy between 4 and 4+2))
 group by cd_gender, cd_marital_status, cd_education_status,
          cd_purchase_estimate, cd_credit_rating
 order by cd_gender, cd_marital_status, cd_education_status,
          cd_purchase_estimate, cd_credit_rating
 limit 100
            
