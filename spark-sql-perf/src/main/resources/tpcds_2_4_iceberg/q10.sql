--q10.sql--

 select
  cd_gender, cd_marital_status, cd_education_status, count(*) cnt1,
  cd_purchase_estimate, count(*) cnt2, cd_credit_rating, count(*) cnt3,
  cd_dep_count, count(*) cnt4, cd_dep_employed_count,  count(*) cnt5,
  cd_dep_college_count, count(*) cnt6
 from
     glue_catalog.tpcds_iceberg.customer c, glue_catalog.tpcds_iceberg.glue_catalog.tpcds_iceberg.customer_address ca, glue_catalog.tpcds_iceberg.customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_county in ('Rush County','Toole County','Jefferson County',
                'Dona Ana County','La Porte County') and
  cd_demo_sk = c.c_current_cdemo_sk AND
  exists (select * from glue_catalog.tpcds_iceberg.store_sales, glue_catalog.tpcds_iceberg.date_dim
          where c.c_customer_sk = ss_customer_sk AND
                ss_sold_date_sk = d_date_sk AND
                d_year = 2002 AND
                d_moy between 1 AND 1+3) AND
   (exists (select * from  glue_catalog.tpcds_iceberg.web_sales, glue_catalog.tpcds_iceberg.date_dim
            where c.c_customer_sk = ws_bill_customer_sk AND
                  ws_sold_date_sk = d_date_sk AND
                  d_year = 2002 AND
                  d_moy between 1 AND 1+3) or
    exists (select * from glue_catalog.tpcds_iceberg.catalog_sales, glue_catalog.tpcds_iceberg.date_dim
            where c.c_customer_sk = cs_ship_customer_sk AND
                  cs_sold_date_sk = d_date_sk AND
                  d_year = 2002 AND
                  d_moy between 1 AND 1+3))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
LIMIT 100
            
