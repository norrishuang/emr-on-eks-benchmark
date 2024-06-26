--q38.sql--

 select count(*) from (
    select distinct c_last_name, c_first_name, d_date
    from  glue_catalog.tpcds_iceberg.store_sales a1,
          glue_catalog.tpcds_iceberg.date_dim b1,
          glue_catalog.tpcds_iceberg.customer c1
          where a1.ss_sold_date_sk = b1.d_date_sk
      and a1.ss_customer_sk = c1.c_customer_sk
      and d_month_seq between 1200 and  1200 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from glue_catalog.tpcds_iceberg.catalog_sales a2,
         glue_catalog.tpcds_iceberg.date_dim b2,
         glue_catalog.tpcds_iceberg.customer c2
          where a2.cs_sold_date_sk = b2.d_date_sk
      and a2.cs_bill_customer_sk = c2.c_customer_sk
      and d_month_seq between  1200 and  1200 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from  glue_catalog.tpcds_iceberg.web_sales a3,
          glue_catalog.tpcds_iceberg.date_dim b3,
          glue_catalog.tpcds_iceberg.customer c3
          where  a3.ws_sold_date_sk = b3.d_date_sk
      and a3.ws_bill_customer_sk = c3.c_customer_sk
      and d_month_seq between  1200 and  1200 + 11
 ) hot_cust
 limit 100
            
