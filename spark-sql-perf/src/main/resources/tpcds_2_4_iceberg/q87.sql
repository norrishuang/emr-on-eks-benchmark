--q87.sql--

 select count(*)
 from ((select distinct c_last_name, c_first_name, d_date
       from  glue_catalog.tpcds_iceberg.store_sales,  glue_catalog.tpcds_iceberg.date_dim, glue_catalog.tpcds_iceberg.customer
       where store_sales.ss_sold_date_sk =  glue_catalog.tpcds_iceberg.date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1200 and 1200+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from glue_catalog.tpcds_iceberg.catalog_sales,  glue_catalog.tpcds_iceberg.date_dim, glue_catalog.tpcds_iceberg.customer
       where catalog_sales.cs_sold_date_sk =  glue_catalog.tpcds_iceberg.date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1200 and 1200+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from  glue_catalog.tpcds_iceberg.web_sales,  glue_catalog.tpcds_iceberg.date_dim, customer
       where  glue_catalog.tpcds_iceberg.web_sales.ws_sold_date_sk =  glue_catalog.tpcds_iceberg.date_dim.d_date_sk
         and  glue_catalog.tpcds_iceberg.web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1200 and 1200+11)
) cool_cust
            
