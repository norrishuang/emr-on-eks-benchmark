--q38.sql--

 select count(*) from (
    select distinct c_last_name, c_first_name, d_date
    from  dev.spectrum_iceberg_schema.store_returns.store_sales a1,
          dev.spectrum_iceberg_schema.store_returns.date_dim b1,
          dev.spectrum_iceberg_schema.store_returns.customer c1
          where a1.ss_sold_date_sk = b1.d_date_sk
      and a1.ss_customer_sk = c1.c_customer_sk
      and d_month_seq between 1200 and  1200 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from dev.spectrum_iceberg_schema.store_returns.catalog_sales a2,
         dev.spectrum_iceberg_schema.store_returns.date_dim b2,
         dev.spectrum_iceberg_schema.store_returns.customer c2
          where a2.cs_sold_date_sk = b2.d_date_sk
      and a2.cs_bill_customer_sk = c2.c_customer_sk
      and d_month_seq between  1200 and  1200 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from  dev.spectrum_iceberg_schema.store_returns.web_sales a3,
          dev.spectrum_iceberg_schema.store_returns.date_dim b3,
          dev.spectrum_iceberg_schema.store_returns.customer c3
          where  a3.ws_sold_date_sk = b3.d_date_sk
      and a3.ws_bill_customer_sk = c3.c_customer_sk
      and d_month_seq between  1200 and  1200 + 11
 ) hot_cust
 limit 100
            
