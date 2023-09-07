--q96.sql--

 select count(*)
 from  dev.spectrum_iceberg_schema.store_returns.store_sales,
       dev.spectrum_iceberg_schema.store_returns.household_demographics b1,
       dev.spectrum_iceberg_schema.store_returns.time_dim c1,
       dev.spectrum_iceberg_schema.store_returns.store d1
 where ss_sold_time_sk = c1.t_time_sk
     and ss_hdemo_sk = b1.hd_demo_sk
     and ss_store_sk = s_store_sk
     and c1.t_hour = 20
     and c1.t_minute >= 30
     and b1.hd_dep_count = 7
     and d1.s_store_name = 'ese'
 order by count(*)
 limit 100
            
