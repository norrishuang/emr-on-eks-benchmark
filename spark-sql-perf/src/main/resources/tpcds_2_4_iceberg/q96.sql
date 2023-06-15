--q96.sql--

 select count(*)
 from  glue_catalog.tpcds_iceberg.store_sales,
       glue_catalog.tpcds_iceberg.household_demographics,
       glue_catalog.tpcds_iceberg.time_dim,
       glue_catalog.tpcds_iceberg.store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 20
     and time_dim.t_minute >= 30
     and household_demographics.hd_dep_count = 7
     and store.s_store_name = 'ese'
 order by count(*)
 limit 100
            
