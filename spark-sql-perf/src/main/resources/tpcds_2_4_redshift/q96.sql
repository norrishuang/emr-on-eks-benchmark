--q96.sql--

 select count(*)
 from  dev.%s.store_sales,
       dev.%s.household_demographics b1,
       dev.%s.time_dim c1,
       dev.%s.store d1
 where ss_sold_time_sk = c1.t_time_sk
     and ss_hdemo_sk = b1.hd_demo_sk
     and ss_store_sk = s_store_sk
     and c1.t_hour = 20
     and c1.t_minute >= 30
     and b1.hd_dep_count = 7
     and d1.s_store_name = 'ese'
 order by count(*)
 limit 100
            
