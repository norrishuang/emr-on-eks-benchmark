--q88.sql--

 select  *
 from
   (select count(*) h8_30_to_9
    from  {0}.{1}.store_sales a,
          {0}.{1}.household_demographics b,
          {0}.{1}.time_dim c,
          {0}.{1}.store d
    where ss_sold_time_sk = c.t_time_sk
     and ss_hdemo_sk = b.hd_demo_sk
     and ss_store_sk = s_store_sk
     and  c.t_hour = 8
     and  c.t_minute >= 30
     and ((b.hd_dep_count = 4 and b.hd_vehicle_count<=4+2) or
          (b.hd_dep_count = 2 and b.hd_vehicle_count<=2+2) or
          (b.hd_dep_count = 0 and b.hd_vehicle_count<=0+2))
     and d.s_store_name = 'ese') s1 cross join
   (select count(*) h9_to_9_30
    from  {0}.{1}.store_sales a,
          {0}.{1}.household_demographics b,
          {0}.{1}.time_dim c,
          {0}.{1}.store d
    where ss_sold_time_sk = c.t_time_sk
      and ss_hdemo_sk = b.hd_demo_sk
      and ss_store_sk = s_store_sk
      and c.t_hour = 9
      and c.t_minute < 30
      and ((b.hd_dep_count = 4 and b.hd_vehicle_count<=4+2) or
          (b.hd_dep_count = 2 and b.hd_vehicle_count<=2+2) or
          (b.hd_dep_count = 0 and b.hd_vehicle_count<=0+2))
      and d.s_store_name = 'ese') s2 cross join
 (select count(*) h9_30_to_10
 from  {0}.{1}.store_sales a1,
       {0}.{1}.household_demographics b1,
       {0}.{1}.time_dim c1,
       {0}.{1}.store d1
 where ss_sold_time_sk = c1.t_time_sk
     and ss_hdemo_sk = b1.hd_demo_sk
     and ss_store_sk = s_store_sk
     and c1.t_hour = 9
     and c1.t_minute >= 30
     and ((b1.hd_dep_count = 4 and b1.hd_vehicle_count<=4+2) or
          (b1.hd_dep_count = 2 and b1.hd_vehicle_count<=2+2) or
          (b1.hd_dep_count = 0 and b1.hd_vehicle_count<=0+2))
     and d1.s_store_name = 'ese') s3 cross join
 (select count(*) h10_to_10_30
 from  {0}.{1}.store_sales a2,
       {0}.{1}.household_demographics b2,
       {0}.{1}.time_dim c2,
       {0}.{1}.store d2
 where ss_sold_time_sk = c2.t_time_sk
     and ss_hdemo_sk = b2.hd_demo_sk
     and ss_store_sk = s_store_sk
     and c2.t_hour = 10
     and c2.t_minute < 30
     and ((b2.hd_dep_count = 4 and b2.hd_vehicle_count<=4+2) or
          (b2.hd_dep_count = 2 and b2.hd_vehicle_count<=2+2) or
          (b2.hd_dep_count = 0 and b2.hd_vehicle_count<=0+2))
     and d2.s_store_name = 'ese') s4 cross join
 (select count(*) h10_30_to_11
 from  {0}.{1}.store_sales a3,
       {0}.{1}.household_demographics b3,
       {0}.{1}.time_dim c3,
       {0}.{1}.store d3
 where ss_sold_time_sk = c3.t_time_sk
     and ss_hdemo_sk = b3.hd_demo_sk
     and ss_store_sk = s_store_sk
     and c3.t_hour = 10
     and c3.t_minute >= 30
     and ((b3.hd_dep_count = 4 and b3.hd_vehicle_count<=4+2) or
          (b3.hd_dep_count = 2 and b3.hd_vehicle_count<=2+2) or
          (b3.hd_dep_count = 0 and b3.hd_vehicle_count<=0+2))
     and d3.s_store_name = 'ese') s5 cross join
 (select count(*) h11_to_11_30
 from  {0}.{1}.store_sales a4,
       {0}.{1}.household_demographics b4,
       {0}.{1}.time_dim c4,
       {0}.{1}.store d4
 where ss_sold_time_sk = c4.t_time_sk
     and ss_hdemo_sk = b4.hd_demo_sk
     and ss_store_sk = s_store_sk
     and c4.t_hour = 11
     and c4.t_minute < 30
     and ((b4.hd_dep_count = 4 and b4.hd_vehicle_count<=4+2) or
          (b4.hd_dep_count = 2 and b4.hd_vehicle_count<=2+2) or
          (b4.hd_dep_count = 0 and b4.hd_vehicle_count<=0+2))
     and d4.s_store_name = 'ese') s6 cross join
 (select count(*) h11_30_to_12
 from  {0}.{1}.store_sales a5,
       {0}.{1}.household_demographics b5,
       {0}.{1}.time_dim c5,
       {0}.{1}.store d5
 where ss_sold_time_sk = c5.t_time_sk
     and ss_hdemo_sk = b5.hd_demo_sk
     and ss_store_sk = s_store_sk
     and c5.t_hour = 11
     and c5.t_minute >= 30
     and ((b5.hd_dep_count = 4 and b5.hd_vehicle_count<=4+2) or
          (b5.hd_dep_count = 2 and b5.hd_vehicle_count<=2+2) or
          (b5.hd_dep_count = 0 and b5.hd_vehicle_count<=0+2))
     and d5.s_store_name = 'ese') s7 cross join
 (select count(*) h12_to_12_30
 from  {0}.{1}.store_sales a6,
       {0}.{1}.household_demographics b6,
       {0}.{1}.time_dim c6,
       {0}.{1}.store  d6
 where ss_sold_time_sk = c6.t_time_sk
     and ss_hdemo_sk = b6.hd_demo_sk
     and ss_store_sk = s_store_sk
     and c6.t_hour = 12
     and c6.t_minute < 30
     and ((b6.hd_dep_count = 4 and b6.hd_vehicle_count<=4+2) or
          (b6.hd_dep_count = 2 and b6.hd_vehicle_count<=2+2) or
          (b6.hd_dep_count = 0 and b6.hd_vehicle_count<=0+2))
     and d6.s_store_name = 'ese') s8
            
