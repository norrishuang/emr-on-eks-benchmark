--q90.sql--

 select cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
 from ( select count(*) amc
       from  dev.{0}.web_sales a1,
             dev.{0}.household_demographics b1,
             dev.{0}.time_dim c1,
             dev.{0}.web_page d1
       where ws_sold_time_sk = c1.t_time_sk
         and ws_ship_hdemo_sk = b1.hd_demo_sk
         and ws_web_page_sk = d1.wp_web_page_sk
         and c1.t_hour between 8 and 8+1
         and b1.hd_dep_count = 6
         and d1.wp_char_count between 5000 and 5200) at cross join
      ( select count(*) pmc
       from  dev.{0}.web_sales a2,
             dev.{0}.household_demographics b2,
             dev.{0}.time_dim c2,
             dev.{0}.web_page d2
       where ws_sold_time_sk = c2.t_time_sk
         and ws_ship_hdemo_sk = b2.hd_demo_sk
         and ws_web_page_sk = d2.wp_web_page_sk
         and c2.t_hour between 19 and 19+1
         and b2.hd_dep_count = 6
         and d2.wp_char_count between 5000 and 5200) pt
 order by am_pm_ratio
 limit 100
            
