--q62.sql--

select
    substr(w_warehouse_name,1,20)
     ,sm_type
     ,web_name
     ,sum(case when (ws_ship_date_sk - cast(ws_sold_date_sk as int) <= 30 ) then 1 else 0 end)  as days_30
     ,sum(case when (ws_ship_date_sk - cast(ws_sold_date_sk as int) > 30) and
                    (ws_ship_date_sk - cast(ws_sold_date_sk as int) <= 60) then 1 else 0 end )  as days_31_60
     ,sum(case when (ws_ship_date_sk - cast(ws_sold_date_sk as int) > 60) and
                    (ws_ship_date_sk - cast(ws_sold_date_sk as int) <= 90) then 1 else 0 end)  as days_61_90
     ,sum(case when (ws_ship_date_sk - cast(ws_sold_date_sk as int) > 90) and
                    (ws_ship_date_sk - cast(ws_sold_date_sk as int) <= 120) then 1 else 0 end)  as days_91_120
     ,sum(case when (ws_ship_date_sk - cast(ws_sold_date_sk as int)  > 120) then 1 else 0 end)  as greate_than_120_days
from
    web_sales, warehouse, ship_mode, web_site, date_dim
where
    d_month_seq between 1200 and 1200 + 11
  and ws_ship_date_sk   = d_date_sk
  and ws_warehouse_sk   = w_warehouse_sk
  and ws_ship_mode_sk   = sm_ship_mode_sk
  and ws_web_site_sk    = web_site_sk
group by
    substr(w_warehouse_name,1,20), sm_type, web_name
order by
    substr(w_warehouse_name,1,20), sm_type, web_name
limit 100
            
