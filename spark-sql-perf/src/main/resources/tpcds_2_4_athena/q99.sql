--q99.sql--

select
    substr(w_warehouse_name,1,20), sm_type, cc_name
     ,sum(case when (cs_ship_date_sk - cast(cs_sold_date_sk as integer) <= 30 ) then 1 else 0 end)  as days_30
     ,sum(case when (cs_ship_date_sk - cast(cs_sold_date_sk as integer) > 30) and
                    (cs_ship_date_sk - cast(cs_sold_date_sk as integer) <= 60) then 1 else 0 end )  as days_31_60
     ,sum(case when (cs_ship_date_sk - cast(cs_sold_date_sk as integer) > 60) and
                    (cs_ship_date_sk - cast(cs_sold_date_sk as integer) <= 90) then 1 else 0 end)  as days_61_90
     ,sum(case when (cs_ship_date_sk - cast(cs_sold_date_sk as integer) > 90) and
                    (cs_ship_date_sk - cast(cs_sold_date_sk as integer) <= 120) then 1 else 0 end)  as days_91_120
     ,sum(case when (cs_ship_date_sk - cast(cs_sold_date_sk as integer)  > 120) then 1 else 0 end)  as great_than_120_days
from
    catalog_sales, warehouse, ship_mode, call_center, date_dim
where
    d_month_seq between 1200 and 1200 + 11
  and cs_ship_date_sk   =  d_date_sk
  and cs_warehouse_sk   = w_warehouse_sk
  and cs_ship_mode_sk   = sm_ship_mode_sk
  and cs_call_center_sk = cc_call_center_sk
group by
    substr(w_warehouse_name,1,20), sm_type, cc_name
order by substr(w_warehouse_name,1,20), sm_type, cc_name
limit 100
            
