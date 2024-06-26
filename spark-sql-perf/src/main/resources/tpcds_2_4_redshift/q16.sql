--q16.sql--

select
    count(distinct cs_order_number) as order_count,
    sum(cs_ext_ship_cost) as total_shipping_cost,
    sum(cs_net_profit) as total_net_profit
from
    dev.{0}.catalog_sales cs1,
    dev.{0}.date_dim,
    dev.{0}.customer_address,
    dev.{0}.call_center
where
    d_date between cast ('2002-02-01' as date) and (cast('2002-02-01' as date) + interval '60' day)
  and cs1.cs_ship_date_sk = d_date_sk
  and cs1.cs_ship_addr_sk = ca_address_sk
  and ca_state = 'GA'
  and cs1.cs_call_center_sk = cc_call_center_sk
  and cc_county in ('Williamson County','Williamson County','Williamson County','Williamson County', 'Williamson County')
  and exists (select *
              from dev.{0}.catalog_sales cs2
              where cs1.cs_order_number = cs2.cs_order_number
                and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
  and not exists(select *
                 from dev.{0}.catalog_returns cr1
                 where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number)
limit 100
            
