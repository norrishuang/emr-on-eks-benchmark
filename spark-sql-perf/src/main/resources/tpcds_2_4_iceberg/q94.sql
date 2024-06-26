--q94.sql--

 select
    count(distinct ws_order_number) as `order count`
   ,sum(ws_ext_ship_cost) as `total shipping cost`
   ,sum(ws_net_profit) as `total net profit`
 from
     glue_catalog.tpcds_iceberg.web_sales ws1,
     glue_catalog.tpcds_iceberg.date_dim,
     glue_catalog.tpcds_iceberg.customer_address,
     glue_catalog.tpcds_iceberg.web_site
 where
     d_date between cast('1999-02-01' as date) and
            (cast('1999-02-01' as date) + interval '60' day)
 and ws1.ws_ship_date_sk = d_date_sk
 and ws1.ws_ship_addr_sk = ca_address_sk
 and ca_state = 'IL'
 and ws1.ws_web_site_sk = web_site_sk
 and web_company_name = 'pri'
 and exists (select *
             from  glue_catalog.tpcds_iceberg.web_sales ws2
             where ws1.ws_order_number = ws2.ws_order_number
               and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
 and not exists(select *
                from glue_catalog.tpcds_iceberg.web_returns wr1
                where ws1.ws_order_number = wr1.wr_order_number)
 order by count(distinct ws_order_number)
 limit 100
            
