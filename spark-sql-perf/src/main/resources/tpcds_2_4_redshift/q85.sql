--q85.sql--

 select
    substring(r_reason_desc,1,20), avg(ws_quantity), avg(wr_refunded_cash), avg(wr_fee)
 from  {0}.{1}.web_sales,
       {0}.{1}.web_returns,
       {0}.{1}.web_page,
       {0}.{1}.customer_demographics cd1,
       {0}.{1}.customer_demographics cd2,
       {0}.{1}.customer_address,
       {0}.{1}.date_dim,
       {0}.{1}.reason
 where ws_web_page_sk = wp_web_page_sk
   and ws_item_sk = wr_item_sk
   and ws_order_number = wr_order_number
   and ws_sold_date_sk = d_date_sk and d_year = 2000
   and cd1.cd_demo_sk = wr_refunded_cdemo_sk
   and cd2.cd_demo_sk = wr_returning_cdemo_sk
   and ca_address_sk = wr_refunded_addr_sk
   and r_reason_sk = wr_reason_sk
   and
   (
    (
     cd1.cd_marital_status = 'M'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Advanced Degree'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 100.00 and 150.00
    )
   or
    (
     cd1.cd_marital_status = 'S'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'College'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 50.00 and 100.00
    )
   or
    (
     cd1.cd_marital_status = 'W'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = '2 yr Degree'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 150.00 and 200.00
    )
   )
   and
   (
    (
     ca_country = 'United States'
     and
     ca_state in ('IN', 'OH', 'NJ')
     and ws_net_profit between 100 and 200
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('WI', 'CT', 'KY')
     and ws_net_profit between 150 and 300
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('LA', 'IA', 'AR')
     and ws_net_profit between 50 and 250
    )
   )
 group by r_reason_desc
 order by substring(r_reason_desc,1,20)
        ,avg(ws_quantity)
        ,avg(wr_refunded_cash)
        ,avg(wr_fee)
 limit 100
            
