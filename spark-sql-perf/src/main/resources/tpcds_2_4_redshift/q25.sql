--q25.sql--

 select i_item_id, i_item_desc, s_store_id, s_store_name,
    sum(ss_net_profit) as store_sales_profit,
    sum(sr_net_loss) as store_returns_loss,
    sum(cs_net_profit) as catalog_sales_profit
 from
     {0}.{1}.store_sales, {0}.{1}.store_returns, {0}.{1}.catalog_sales, {0}.{1}.date_dim d1, {0}.{1}.date_dim d2, {0}.{1}.date_dim d3,
    {0}.{1}.store, {0}.{1}.item
 where
    d1.d_moy = 4
    and d1.d_year = 2001
    and d1.d_date_sk = ss_sold_date_sk
    and i_item_sk = ss_item_sk
    and s_store_sk = ss_store_sk
    and ss_customer_sk = sr_customer_sk
    and ss_item_sk = sr_item_sk
    and ss_ticket_number = sr_ticket_number
    and sr_returned_date_sk = d2.d_date_sk
    and d2.d_moy between 4 and 10
    and d2.d_year = 2001
    and sr_customer_sk = cs_bill_customer_sk
    and sr_item_sk = cs_item_sk
    and cs_sold_date_sk = d3.d_date_sk
    and d3.d_moy between 4 and 10
    and d3.d_year = 2001
 group by
    i_item_id, i_item_desc, s_store_id, s_store_name
 order by
    i_item_id, i_item_desc, s_store_id, s_store_name
 limit 100
            
