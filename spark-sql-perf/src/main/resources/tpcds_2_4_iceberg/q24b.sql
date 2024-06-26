--q24b.sql--

 with ssales as
 (select c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color,
         i_current_price, i_manager_id, i_units, i_size, sum(ss_net_paid) netpaid
 from  glue_catalog.tpcds_iceberg.store_sales, glue_catalog.tpcds_iceberg.store_returns, glue_catalog.tpcds_iceberg.store,  glue_catalog.tpcds_iceberg.item,  glue_catalog.tpcds_iceberg.customer, glue_catalog.tpcds_iceberg.customer_address
 where ss_ticket_number = sr_ticket_number
   and ss_item_sk = sr_item_sk
   and ss_customer_sk = c_customer_sk
   and ss_item_sk = i_item_sk
   and ss_store_sk = s_store_sk
   and c_birth_country = upper(ca_country)
   and s_zip = ca_zip
   and s_market_id = 8
 group by c_last_name, c_first_name, s_store_name, ca_state, s_state,
          i_color, i_current_price, i_manager_id, i_units, i_size)
 select c_last_name, c_first_name, s_store_name, sum(netpaid) paid
 from ssales
 where i_color = 'chiffon'
 group by c_last_name, c_first_name, s_store_name
 having sum(netpaid) > (select 0.05*avg(netpaid) from ssales)
            
