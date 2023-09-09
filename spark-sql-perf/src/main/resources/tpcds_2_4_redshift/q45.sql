--q45.sql--

 select ca_zip, ca_city, sum(ws_sales_price)
 from  dev.{0}.web_sales,
       dev.{0}.customer,
       dev.{0}.customer_address,
       dev.{0}.date_dim,
       dev.{0}.item
 where ws_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk
 	and ws_item_sk = i_item_sk
 	and ( substring(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
 	      or
 	      i_item_id in (select i_item_id
                             from dev.{0}.item
                             where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
                             )
 	    )
 	and ws_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2001
 group by ca_zip, ca_city
 order by ca_zip, ca_city
 limit 100
            
