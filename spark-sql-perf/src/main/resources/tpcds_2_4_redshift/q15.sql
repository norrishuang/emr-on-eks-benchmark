--q15.sql--

 select ca_zip, sum(cs_sales_price)
 from dev.{0}.catalog_sales,
      dev.{0}.customer,
      dev.{0}.customer_address,
      dev.{0}.date_dim
 where cs_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk
 	and ( substring(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
 	      or ca_state in ('CA','WA','GA')
 	      or cs_sales_price > 500)
 	and cs_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2001
 group by ca_zip
 order by ca_zip
 limit 100
            
