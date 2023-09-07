--q93.sql--

 select ss_customer_sk, sum(act_sales) sumsales
 from (select
         ss_item_sk, ss_ticket_number, ss_customer_sk,
         case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                  else (ss_quantity*ss_sales_price) end act_sales
       from dev.spectrum_iceberg_schema.store_sales
       left outer join dev.spectrum_iceberg_schema.store_returns
       on (sr_item_sk = ss_item_sk and sr_ticket_number = ss_ticket_number),
            dev.spectrum_iceberg_schema.reason
       where sr_reason_sk = r_reason_sk and r_reason_desc = 'reason 28') t
 group by ss_customer_sk
 order by sumsales, ss_customer_sk
 limit 100
            
