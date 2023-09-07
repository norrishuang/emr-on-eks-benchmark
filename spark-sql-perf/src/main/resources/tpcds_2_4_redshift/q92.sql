--q92.sql--

 select sum(ws_ext_discount_amt) as `Excess Discount Amount`
 from  dev.spectrum_iceberg_schema.store_returns.web_sales,
       dev.spectrum_iceberg_schema.store_returns.item,
       dev.spectrum_iceberg_schema.store_returns.date_dim
 where i_manufact_id = 350
 and i_item_sk = ws_item_sk
 and d_date between cast ('2000-01-27' as date) and (cast('2000-01-27' as date) + interval '90' day)
 and d_date_sk = ws_sold_date_sk
 and ws_ext_discount_amt >
     (
       SELECT 1.3 * avg(ws_ext_discount_amt)
       FROM  dev.spectrum_iceberg_schema.store_returns.web_sales,
             dev.spectrum_iceberg_schema.store_returns.date_dim
       WHERE ws_item_sk = i_item_sk
         and d_date between cast ('2000-01-27' as date) and (cast('2000-01-27' as date) + interval '90' day)
         and d_date_sk = ws_sold_date_sk
     )
 order by sum(ws_ext_discount_amt)
 limit 100
            
