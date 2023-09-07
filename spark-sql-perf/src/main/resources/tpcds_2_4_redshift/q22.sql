--q22.sql--

 select i_product_name, i_brand, i_class, i_category, avg(inv_quantity_on_hand) qoh
       from dev.spectrum_iceberg_schema.inventory, dev.spectrum_iceberg_schema.date_dim,  dev.spectrum_iceberg_schema.item, dev.spectrum_iceberg_schema.warehouse
       where inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and inv_warehouse_sk = w_warehouse_sk
              and d_month_seq between 1200 and 1200 + 11
       group by rollup(i_product_name, i_brand, i_class, i_category)
 order by qoh, i_product_name, i_brand, i_class, i_category
 limit 100
            
