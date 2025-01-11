--q22.sql--

 select i_product_name, i_brand, i_class, i_category, avg(inv_quantity_on_hand) qoh
       from {0}.{1}.inventory, {0}.{1}.date_dim,  {0}.{1}.item, {0}.{1}.warehouse
       where inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and inv_warehouse_sk = w_warehouse_sk
              and d_month_seq between 1200 and 1200 + 11
       group by rollup(i_product_name, i_brand, i_class, i_category)
 order by qoh, i_product_name, i_brand, i_class, i_category
 limit 100
            
