--q72.sql--

 select i_item_desc
       ,w_warehouse_name
       ,d1.d_week_seq
       ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
       ,sum(case when p_promo_sk is not null then 1 else 0 end) promo
       ,count(*) total_cnt
 from dev.spectrum_iceberg_schema.catalog_sales
 join dev.spectrum_iceberg_schema.inventory on (cs_item_sk = inv_item_sk)
 join dev.spectrum_iceberg_schema.warehouse on (w_warehouse_sk=inv_warehouse_sk)
 join dev.spectrum_iceberg_schema.item on (i_item_sk = cs_item_sk)
 join dev.spectrum_iceberg_schema.customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
 join dev.spectrum_iceberg_schema.household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
 join dev.spectrum_iceberg_schema.date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
 join dev.spectrum_iceberg_schema.date_dim d2 on (inv_date_sk = d2.d_date_sk)
 join dev.spectrum_iceberg_schema.date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
 left outer join dev.spectrum_iceberg_schema.promotion on (cs_promo_sk=p_promo_sk)
 left outer join dev.spectrum_iceberg_schema.catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
 where d1.d_week_seq = d2.d_week_seq
   and inv_quantity_on_hand < cs_quantity
   and d3.d_date > (cast(d1.d_date AS DATE) + interval '5' day)
   and hd_buy_potential = '>10000'
   and d1.d_year = 1999
   and cd_marital_status = 'D'
 group by i_item_desc,w_warehouse_name,d1.d_week_seq
 order by total_cnt desc, i_item_desc, w_warehouse_name, d1.d_week_seq
 limit 100
            