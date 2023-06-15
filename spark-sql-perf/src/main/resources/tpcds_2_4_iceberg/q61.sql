--q61.sql--

 select promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100
 from
   (select sum(ss_ext_sales_price) promotions
     from   glue_catalog.tpcds_iceberg.store_sales, glue_catalog.tpcds_iceberg.store, glue_catalog.tpcds_iceberg.promotion, glue_catalog.tpcds_iceberg.date_dim,  glue_catalog.tpcds_iceberg.customer, glue_catalog.tpcds_iceberg.customer_address, glue_catalog.tpcds_iceberg.item
     where ss_sold_date_sk = d_date_sk
     and   ss_store_sk = s_store_sk
     and   ss_promo_sk = p_promo_sk
     and   ss_customer_sk= c_customer_sk
     and   ca_address_sk = c_current_addr_sk
     and   ss_item_sk = i_item_sk
     and   ca_gmt_offset = -5
     and   i_category = 'Jewelry'
     and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
     and   s_gmt_offset = -5
     and   d_year = 1998
     and   d_moy  = 11) promotional_sales cross join
   (select sum(ss_ext_sales_price) total
     from   glue_catalog.tpcds_iceberg.store_sales, glue_catalog.tpcds_iceberg.store,  glue_catalog.tpcds_iceberg.date_dim,  glue_catalog.tpcds_iceberg.customer, glue_catalog.tpcds_iceberg.customer_address, glue_catalog.tpcds_iceberg.item
     where ss_sold_date_sk = d_date_sk
     and   ss_store_sk = s_store_sk
     and   ss_customer_sk= c_customer_sk
     and   ca_address_sk = c_current_addr_sk
     and   ss_item_sk = i_item_sk
     and   ca_gmt_offset = -5
     and   i_category = 'Jewelry'
     and   s_gmt_offset = -5
     and   d_year = 1998
     and   d_moy  = 11) all_sales
 order by promotions, total
 limit 100
            
