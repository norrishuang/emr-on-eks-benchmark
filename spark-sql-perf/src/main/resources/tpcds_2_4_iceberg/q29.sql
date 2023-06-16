--q29.sql--

 select
     i_item_id
    ,i_item_desc
    ,s_store_id
    ,s_store_name
    ,sum(ss_quantity)        as store_sales_quantity
    ,sum(sr_return_quantity) as store_returns_quantity
    ,sum(cs_quantity)        as catalog_sales_quantity
 from
     glue_catalog.tpcds_iceberg.store_sales,
     glue_catalog.tpcds_iceberg.store_returns,
     glue_catalog.tpcds_iceberg.catalog_sales,
     glue_catalog.tpcds_iceberg.date_dim d1,
     glue_catalog.tpcds_iceberg.date_dim d2,
     glue_catalog.tpcds_iceberg.date_dim d3,
     glue_catalog.tpcds_iceberg.store,
     glue_catalog.tpcds_iceberg.item
 where
     d1.d_moy               = 9
 and d1.d_year              = 1999
 and d1.d_date_sk           = ss_sold_date_sk
 and i_item_sk              = ss_item_sk
 and s_store_sk             = ss_store_sk
 and ss_customer_sk         = sr_customer_sk
 and ss_item_sk             = sr_item_sk
 and ss_ticket_number       = sr_ticket_number
 and sr_returned_date_sk    = d2.d_date_sk
 and d2.d_moy               between 9 and  9 + 3
 and d2.d_year              = 1999
 and sr_customer_sk         = cs_bill_customer_sk
 and sr_item_sk             = cs_item_sk
 and cs_sold_date_sk        = d3.d_date_sk
 and d3.d_year              in (1999,1999+1,1999+2)
 group by
    i_item_id, i_item_desc, s_store_id, s_store_name
 order by
    i_item_id, i_item_desc, s_store_id, s_store_name
 limit 100
            
