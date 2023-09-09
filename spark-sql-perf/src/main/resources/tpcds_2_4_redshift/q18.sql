--q18.sql--

 select i_item_id,
        ca_country,
        ca_state,
        ca_county,
        avg( cast(cs_quantity as decimal(12,2))) agg1,
        avg( cast(cs_list_price as decimal(12,2))) agg2,
        avg( cast(cs_coupon_amt as decimal(12,2))) agg3,
        avg( cast(cs_sales_price as decimal(12,2))) agg4,
        avg( cast(cs_net_profit as decimal(12,2))) agg5,
        avg( cast(c_birth_year as decimal(12,2))) agg6,
        avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7
 from dev.{0}.catalog_sales,
      dev.{0}.customer_demographics cd1,
      dev.{0}.customer_demographics cd2,
      dev.{0}.customer,
      dev.{0}.customer_address,
      dev.{0}.date_dim,
      dev.{0}.item
 where cs_sold_date_sk = d_date_sk and
       cs_item_sk = i_item_sk and
       cs_bill_cdemo_sk = cd1.cd_demo_sk and
       cs_bill_customer_sk = c_customer_sk and
       cd1.cd_gender = 'F' and
       cd1.cd_education_status = 'Unknown' and
       c_current_cdemo_sk = cd2.cd_demo_sk and
       c_current_addr_sk = ca_address_sk and
       c_birth_month in (1,6,8,9,12,2) and
       d_year = 1998 and
       ca_state  in ('MS','IN','ND','OK','NM','VA','MS')
 group by rollup (i_item_id, ca_country, ca_state, ca_county)
 order by ca_country, ca_state, ca_county, i_item_id
 LIMIT 100
            
