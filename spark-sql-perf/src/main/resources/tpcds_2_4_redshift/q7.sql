--q7.sql--

 SELECT i_item_id,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 FROM {0}.{1}.store_sales,
      {0}.{1}.customer_demographics,
      {0}.{1}.date_dim,
      {0}.{1}.item,
      {0}.{1}.promotion
 WHERE ss_sold_date_sk = d_date_sk AND
       ss_item_sk = i_item_sk AND
       ss_cdemo_sk = cd_demo_sk AND
       ss_promo_sk = p_promo_sk AND
       cd_gender = 'M' AND
       cd_marital_status = 'S' AND
       cd_education_status = 'College' AND
       (p_channel_email = 'N' or p_channel_event = 'N') AND
       d_year = 2000
 GROUP BY i_item_id
 ORDER BY i_item_id LIMIT 100
            
