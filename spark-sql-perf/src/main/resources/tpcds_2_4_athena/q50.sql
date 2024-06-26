--q50.sql--

select
    s_store_name, s_company_id, s_street_number, s_street_name, s_street_type,
    s_suite_number, s_city, s_county, s_state, s_zip
     ,sum(case when (sr_returned_date_sk - ss_sold_date_sk <= 30 ) then 1 else 0 end)  as days_30
     ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 30) and
                    (sr_returned_date_sk - ss_sold_date_sk <= 60) then 1 else 0 end )  as days_31_60
     ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 60) and
                    (sr_returned_date_sk - ss_sold_date_sk <= 90) then 1 else 0 end)  as days_61_90
     ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 90) and
                    (sr_returned_date_sk - ss_sold_date_sk <= 120) then 1 else 0 end)  as days_91_120
     ,sum(case when (sr_returned_date_sk - ss_sold_date_sk  > 120) then 1 else 0 end)  as greater_than_120
from
    store_sales, store_returns, store, date_dim d1, date_dim d2
where
        d2.d_year = 2001
  and d2.d_moy  = 8
  and ss_ticket_number = sr_ticket_number
  and ss_item_sk = sr_item_sk
  and ss_sold_date_sk   = d1.d_date_sk
  and sr_returned_date_sk   = d2.d_date_sk
  and ss_customer_sk = sr_customer_sk
  and ss_store_sk = s_store_sk
group by
    s_store_name, s_company_id, s_street_number, s_street_name, s_street_type,
    s_suite_number, s_city, s_county, s_state, s_zip
order by
    s_store_name, s_company_id, s_street_number, s_street_name, s_street_type,
    s_suite_number, s_city, s_county, s_state, s_zip
limit 100
            
