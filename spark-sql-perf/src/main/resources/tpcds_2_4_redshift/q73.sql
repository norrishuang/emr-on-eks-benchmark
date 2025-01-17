--q73.sql--

 select
    c_last_name, c_first_name, c_salutation, c_preferred_cust_flag,
    ss_ticket_number, cnt from
   (select ss_ticket_number, ss_customer_sk, count(*) cnt
    from  {0}.{1}.store_sales a,
          {0}.{1}.date_dim b,
          {0}.{1}.store c,
          {0}.{1}.household_demographics d
    where a.ss_sold_date_sk =  b.d_date_sk
    and a.ss_store_sk = c.s_store_sk
    and a.ss_hdemo_sk = d.hd_demo_sk
    and  b.d_dom between 1 and 2
    and (d.hd_buy_potential = '>10000' or
         d.hd_buy_potential = 'unknown')
    and d.hd_vehicle_count > 0
    and case when d.hd_vehicle_count > 0 then
             d.hd_dep_count/ d.hd_vehicle_count else null end > 1
    and b.d_year in (1999,1999+1,1999+2)
    and c.s_county in ('Williamson County','Franklin Parish','Bronx County','Orange County')
    group by ss_ticket_number,ss_customer_sk) dj, {0}.{1}.customer
    where ss_customer_sk = c_customer_sk
      and cnt between 1 and 5
    order by cnt desc, c_last_name asc

