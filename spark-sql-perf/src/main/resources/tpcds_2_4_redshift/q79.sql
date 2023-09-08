--q79.sql--

 select
  c_last_name,c_first_name,substring(s_city,1,30),ss_ticket_number,amt,profit
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,c.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from  dev.%s.store_sales a,
          dev.%s.date_dim b,
          dev.%s.store c,
          dev.%s.household_demographics d
    where a.ss_sold_date_sk =  b.d_date_sk
    and a.ss_store_sk = c.s_store_sk
    and a.ss_hdemo_sk = d.hd_demo_sk
    and (d.hd_dep_count = 6 or
        d.hd_vehicle_count > 2)
    and b.d_dow = 1
    and b.d_year in (1999,1999+1,1999+2)
    and c.s_number_employees between 200 and 295
    group by ss_ticket_number, ss_customer_sk, ss_addr_sk, c.s_city) ms, dev.%s.customer
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,substring(s_city,1,30), profit
 limit 100
            
