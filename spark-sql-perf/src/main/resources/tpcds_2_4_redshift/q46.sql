--q46.sql--

 select c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, amt,profit
 from
   (select ss_ticket_number
          ,ss_customer_sk
          ,ca_city bought_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from  {0}.{1}.store_sales a,
          {0}.{1}.date_dim b,
          {0}.{1}.store c,
          {0}.{1}.household_demographics d,
          {0}.{1}.customer_address e
    where a.ss_sold_date_sk = b.d_date_sk
    and a.ss_store_sk = c.s_store_sk
    and a.ss_hdemo_sk = d.hd_demo_sk
    and a.ss_addr_sk = e.ca_address_sk
    and (d.hd_dep_count = 4 or
         d.hd_vehicle_count= 3)
    and b.d_dow in (6,0)
    and b.d_year in (1999,1999+1,1999+2)
    and c.s_city in ('Fairview','Midway','Fairview','Fairview','Fairview')
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn, {0}.{1}.customer, {0}.{1}.customer_address current_addr
    where ss_customer_sk = c_customer_sk
      and {0}.{1}.customer.c_current_addr_sk = current_addr.ca_address_sk
      and current_addr.ca_city <> bought_city
  order by c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number
  limit 100
            
