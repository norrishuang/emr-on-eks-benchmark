--q68.sql--

 select
    c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, extended_price,
    extended_tax, list_price
 from (select
        ss_ticket_number, ss_customer_sk, ca_city bought_city,
        sum(ss_ext_sales_price) extended_price,
        sum(ss_ext_list_price) list_price,
        sum(ss_ext_tax) extended_tax
     from  {0}.{1}.store_sales,
           {0}.{1}.date_dim,
           {0}.{1}.store,
           {0}.{1}.household_demographics,
           {0}.{1}.customer_address
     where store_sales.ss_sold_date_sk =  {0}.{1}.date_dim.d_date_sk
        and store_sales.ss_store_sk = store.s_store_sk
        and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        and store_sales.ss_addr_sk ={0}.{1}.customer_address.ca_address_sk
        and  {0}.{1}.date_dim.d_dom between 1 and 2
        and (household_demographics.hd_dep_count = 4 or
             household_demographics.hd_vehicle_count = 3)
        and  {0}.{1}.date_dim.d_year in (1999,1999+1,1999+2)
        and store.s_city in ('Midway','Fairview')
     group by ss_ticket_number, ss_customer_sk, ss_addr_sk,ca_city) dn,
     {0}.{1}.customer,
     {0}.{1}.customer_address current_addr
 where ss_customer_sk = c_customer_sk
   and customer.c_current_addr_sk = current_addr.ca_address_sk
   and current_addr.ca_city <> bought_city
 order by c_last_name, ss_ticket_number
 limit 100
            
