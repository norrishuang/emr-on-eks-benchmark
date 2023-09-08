--q6.sql--

SELECT state, cnt FROM (
 SELECT a.ca_state state, count(*) cnt
 FROM
     dev.%s.customer_address a,
     dev.%s.customer c,
     dev.%s.store_sales s,
     dev.%s.date_dim d,
     dev.%s.item i
 WHERE a.ca_address_sk = c.c_current_addr_sk
 	AND c.c_customer_sk = s.ss_customer_sk
 	AND s.ss_sold_date_sk = d.d_date_sk
 	AND s.ss_item_sk = i.i_item_sk
 	AND d.d_month_seq =
 	     (SELECT distinct (d_month_seq) FROM dev.%s.date_dim
        WHERE d_year = 2001 AND d_moy = 1)
 	AND i.i_current_price > 1.2 *
             (SELECT avg(j.i_current_price) FROM dev.%s.item j
 	            WHERE j.i_category = i.i_category)
 GROUP BY a.ca_state
) x
WHERE cnt >= 10
ORDER BY cnt LIMIT 100
            
