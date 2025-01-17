--q86.sql--

select sum(ws_net_paid) as total_sum, i_category, i_class,
  grouping(i_category)+grouping(i_class) as lochierarchy,
  rank() over (
 	    partition by grouping(i_category)+grouping(i_class),
 	    case when grouping(i_class) = 0 then i_category end
 	    order by sum(ws_net_paid) desc) as rank_within_parent
 from
     {0}.{1}.web_sales,
     {0}.{1}.date_dim d1,
     {0}.{1}.item
 where
    d1.d_month_seq between 1200 and 1200+11
 and d1.d_date_sk = ws_sold_date_sk
 and i_item_sk  = ws_item_sk
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc,
   case when grouping(i_category)+grouping(i_class) = 0 then i_category end,
   rank_within_parent
 limit 100
            
