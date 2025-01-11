--q36.sql--

select
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin,
    i_category,
    i_class,
    grouping(i_category) + grouping(i_class) as lochierarchy,
    rank() over (
        partition by grouping(i_category) + grouping(i_class),
        case when grouping(i_class) = 0 then i_category end
        order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc
    ) as rank_within_parent
from
    {0}.{1}.store_sales,
    {0}.{1}.date_dim d1,
    {0}.{1}.item,
    {0}.{1}.store
where
    d1.d_year = 2001
    and d1.d_date_sk = ss_sold_date_sk
    and i_item_sk = ss_item_sk
    and s_store_sk = ss_store_sk
    and s_state in ('TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN')
group by rollup(i_category, i_class)
order by
    case when grouping(i_category) + grouping(i_class) = 0 then i_category end,
    rank_within_parent,
    grouping(i_category) + grouping(i_class) desc
limit 100;
