--q77.sql--

 WITH ss AS (
  SELECT s_store_sk, SUM(ss_ext_sales_price) AS sales, SUM(ss_net_profit) AS profit
  FROM dev.{0}.store_sales
  JOIN dev.{0}.date_dim ON ss_sold_date_sk = d_date_sk
  JOIN dev.{0}.store ON ss_store_sk = s_store_sk
  WHERE d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL '30' DAY)
  GROUP BY s_store_sk
),
sr AS (
  SELECT s_store_sk, SUM(sr_return_amt) AS return_amt, SUM(sr_net_loss) AS profit_loss
  FROM dev.{0}.store_returns
  JOIN dev.{0}.date_dim ON sr_returned_date_sk = d_date_sk
  JOIN dev.{0}.store ON sr_store_sk = s_store_sk
  WHERE d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL '30' DAY)
  GROUP BY s_store_sk
),
cs AS (
  SELECT cs_call_center_sk, SUM(cs_ext_sales_price) AS sales, SUM(cs_net_profit) AS profit
  FROM dev.{0}.catalog_sales
  JOIN dev.{0}.date_dim ON cs_sold_date_sk = d_date_sk
  WHERE d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL '30' DAY)
  GROUP BY cs_call_center_sk
),
cr AS (
  SELECT cr_call_center_sk, SUM(cr_return_amount) AS return_amt, SUM(cr_net_loss) AS profit_loss
  FROM dev.{0}.catalog_returns
  JOIN dev.{0}.date_dim ON cr_returned_date_sk = d_date_sk
  WHERE d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL '30' DAY)
  GROUP BY cr_call_center_sk
),
ws AS (
  SELECT wp_web_page_sk, SUM(ws_ext_sales_price) AS sales, SUM(ws_net_profit) AS profit
  FROM dev.{0}.web_sales
  JOIN dev.{0}.date_dim ON ws_sold_date_sk = d_date_sk
  JOIN dev.{0}.web_page ON ws_web_page_sk = wp_web_page_sk
  WHERE d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL '30' DAY)
  GROUP BY wp_web_page_sk
),
wr AS (
  SELECT wp_web_page_sk, SUM(wr_return_amt) AS return_amt, SUM(wr_net_loss) AS profit_loss
  FROM dev.{0}.web_returns
  JOIN dev.{0}.date_dim ON wr_returned_date_sk = d_date_sk
  JOIN dev.{0}.web_page ON wr_web_page_sk = wp_web_page_sk
  WHERE d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL '30' DAY)
  GROUP BY wp_web_page_sk
)
SELECT
  channel,
  id,
  SUM(sales) AS sales,
  SUM(return_amt) AS returns,
  SUM(profit) AS profit
FROM (
  SELECT
    'store channel' AS channel,
    ss.s_store_sk AS id,
    sales,
    COALESCE(sr.return_amt, 0) AS return_amt,
    (profit - COALESCE(sr.profit_loss, 0)) AS profit
  FROM ss
  LEFT JOIN sr ON ss.s_store_sk = sr.s_store_sk
  UNION ALL
  SELECT
    'catalog channel' AS channel,
    cs.cs_call_center_sk AS id,
    sales,
    cr.return_amt,
    (profit - cr.profit_loss) AS profit
  FROM cs
  CROSS JOIN cr
  UNION ALL
  SELECT
    'web channel' AS channel,
    ws.wp_web_page_sk AS id,
    sales,
    COALESCE(wr.return_amt, 0) AS return_amt,
    (profit - COALESCE(wr.profit_loss, 0)) AS profit
  FROM ws
  LEFT JOIN wr ON ws.wp_web_page_sk = wr.wp_web_page_sk
) x
GROUP BY ROLLUP(channel, id)
ORDER BY channel, id
LIMIT 100;
