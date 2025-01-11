--23.web_sales
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.web_sales PURGE;
create table {CATALOG}.{DATABASE}.web_sales
    USING iceberg
    PARTITIONED BY (ws_sold_date_sk)
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
    select * from tpcds.web_sales;
