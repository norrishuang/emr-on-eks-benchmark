--18.store_sales
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.store_sales PURGE;
create table {CATALOG}.{DATABASE}.store_sales
    USING iceberg
    PARTITIONED BY (ss_sold_date_sk)
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
    select * from tpcds.store_sales;