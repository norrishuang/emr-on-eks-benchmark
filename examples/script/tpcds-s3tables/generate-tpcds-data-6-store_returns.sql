--17.store_returns
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.store_returns PURGE;
create table {CATALOG}.{DATABASE}.store_returns
    USING iceberg
    PARTITIONED BY (sr_returned_date_sk)
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
    select * from tpcds.store_returns;