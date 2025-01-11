--22.web_returns
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.web_returns PURGE;
create table {CATALOG}.{DATABASE}.web_returns
    USING iceberg
    PARTITIONED BY (wr_returned_date_sk)
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
    select * from tpcds.web_returns;
