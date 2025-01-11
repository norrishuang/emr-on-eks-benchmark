--3.catalog_returns
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.catalog_returns PURGE;
CREATE TABLE {CATALOG}.{DATABASE}.catalog_returns
    USING iceberg
PARTITIONED BY (cr_returned_date_sk)
TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.catalog_returns;