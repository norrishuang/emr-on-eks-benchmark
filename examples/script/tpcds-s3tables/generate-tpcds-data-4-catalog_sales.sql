--4.catalog_sales
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.catalog_sales PURGE;
CREATE TABLE {CATALOG}.{DATABASE}.catalog_sales
    USING iceberg
PARTITIONED BY (cs_sold_date_sk)
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.catalog_sales;
