--2.catalog_page
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.catalog_page PURGE;
create table {CATALOG}.{DATABASE}.catalog_page
    USING iceberg
TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.catalog_page;