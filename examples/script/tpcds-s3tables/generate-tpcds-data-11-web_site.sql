--24.web_site
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.web_site PURGE;
create table {CATALOG}.{DATABASE}.web_site
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
    select * from tpcds.web_site;