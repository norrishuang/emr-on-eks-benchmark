--19.time_dim
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.time_dim PURGE;
create table {CATALOG}.{DATABASE}.time_dim
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
    select * from tpcds.time_dim;

--20.warehouse
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.warehouse PURGE;
create table {CATALOG}.{DATABASE}.warehouse
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
    select * from tpcds.warehouse;

--21.web_page
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.web_page PURGE;
create table {CATALOG}.{DATABASE}.web_page
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
    select * from tpcds.web_page;