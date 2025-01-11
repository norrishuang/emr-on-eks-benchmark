--1.call_center
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.call_center PURGE;
create table {CATALOG}.{DATABASE}.call_center
    USING iceberg
TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.call_center;