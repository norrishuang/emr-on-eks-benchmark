--5.customer
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.customer PURGE;
create table {CATALOG}.{DATABASE}.customer
    USING iceberg
TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.customer;

--6.customer_address
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.customer_address PURGE;
create table {CATALOG}.{DATABASE}.customer_address
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.customer_address;

--7.customer_demographics
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.customer_demographics PURGE;
create table {CATALOG}.{DATABASE}.customer_demographics
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.customer_demographics;

--8.date_dim
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.date_dim PURGE;

create table {CATALOG}.{DATABASE}.date_dim
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.date_dim;

--9.household_demographics
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.household_demographics PURGE;
create table {CATALOG}.{DATABASE}.household_demographics
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.household_demographics;

--10.income_band
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.income_band PURGE;
create table {CATALOG}.{DATABASE}.income_band
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.income_band;

--11.inventory
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.inventory PURGE;
create table {CATALOG}.{DATABASE}.inventory
    USING iceberg
    PARTITIONED BY (inv_date_sk)
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
    select * from tpcds.inventory;

--12.item
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.item PURGE;
create table {CATALOG}.{DATABASE}.item
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.item;

--13.promotion
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.promotion PURGE;
create table {CATALOG}.{DATABASE}.promotion
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.promotion;

--14.reason
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.reason PURGE;
create table {CATALOG}.{DATABASE}.reason
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
select * from tpcds.reason;

--15.ship_mode
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.ship_mode PURGE;
create table {CATALOG}.{DATABASE}.ship_mode
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
    select * from tpcds.ship_mode;


--16.store
DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.store PURGE;
create table {CATALOG}.{DATABASE}.store
    USING iceberg
    TBLPROPERTIES (
    'write.object-storage.enabled'='{ENABLE_HASH}',
    'write.target-file-size-bytes'='{FILESIZE}') as
    select * from tpcds.store;
