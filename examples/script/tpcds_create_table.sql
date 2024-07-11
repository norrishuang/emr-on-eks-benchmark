CREATE EXTERNAL TABLE `call_center`(
  `cc_call_center_sk` int, 
  `cc_call_center_id` string, 
  `cc_rec_start_date` date, 
  `cc_rec_end_date` date, 
  `cc_closed_date_sk` int, 
  `cc_open_date_sk` int, 
  `cc_name` string, 
  `cc_class` string, 
  `cc_employees` int, 
  `cc_sq_ft` int, 
  `cc_hours` string, 
  `cc_manager` string, 
  `cc_mkt_id` int, 
  `cc_mkt_class` string, 
  `cc_mkt_desc` string, 
  `cc_market_manager` string, 
  `cc_division` int, 
  `cc_division_name` string, 
  `cc_company` int, 
  `cc_company_name` string, 
  `cc_street_number` string, 
  `cc_street_name` string, 
  `cc_street_type` string, 
  `cc_suite_number` string, 
  `cc_city` string, 
  `cc_county` string, 
  `cc_state` string, 
  `cc_zip` string, 
  `cc_country` string, 
  `cc_gmt_offset` decimal(5,2), 
  `cc_tax_percentage` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/call_center/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='tpcds_crawler', 
  'averageRecordSize'='250', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='2', 
  'recordCount'='36', 
  'sizeKey'='13985', 
  'typeOfData'='file');

--create table catalog_page
DROP TABLE catalog_page;
CREATE EXTERNAL TABLE `catalog_page`(
  `cp_catalog_page_sk` int, 
  `cp_catalog_page_id` string, 
  `cp_start_date_sk` int, 
  `cp_end_date_sk` int, 
  `cp_department` string, 
  `cp_catalog_number` int, 
  `cp_catalog_page_number` int, 
  `cp_description` string, 
  `cp_type` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/catalog_page/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='tpcds_crawler', 
  'averageRecordSize'='103', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='2', 
  'recordCount'='11718', 
  'sizeKey'='697339', 
  'typeOfData'='file');

--create table catalog_returns
DROP TABLE catalog_returns;
CREATE EXTERNAL TABLE `catalog_returns`(
  `cr_returned_time_sk` int, 
  `cr_item_sk` int, 
  `cr_refunded_customer_sk` int, 
  `cr_refunded_cdemo_sk` int, 
  `cr_refunded_hdemo_sk` int, 
  `cr_refunded_addr_sk` int, 
  `cr_returning_customer_sk` int, 
  `cr_returning_cdemo_sk` int, 
  `cr_returning_hdemo_sk` int, 
  `cr_returning_addr_sk` int, 
  `cr_call_center_sk` int, 
  `cr_catalog_page_sk` int, 
  `cr_ship_mode_sk` int, 
  `cr_warehouse_sk` int, 
  `cr_reason_sk` int, 
  `cr_order_number` bigint, 
  `cr_return_quantity` int, 
  `cr_return_amount` decimal(7,2), 
  `cr_return_tax` decimal(7,2), 
  `cr_return_amt_inc_tax` decimal(7,2), 
  `cr_fee` decimal(7,2), 
  `cr_return_ship_cost` decimal(7,2), 
  `cr_refunded_cash` decimal(7,2), 
  `cr_reversed_charge` decimal(7,2), 
  `cr_store_credit` decimal(7,2), 
  `cr_net_loss` decimal(7,2))
PARTITIONED BY ( 
  `cr_returned_date_sk` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/catalog_returns/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='tpcds_crawler', 
  'averageRecordSize'='83', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='2105', 
  'partition_filtering.enabled'='true', 
  'recordCount'='71996186', 
  'sizeKey'='5748082420', 
  'typeOfData'='file');
MSCK REPAIR TABLE catalog_returns;


--create table catalog_sales
DROP TABLE catalog_sales;
CREATE EXTERNAL TABLE `catalog_sales`(
  `cs_sold_time_sk` int,
  `cs_ship_date_sk` int,
  `cs_bill_customer_sk` int,
  `cs_bill_cdemo_sk` int,
  `cs_bill_hdemo_sk` int,
  `cs_bill_addr_sk` int,
  `cs_ship_customer_sk` int,
  `cs_ship_cdemo_sk` int,
  `cs_ship_hdemo_sk` int,
  `cs_ship_addr_sk` int,
  `cs_call_center_sk` int,
  `cs_catalog_page_sk` int,
  `cs_ship_mode_sk` int,
  `cs_warehouse_sk` int,
  `cs_item_sk` int,
  `cs_promo_sk` int,
  `cs_order_number` bigint,
  `cs_quantity` int,
  `cs_wholesale_cost` decimal(7,2),
  `cs_list_price` decimal(7,2),
  `cs_sales_price` decimal(7,2),
  `cs_ext_discount_amt` decimal(7,2),
  `cs_ext_sales_price` decimal(7,2),
  `cs_ext_wholesale_cost` decimal(7,2),
  `cs_ext_list_price` decimal(7,2),
  `cs_ext_tax` decimal(7,2),
  `cs_coupon_amt` decimal(7,2),
  `cs_ext_ship_cost` decimal(7,2),
  `cs_net_paid` decimal(7,2),
  `cs_net_paid_inc_tax` decimal(7,2),
  `cs_net_paid_inc_ship` decimal(7,2),
  `cs_net_paid_inc_ship_tax` decimal(7,2),
  `cs_net_profit` decimal(7,2))
PARTITIONED BY (
  `cs_sold_date_sk` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/catalog_sales/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0',
  'CrawlerSchemaSerializerVersion'='1.0',
  'UPDATED_BY_CRAWLER'='tpcds_crawler',
  'averageRecordSize'='71',
  'classification'='parquet',
  'compressionType'='none',
  'objectCount'='1838',
  'partition_filtering.enabled'='true',
  'recordCount'='719999843',
  'sizeKey'='51105089105',
  'typeOfData'='file');
MSCK REPAIR TABLE catalog_sales;

--create table customer
DROP TABLE customer;
CREATE EXTERNAL TABLE `customer`(
  `c_customer_sk` int, 
  `c_customer_id` string, 
  `c_current_cdemo_sk` int, 
  `c_current_hdemo_sk` int, 
  `c_current_addr_sk` int, 
  `c_first_shipto_date_sk` int, 
  `c_first_sales_date_sk` int, 
  `c_salutation` string, 
  `c_first_name` string, 
  `c_last_name` string, 
  `c_preferred_cust_flag` string, 
  `c_birth_day` int, 
  `c_birth_month` int, 
  `c_birth_year` int, 
  `c_birth_country` string, 
  `c_login` string, 
  `c_email_address` string, 
  `c_last_review_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/customer/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');

--create table customer_address
DROP TABLE customer_address;
CREATE EXTERNAL TABLE `customer_address`(
  `ca_address_sk` int, 
  `ca_address_id` string, 
  `ca_street_number` string, 
  `ca_street_name` string, 
  `ca_street_type` string, 
  `ca_suite_number` string, 
  `ca_city` string, 
  `ca_county` string, 
  `ca_state` string, 
  `ca_zip` string, 
  `ca_country` string, 
  `ca_gmt_offset` decimal(5,2), 
  `ca_location_type` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/customer_address/'
TBLPROPERTIES (
  'averageRecordSize'='34', 
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');


--create table customer_demographics
DROP TABLE customer_demographics;
CREATE EXTERNAL TABLE `customer_demographics`(
  `cd_demo_sk` int, 
  `cd_gender` string, 
  `cd_marital_status` string, 
  `cd_education_status` string, 
  `cd_purchase_estimate` int, 
  `cd_credit_rating` string, 
  `cd_dep_count` int, 
  `cd_dep_employed_count` int, 
  `cd_dep_college_count` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/customer_demographics/'
TBLPROPERTIES (
  'averageRecordSize'='4', 
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');

--create table date_dim
DROP TABLE date_dim;
CREATE EXTERNAL TABLE `date_dim`(
  `d_date_sk` int, 
  `d_date_id` string, 
  `d_date` date, 
  `d_month_seq` int, 
  `d_week_seq` int, 
  `d_quarter_seq` int, 
  `d_year` int, 
  `d_dow` int, 
  `d_moy` int, 
  `d_dom` int, 
  `d_qoy` int, 
  `d_fy_year` int, 
  `d_fy_quarter_seq` int, 
  `d_fy_week_seq` int, 
  `d_day_name` string, 
  `d_quarter_name` string, 
  `d_holiday` string, 
  `d_weekend` string, 
  `d_following_holiday` string, 
  `d_first_dom` int, 
  `d_last_dom` int, 
  `d_same_day_ly` int, 
  `d_same_day_lq` int, 
  `d_current_day` string, 
  `d_current_week` string, 
  `d_current_month` string, 
  `d_current_quarter` string, 
  `d_current_year` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/date_dim/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');

--create table household_demographics
DROP TABLE household_demographics;
CREATE EXTERNAL TABLE `household_demographics`(
  `hd_demo_sk` int, 
  `hd_income_band_sk` int, 
  `hd_buy_potential` string, 
  `hd_dep_count` int, 
  `hd_vehicle_count` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/household_demographics/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');

--create table income_band
DROP TABLE income_band;
CREATE EXTERNAL TABLE `income_band`(
  `ib_income_band_sk` int, 
  `ib_lower_bound` int, 
  `ib_upper_bound` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/income_band/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');

DROP TABLE inventory;
CREATE EXTERNAL TABLE `inventory`(
  `inv_item_sk` int, 
  `inv_warehouse_sk` int, 
  `inv_quantity_on_hand` int)
PARTITIONED BY ( 
  `inv_date_sk` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/inventory/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'partition_filtering.enabled'='true', 
  'typeOfData'='file');

MSCK REPAIR TABLE inventory;

--create table item
DROP TABLE item;
CREATE EXTERNAL TABLE `item`(
  `i_item_sk` int, 
  `i_item_id` string, 
  `i_rec_start_date` date, 
  `i_rec_end_date` date, 
  `i_item_desc` string, 
  `i_current_price` decimal(7,2), 
  `i_wholesale_cost` decimal(7,2), 
  `i_brand_id` int, 
  `i_brand` string, 
  `i_class_id` int, 
  `i_class` string, 
  `i_category_id` int, 
  `i_category` string, 
  `i_manufact_id` int, 
  `i_manufact` string, 
  `i_size` string, 
  `i_formulation` string, 
  `i_color` string, 
  `i_units` string, 
  `i_container` string, 
  `i_manager_id` int, 
  `i_product_name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/item/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');


--create table promotion;
DROP TABLE promotion;
CREATE EXTERNAL TABLE `promotion`(
  `p_promo_sk` int, 
  `p_promo_id` string, 
  `p_start_date_sk` int, 
  `p_end_date_sk` int, 
  `p_item_sk` int, 
  `p_cost` decimal(15,2), 
  `p_response_target` int, 
  `p_promo_name` string, 
  `p_channel_dmail` string, 
  `p_channel_email` string, 
  `p_channel_catalog` string, 
  `p_channel_tv` string, 
  `p_channel_radio` string, 
  `p_channel_press` string, 
  `p_channel_event` string, 
  `p_channel_demo` string, 
  `p_channel_details` string, 
  `p_purpose` string, 
  `p_discount_active` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/promotion/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');


--create table reason
DROP TABLE reason;
CREATE EXTERNAL TABLE `reason`(
  `r_reason_sk` int, 
  `r_reason_id` string, 
  `r_reason_desc` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/reason/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');


--create table ship_mode
DROP TABLE ship_mode;
CREATE EXTERNAL TABLE `ship_mode`(
  `sm_ship_mode_sk` int, 
  `sm_ship_mode_id` string, 
  `sm_type` string, 
  `sm_code` string, 
  `sm_carrier` string, 
  `sm_contract` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/ship_mode/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');

--create table store
DROP TABLE store;
CREATE EXTERNAL TABLE `store`(
  `s_store_sk` int, 
  `s_store_id` string, 
  `s_rec_start_date` date, 
  `s_rec_end_date` date, 
  `s_closed_date_sk` int, 
  `s_store_name` string, 
  `s_number_employees` int, 
  `s_floor_space` int, 
  `s_hours` string, 
  `s_manager` string, 
  `s_market_id` int, 
  `s_geography_class` string, 
  `s_market_desc` string, 
  `s_market_manager` string, 
  `s_division_id` int, 
  `s_division_name` string, 
  `s_company_id` int, 
  `s_company_name` string, 
  `s_street_number` string, 
  `s_street_name` string, 
  `s_street_type` string, 
  `s_suite_number` string, 
  `s_city` string, 
  `s_county` string, 
  `s_state` string, 
  `s_zip` string, 
  `s_country` string, 
  `s_gmt_offset` decimal(5,2), 
  `s_tax_precentage` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/store/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');

--create table store_returns
DROP TABLE store_returns;
CREATE EXTERNAL TABLE `store_returns`(
  `sr_return_time_sk` int, 
  `sr_item_sk` int, 
  `sr_customer_sk` int, 
  `sr_cdemo_sk` int, 
  `sr_hdemo_sk` int, 
  `sr_addr_sk` int, 
  `sr_store_sk` int, 
  `sr_reason_sk` int, 
  `sr_ticket_number` bigint, 
  `sr_return_quantity` int, 
  `sr_return_amt` decimal(7,2), 
  `sr_return_tax` decimal(7,2), 
  `sr_return_amt_inc_tax` decimal(7,2), 
  `sr_fee` decimal(7,2), 
  `sr_return_ship_cost` decimal(7,2), 
  `sr_refunded_cash` decimal(7,2), 
  `sr_reversed_charge` decimal(7,2), 
  `sr_store_credit` decimal(7,2), 
  `sr_net_loss` decimal(7,2))
PARTITIONED BY ( 
  `sr_returned_date_sk` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/store_returns/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'partition_filtering.enabled'='true', 
  'typeOfData'='file');

MSCK REPAIR TABLE store_returns;

DROP TABLE store_sales;
CREATE EXTERNAL TABLE `store_sales`(
  `ss_sold_time_sk` int, 
  `ss_item_sk` int, 
  `ss_customer_sk` int, 
  `ss_cdemo_sk` int, 
  `ss_hdemo_sk` int, 
  `ss_addr_sk` int, 
  `ss_store_sk` int, 
  `ss_promo_sk` int, 
  `ss_ticket_number` bigint, 
  `ss_quantity` int, 
  `ss_wholesale_cost` decimal(7,2), 
  `ss_list_price` decimal(7,2), 
  `ss_sales_price` decimal(7,2), 
  `ss_ext_discount_amt` decimal(7,2), 
  `ss_ext_sales_price` decimal(7,2), 
  `ss_ext_wholesale_cost` decimal(7,2), 
  `ss_ext_list_price` decimal(7,2), 
  `ss_ext_tax` decimal(7,2), 
  `ss_coupon_amt` decimal(7,2), 
  `ss_net_paid` decimal(7,2), 
  `ss_net_paid_inc_tax` decimal(7,2), 
  `ss_net_profit` decimal(7,2))
PARTITIONED BY ( 
  `ss_sold_date_sk` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/store_sales/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'partition_filtering.enabled'='true', 
  'typeOfData'='file');

MSCK REPAIR TABLE store_sales;

DROP TABLE time_dim;
CREATE EXTERNAL TABLE `time_dim`(
  `t_time_sk` int, 
  `t_time_id` string, 
  `t_time` int, 
  `t_hour` int, 
  `t_minute` int, 
  `t_second` int, 
  `t_am_pm` string, 
  `t_shift` string, 
  `t_sub_shift` string, 
  `t_meal_time` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/time_dim/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');


--create table warehouse
DROP TABLE warehouse;
CREATE EXTERNAL TABLE `warehouse`(
  `w_warehouse_sk` int, 
  `w_warehouse_id` string, 
  `w_warehouse_name` string, 
  `w_warehouse_sq_ft` int, 
  `w_street_number` string, 
  `w_street_name` string, 
  `w_street_type` string, 
  `w_suite_number` string, 
  `w_city` string, 
  `w_county` string, 
  `w_state` string, 
  `w_zip` string, 
  `w_country` string, 
  `w_gmt_offset` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/warehouse/'
TBLPROPERTIES ( 
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');

--create table web_page
DROP TABLE web_page;
CREATE EXTERNAL TABLE `web_page`(
  `wp_web_page_sk` int, 
  `wp_web_page_id` string, 
  `wp_rec_start_date` date, 
  `wp_rec_end_date` date, 
  `wp_creation_date_sk` int, 
  `wp_access_date_sk` int, 
  `wp_autogen_flag` string, 
  `wp_customer_sk` int, 
  `wp_url` string, 
  `wp_type` string, 
  `wp_char_count` int, 
  `wp_link_count` int, 
  `wp_image_count` int, 
  `wp_max_ad_count` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/web_page/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');

--create table web_returns
DROP TABLE web_returns;
CREATE EXTERNAL TABLE `web_returns`(
  `wr_returned_time_sk` int, 
  `wr_item_sk` int, 
  `wr_refunded_customer_sk` int, 
  `wr_refunded_cdemo_sk` int, 
  `wr_refunded_hdemo_sk` int, 
  `wr_refunded_addr_sk` int, 
  `wr_returning_customer_sk` int, 
  `wr_returning_cdemo_sk` int, 
  `wr_returning_hdemo_sk` int, 
  `wr_returning_addr_sk` int, 
  `wr_web_page_sk` int, 
  `wr_reason_sk` int, 
  `wr_order_number` bigint, 
  `wr_return_quantity` int, 
  `wr_return_amt` decimal(7,2), 
  `wr_return_tax` decimal(7,2), 
  `wr_return_amt_inc_tax` decimal(7,2), 
  `wr_fee` decimal(7,2), 
  `wr_return_ship_cost` decimal(7,2), 
  `wr_refunded_cash` decimal(7,2), 
  `wr_reversed_charge` decimal(7,2), 
  `wr_account_credit` decimal(7,2), 
  `wr_net_loss` decimal(7,2))
PARTITIONED BY ( 
  `wr_returned_date_sk` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/web_returns/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'partition_filtering.enabled'='true', 
  'typeOfData'='file');

MSCK REPAIR TABLE web_returns;

DROP TABLE web_sales;
CREATE EXTERNAL TABLE `web_sales`(
  `ws_sold_time_sk` int, 
  `ws_ship_date_sk` int, 
  `ws_item_sk` int, 
  `ws_bill_customer_sk` int, 
  `ws_bill_cdemo_sk` int, 
  `ws_bill_hdemo_sk` int, 
  `ws_bill_addr_sk` int, 
  `ws_ship_customer_sk` int, 
  `ws_ship_cdemo_sk` int, 
  `ws_ship_hdemo_sk` int, 
  `ws_ship_addr_sk` int, 
  `ws_web_page_sk` int, 
  `ws_web_site_sk` int, 
  `ws_ship_mode_sk` int, 
  `ws_warehouse_sk` int, 
  `ws_promo_sk` int, 
  `ws_order_number` bigint, 
  `ws_quantity` int, 
  `ws_wholesale_cost` decimal(7,2), 
  `ws_list_price` decimal(7,2), 
  `ws_sales_price` decimal(7,2), 
  `ws_ext_discount_amt` decimal(7,2), 
  `ws_ext_sales_price` decimal(7,2), 
  `ws_ext_wholesale_cost` decimal(7,2), 
  `ws_ext_list_price` decimal(7,2), 
  `ws_ext_tax` decimal(7,2), 
  `ws_coupon_amt` decimal(7,2), 
  `ws_ext_ship_cost` decimal(7,2), 
  `ws_net_paid` decimal(7,2), 
  `ws_net_paid_inc_tax` decimal(7,2), 
  `ws_net_paid_inc_ship` decimal(7,2), 
  `ws_net_paid_inc_ship_tax` decimal(7,2), 
  `ws_net_profit` decimal(7,2))
PARTITIONED BY ( 
  `ws_sold_date_sk` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/web_sales/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');

MSCK REPAIR TABLE web_sales;

DROP TABLE web_site;
CREATE EXTERNAL TABLE `web_site`(
  `web_site_sk` int, 
  `web_site_id` string, 
  `web_rec_start_date` date, 
  `web_rec_end_date` date, 
  `web_name` string, 
  `web_open_date_sk` int, 
  `web_close_date_sk` int, 
  `web_class` string, 
  `web_manager` string, 
  `web_mkt_id` int, 
  `web_mkt_class` string, 
  `web_mkt_desc` string, 
  `web_market_manager` string, 
  `web_company_id` int, 
  `web_company_name` string, 
  `web_street_number` string, 
  `web_street_name` string, 
  `web_street_type` string, 
  `web_suite_number` string, 
  `web_city` string, 
  `web_county` string, 
  `web_state` string, 
  `web_zip` string, 
  `web_country` string, 
  `web_gmt_offset` decimal(5,2), 
  `web_tax_percentage` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://tpcds-bemchmark-bucket-812046859005/BLOG_TPCDS-TEST-3T-partitioned/web_site/'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');
