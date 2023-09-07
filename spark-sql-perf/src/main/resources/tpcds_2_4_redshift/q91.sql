--q91.sql--

 select
        cc_call_center_id Call_Center, cc_name Call_Center_Name, cc_manager Manager,
        sum(cr_net_loss) Returns_Loss
 from
     dev.spectrum_iceberg_schema.store_returns.call_center,
     dev.spectrum_iceberg_schema.store_returns.catalog_returns,
     dev.spectrum_iceberg_schema.store_returns.date_dim,
     dev.spectrum_iceberg_schema.store_returns.customer,
     dev.spectrum_iceberg_schema.store_returns.customer_address,
     dev.spectrum_iceberg_schema.store_returns.customer_demographics,
     dev.spectrum_iceberg_schema.store_returns.household_demographics
 where
        cr_call_center_sk        = cc_call_center_sk
 and    cr_returned_date_sk      = d_date_sk
 and    cr_returning_customer_sk = c_customer_sk
 and    cd_demo_sk               = c_current_cdemo_sk
 and    hd_demo_sk               = c_current_hdemo_sk
 and    ca_address_sk            = c_current_addr_sk
 and    d_year                   = 1998
 and    d_moy                    = 11
 and    ( (cd_marital_status     = 'M' and cd_education_status = 'Unknown')
        or(cd_marital_status     = 'W' and cd_education_status = 'Advanced Degree'))
 and    hd_buy_potential like 'Unknown%'
 and    ca_gmt_offset            = -7
 group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
 order by sum(cr_net_loss) desc
            
