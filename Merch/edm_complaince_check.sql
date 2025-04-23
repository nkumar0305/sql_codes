-- create temp function get_sell_price(lookback_date date,myd_product_id string,myd_variant_id string) as (
-- (select  
-- product_price as product_sell_price

-- from `gcp-wow-rwds-ai-ed-mktplc-dev.wmp.wmp_product_lookup_hist` 
-- where 
-- lower(marketplace) = "myd"
-- and run_date = lookback_date
-- and product_id = myd_product_id
-- and Variant_id = myd_variant_id
-- limit 1)
-- );

create temp table sell_price as (
select  
run_date,
product_id,
Variant_id,
cast(first_value(product_price) over (partition by run_date, product_id, Variant_id order by run_date) as float64) AS product_sell_price 
from `gcp-wow-rwds-ai-ed-mktplc-dev.wmp.wmp_product_lookup_hist` 
where 
lower(marketplace) = "myd"
);

create temp function compliance_check (promo_start_date date, lookback_date date,product_price float64, promo_price float64,last_price_change_date date,last_product_oos_date date,product_created_date date) as 

(
struct(
  case when lookback_date<promo_start_date and last_price_change_date between lookback_date and promo_start_date then true end as price_change_check,
  case when  product_created_date > promo_start_date then true end as published_date_check,
  case when lookback_date<promo_start_date and last_product_oos_date between lookback_date and promo_start_date then true end as product_oos_check,
  case when lookback_date<promo_start_date and product_price <promo_price then true end as sellprice_check,
  case when lookback_date<promo_start_date and round(safe_divide((product_price-promo_price),product_price),2)*100 <5.0 then true end as savings_check

)
);

create temp function compliance_flag(complaince_columns struct<field1 bool,field2 bool,field3 bool,field4 bool,field5 bool>) as (
struct(
  case when complaince_columns.field1 is not null or complaince_columns.field2 is not null or complaince_columns.field3 is not null and complaince_columns.field4 is not null and complaince_columns.field5 is not null then "Non Compliant"
  else "Complaint" end as complaint_flag,
regexp_replace(
  concat(ifnull(case when complaince_columns.field1 is not null then "[Price change check failed]" end,""),
         ifnull(case when complaince_columns.field2 is not null then "[Product created after promo start date]" end,""),
         ifnull(case when complaince_columns.field3 is not null then "[Product oos check failed]" end,""),
         ifnull(case when complaince_columns.field4 is not null then "[Promo price greater than sell price]" end,""),
         ifnull(case when complaince_columns.field5 is not null then "[Savings is less than 5%]" end,"")),r"]\[","]\n[") as complaince_text
) );
select 
myd_product_created_date,
myd_productid,
myd_product_name,
myd_variantid,
myd_product_sku,
product_sell_price,
product_retail_price,
product_stock_level,
promotional_price,
promotion_start_date,
promotion_end_date,
last_price_change_date,
last_stock_oos_date,
lookback_14days,
lookback_7days,
lookback_3days,
sell_price_lb14,
sell_price_lb7,
sell_price_lb3,
complaince_check_lb14.complaint_flag as complaint_flag_lb14,
complaince_check_lb14.complaince_text as complaint_text_lb14,
complaince_check_lb7.complaint_flag as complaint_flag_lb7,
complaince_check_lb7.complaince_text as complaint_text_lb7,
complaince_check_lb3.complaint_flag as complaint_flag_lb3,
complaince_check_lb3.complaince_text as complaint_text_lb3
from(
select 
base.*,
sp14.product_sell_price as sell_price_lb14,
sp7.product_sell_price as sell_price_lb7,
sp3.product_sell_price as sell_price_lb3,
compliance_flag(compliance_check(promotion_start_date,lookback_14days,sp14.product_sell_price,promotional_price,last_price_change_date,last_stock_oos_date,myd_product_created_date)) as complaince_check_lb14,
compliance_flag(compliance_check(promotion_start_date,lookback_7days,sp7.product_sell_price,promotional_price,last_price_change_date,last_stock_oos_date,myd_product_created_date)) as complaince_check_lb7,
compliance_flag(compliance_check(promotion_start_date,lookback_3days,sp3.product_sell_price,promotional_price,last_price_change_date,last_stock_oos_date,myd_product_created_date)) as complaince_check_lb3
from(
(
select 
myd_product_created_date,
myd_productid,
myd_product_name,
myd_variantid,
myd_product_sku,
product_sell_price,
product_retail_price,
product_stock_level,
promotional_price,
promotion_start_date,
promotion_end_date,
last_price_change_date,
last_stock_oos_date,
date_sub(promotion_start_date,interval 14 day) as lookback_14days,
date_sub(promotion_start_date,interval 7 day) as lookback_7days,
date_sub(promotion_start_date,interval 3 day) as lookback_3days
from `gcp-wow-rwds-ai-ed-mktplc-dev._3411302a4d4bf286248cff016de43d7b4d5bc402.anon790eec24_832a_4e48_b7a0_fdd119f56915` 
where is_promo_product = true
) as base
left join sell_price as sp14
on(sp14.run_date = base.lookback_14days and 
sp14.product_id = cast(base.myd_productid as string) and 
ifnull(cast(sp14.Variant_id as int64),00100) = ifnull(base.myd_variantid,00100))
left join sell_price as sp7
on(sp7.run_date = base.lookback_7days and 
sp7.product_id = cast(base.myd_productid as string) and 
ifnull(cast(sp7.Variant_id as int64),00100)  = ifnull(base.myd_variantid,00100))
left join sell_price as sp3 
on(sp3.run_date = base.lookback_3days and 
sp3.product_id = cast(base.myd_productid as string) and 
ifnull(cast(sp3.Variant_id as int64),00100) = ifnull(base.myd_variantid,00100) )
)
)
