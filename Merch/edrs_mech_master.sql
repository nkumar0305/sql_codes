create or replace table `gcp-wow-rwds-ai-ed-mktplc-dev.wmp.edrs_merch_proces`
partition by report_run_date
cluster by myd_productid as
with middleware_product_status as (
select
marketplacename,
dealid,
subscribed as middleware_subscribed_status,
unsubscribedreason as product_unsubscribed_reason,
bannernote,
date(subscribedlastupdated) as last_middleware_updated
from `mydeal-bigquery.sql_server_rds_dbo.marketplacedealdistribution`
where lower(marketplacename) = "everydayrewards"
),

myd_product_stats as (
select  
cm.companyid as myd_seller_id,
cm.description as myd_seller_name,
dl.dealid as myd_productid,
dl.maindeal as myd_product_name,
concat("https://www.mydeal.com.au/",dl.url) as myd_product_url,
v.variantid as myd_variantid,
b.brandid as myd_brandid,
b.brandname as myd_brand_name,
mw.tagid as myd_category_id,
mw.level1,
mw.level2,
mw.level3,
mw.level4,
case when v.sku is null then dl.sku else v.sku end as myd_product_sku,
case when v.sellprice is null then dl.customerprice else v.sellprice end as product_sell_price,
case when v.rrp is null then dl.rrp else v.rrp end as product_retail_price,
case when v.stocklevel is null then dl.quota else v.stocklevel end as product_stock_level,
case when date(v.createdtime) is null then date(dl.dealcreatedtime) else date(v.createdtime) end as myd_product_created_date
from `mydeal-bigquery.sql_server_rds_dbo.deal` as dl 
left join `mydeal-bigquery.sql_server_rds_dbo.product` as p 
on(dl.dealid = p.dealid)
left join `mydeal-bigquery.sql_server_rds_dbo.variant` as v 
on(p.productid = v.productid)
left join `mydeal-bigquery.sql_server_rds_dbo.brand` as b 
on(dl.brandid = b.brandid)
left join `mydeal-bigquery.dbt_data_studio.deal_category_seller_mw` as mw 
on(dl.dealid = mw.dealid)
left join `mydeal-bigquery.sql_server_rds_dbo.company` as cm 
on(dl.companyid = cm.companyid)
where 
    dl.dealid in (select distinct dealid from middleware_product_status)
and cm._fivetran_deleted = false 

),
myd_product_sales as (
select 
bd.dealid,
bd.variantid,
bd.companyid,
sum(case 
       when lower(customermarketplace) = "everydayrewards" 
           and (date(acceptedtime) between date_sub(current_date("Australia/Melbourne"),interval 4 week) and current_date("Australia/Melbourne")-1)
           and bd.isfreightprotection = false then totalamount end) as edrs_gmv_l4w,
sum(case 
       when lower(customermarketplace) = "everydayrewards" 
           and (date(acceptedtime) between date_sub(current_date("Australia/Melbourne"),interval 4 week) and current_date("Australia/Melbourne")-1)
           and bd.isfreightprotection = false and bd.isfreight = false then qty end) as edrs_units_l4w,

sum(case 
      when(date(acceptedtime) between date_sub(current_date("Australia/Melbourne"),interval 4 week) and current_date("Australia/Melbourne")-1)
           and bd.isfreightprotection = false then totalamount end) as middleware_gmv_l4w,
sum(case 
       when(date(acceptedtime) between date_sub(current_date("Australia/Melbourne"),interval 4 week) and current_date("Australia/Melbourne")-1)
           and bd.isfreightprotection = false and bd.isfreight = false then qty end) as middleware_units_l4w          

from `mydeal-bigquery.sql_server_rds_dbo.biddeal` as bd 
left join `mydeal-bigquery.sql_server_rds_dbo.recieptitems` as ri 
on(bd.biddealid = ri.biddealid)
left join 
(select paymentrecieptid,customermarketplace  from `mydeal-bigquery.sql_server_rds_dbo.paymentreciept` group by 1,2 ) as p 
on(ri.paymentrecieptid = p.paymentrecieptid)
where bd.dealid in (select distinct dealid from middleware_product_status) 
    and  status in (3,4)
    and totalamount > 0 
    and (date(acceptedtime) between date_sub(current_date("Australia/Melbourne"), interval 5 week) and current_date("Australia/Melbourne")-1)
group by all
),
edrs_product_stats as (
select  
product_id,
Variant_id,
internal_product_id,
seller_id,
date(lastPublishedDate) as edrs_product_created_date,
case when lower(Active_status) = "active" then true else false end as edrs_published,
WMPSellerName as wmp_seller_name
from `gcp-wow-rwds-ai-ed-mktplc-dev.wmp.wmp_product_lookup` 
where lower(marketplace) = "everydayrewards"
),
myd_promo_price as 
(select 
dealid,
variantid,
companyid as seller_id,
promotionalprice as promotional_price,
commission as commission_percentage,
date(promotionstarttime) as promotion_start_date,
date(promotionendtime) as promotion_end_date
from `mydeal-bigquery.sql_server_rds_dbo.dealpromotionalprice` 
where isdeleted = false 
    and _fivetran_deleted = false 
    and isarchived = false 
    and date(promotionendtime)>= current_date("Australia/Melbourne")
)

select 
myd_product_created_date,
myd_product_sku,
myd_productid,
myd_product_name,
myd_variantid,
myd_product_url,
myd_seller_id,
myd_seller_name,
myd_category_id,
level1 as myd_level1,
level2 as myd_level2,
level3 as myd_level3,
level4 as myd_level4,
myd_brandid,
myd_brand_name,
product_sell_price,
product_retail_price,
product_stock_level,
mps.middleware_subscribed_status,
mps.product_unsubscribed_reason,
mps.bannernote,
mps.last_middleware_updated,
eps.internal_product_id,
eps.edrs_product_created_date,
eps.edrs_published,
eps.wmp_seller_name,
mpp.promotional_price,
mpp.commission_percentage,
mpp.promotion_start_date,
mpp.promotion_end_date,
round(mss.edrs_gmv_l4w,3) as edrs_gmv_l4w,
round(mss.edrs_units_l4w,3) as edrs_units_l4w ,
round(mss.middleware_gmv_l4w,3) as middleware_gmv_l4w,
round(mss.middleware_units_l4w,3) as middleware_units_l4w,
current_date("Australia/Melbourne") as report_run_date,
date_sub(current_date("Australia/Melbourne"),interval 4 week) as sales_start_date,
current_date("Australia/Melbourne")-1 as sales_end_date
from myd_product_stats as mdps
left join middleware_product_status as mps
on(mdps.myd_productid = mps.dealid) 
left join  edrs_product_stats as eps
on(cast(mdps.myd_productid as string) = cast(eps.product_id as string) and cast(mdps.myd_seller_id as string) = eps.seller_id
and ifnull(cast(mdps.myd_variantid as string),"0000011") = ifnull(eps.Variant_id,"0000011")) 
left join myd_promo_price as mpp 
on(mdps.myd_productid = mpp.dealid and mdps.myd_seller_id = mpp.seller_id and ifnull(mdps.myd_variantid,0000011) = ifnull(mpp.variantid,0000011))
left join myd_product_sales as mss 
on(mdps.myd_productid = mss.dealid and mdps.myd_seller_id = mss.companyid and ifnull(mdps.myd_variantid,0000011) = ifnull(mss.variantid,0000011))
