create or replace table `gcp-wow-rwds-ai-ed-mktplc-dev.wmp.edm_merch_process`
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
where lower(marketplacename) = "everydaymarket"
)
, myd_product_stats as (
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

myd_product_consolidation as (
select 
*,
case when parent_dealid is not null then parent_dealid else myd_productid end as myd_productid_mod,
case when parent_variantid is not null then parent_variantid else myd_variantid end as myd_variantid_mod
from(
select 
mps.*,
mydsp.dealid as parent_dealid,
mydsp.variantid as parent_variantid
from myd_product_stats as mps
left join `mydeal-bigquery.mydeal_feeds.myd_instock_secondary_dealid` as mydsp
on(mps.myd_productid = mydsp.secondary_dealid)
-- where mps.myd_productid  in (
--   13315029	,1794690,14076619)
)
),
consolidated_catalog as (
select 
mps.*,
edm_product_id,
edm_product_created_date,
edm_product_name,
edm_sku,
edm_product_price,
edm_product_stock,
edm_cat_level1,edm_cat_level2,
edm_cat_level3,edm_cat_level4,
edm_active_status,edm_rrp,
wmp_seller_name,wmp_seller_type
from myd_product_consolidation as mps 
left join 
(select edm_product_id,edm_product_created_date,edm_product_name,edm_sku,edm_product_price,edm_product_stock,edm_cat_level1,edm_cat_level2,edm_cat_level3,edm_cat_level4,edm_active_status,edm_rrp,wmp_seller_name,wmp_seller_type,myd_dealid,myd_variantid from `mydeal-bigquery.dbt_data_studio.product_detail_edm` where last_updated_date = (select max(last_updated_date) from `mydeal-bigquery.dbt_data_studio.product_detail_edm`)) as edmps
on(mps.myd_productid = edmps.myd_dealid and ifnull(mps.myd_variantid,001000) = ifnull(edmps.myd_variantid,001000))
),
base_sales as (
select 
bd.dealid,
mydsp.dealid as parent_dealid,
bd.variantid,
mydsp.variantid as parent_variantid,
bd.companyid,
sum(case 
       when lower(customermarketplace) = "everydaymarket" 
           and (date(acceptedtime) between date_trunc(current_date("Australia/Melbourne"),week(MONDAY)) and date_trunc(current_date("Australia/Melbourne"),week(MONDAY))+6)
           and bd.isfreightprotection = false then totalamount end) as edm_gmv_tw,
sum(case 
       when lower(customermarketplace) = "everydaymarket" 
           and (date(acceptedtime) between date_sub(current_date("Australia/Melbourne"),interval 4 week) and current_date("Australia/Melbourne")-1)
           and bd.isfreightprotection = false then totalamount end) as edm_gmv_l4w,
sum(case 
       when lower(customermarketplace) = "everydaymarket" 
           and (date(acceptedtime) between date_trunc(current_date("Australia/Melbourne"),year) and current_date("Australia/Melbourne"))
           and bd.isfreightprotection = false then totalamount end) as edm_gmv_ytd,
sum(case 
      when(date(acceptedtime) between date_sub(current_date("Australia/Melbourne"),interval 12 month) and current_date("Australia/Melbourne")-1)
           and bd.isfreightprotection = false then totalamount end) as middleware_gmv_l12m,
sum(case 
       when lower(customermarketplace) = "everydaymarket" 
           and (date(acceptedtime) between date_trunc(current_date("Australia/Melbourne"),week(MONDAY)) and date_trunc(current_date("Australia/Melbourne"),week(MONDAY))+6)
           and bd.isfreightprotection = false and bd.isfreight = false then qty end) as edm_units_tw,
sum(case 
       when lower(customermarketplace) = "everydaymarket" 
           and (date(acceptedtime) between date_sub(current_date("Australia/Melbourne"),interval 4 week) and current_date("Australia/Melbourne")-1)
           and bd.isfreightprotection = false and bd.isfreight = false then qty end) as edm_units_l4w,
sum(case 
       when lower(customermarketplace) = "everydaymarket" 
           and (date(acceptedtime) between date_trunc(current_date("Australia/Melbourne"),year) and current_date("Australia/Melbourne"))
           and bd.isfreightprotection = false and bd.isfreight = false then qty end) as edm_units_ytd,
sum(case 
      when(date(acceptedtime) between date_sub(current_date("Australia/Melbourne"),interval 12 month) and current_date("Australia/Melbourne")-1)
           and bd.isfreightprotection = false and bd.isfreight = false then qty end) as middleware_units_l12m       

from `mydeal-bigquery.sql_server_rds_dbo.biddeal` as bd 
left join `mydeal-bigquery.sql_server_rds_dbo.recieptitems` as ri 
on(bd.biddealid = ri.biddealid)
left join 
(select paymentrecieptid,customermarketplace  from `mydeal-bigquery.sql_server_rds_dbo.paymentreciept` group by 1,2 ) as p 
on(ri.paymentrecieptid = p.paymentrecieptid)
left join `mydeal-bigquery.mydeal_feeds.myd_instock_secondary_dealid` as mydsp
on(bd.dealid = mydsp.secondary_dealid)
where (
       (concat(bd.dealid,ifnull(cast(bd.variantid as string),"") ) in (select concat(myd_productid,ifnull(cast(myd_variantid as string),"")) from consolidated_catalog )) or
       (concat(bd.dealid,ifnull(cast(bd.variantid as string),"")) in (select concat(parent_dealid,ifnull(cast(parent_variantid as string),"")) from consolidated_catalog ))

       )
    and  status in (3,4)
    and totalamount > 0 
    and (date(acceptedtime) between date_sub(current_date("Australia/Melbourne"), interval 5 week) and current_date("Australia/Melbourne")-1)
group by all
),

sales_consolidated as (
select 
bsc.dealid,
bsc.variantid,
bsc.companyid,
safe_add(ifnull(bsc.edm_gmv_tw,0),ifnull(bsp.edm_gmv_tw,0)) as edm_gmv_tw,
safe_add(ifnull(bsc.edm_gmv_l4w,0),ifnull(bsp.edm_gmv_l4w,0)) as edm_gmv_l4w,
safe_add(ifnull(bsc.edm_gmv_ytd,0),ifnull(bsp.edm_gmv_ytd,0)) as edm_gmv_ytd,
safe_add(ifnull(bsc.middleware_gmv_l12m,0),ifnull(bsp.middleware_gmv_l12m,0)) as middleware_gmv_l12m,
safe_add(ifnull(bsc.edm_units_tw,0),ifnull(bsp.edm_units_tw,0)) as edm_units_tw,
safe_add(ifnull(bsc.edm_units_l4w,0),ifnull(bsp.edm_units_l4w,0)) as edm_units_l4w,
safe_add(ifnull(bsc.edm_units_ytd,0),ifnull(bsp.edm_units_ytd,0)) as edm_units_ytd,
safe_add(ifnull(bsc.middleware_units_l12m,0),ifnull(bsp.middleware_units_l12m,0)) as middleware_units_l12m
from base_sales as bsc
left join base_sales as bsp 
on(bsc.parent_dealid = bsp.dealid and ifnull(bsc.parent_variantid,001000) = ifnull(bsp.variantid,001000))
),

promo_stats as (
select
* 
from(
select 
dp.dealid,
dp.variantid as variantid,
dp.companyid as seller_id,
dp.promotionalprice as promotional_price,
dp.commission as commission_percentage,
date(dp.promotionstarttime) as promotion_start_date,
date(dp.promotionendtime) as promotion_end_date

from `mydeal-bigquery.sql_server_rds_dbo.dealpromotionalprice` as dp 
left join `mydeal-bigquery.mydeal_feeds.myd_instock_secondary_dealid` as mydsp
on(dp.dealid = mydsp.dealid and ifnull(dp.variantid,001000) = ifnull(mydsp.variantid,001000))
where isdeleted = false 
    and _fivetran_deleted = false 
    and isarchived = false 
    and date(promotionendtime)>= current_date("Australia/Melbourne")
    and mydsp.secondary_dealid is null
group by all 
union all 
select 
mydsp.secondary_dealid as dealid,
null as variantid,
dp.companyid as seller_id,
dp.promotionalprice as promotional_price,
dp.commission as commission_percentage,
date(dp.promotionstarttime) as promotion_start_date,
date(dp.promotionendtime) as promotion_end_date

from `mydeal-bigquery.sql_server_rds_dbo.dealpromotionalprice` as dp 
left join `mydeal-bigquery.mydeal_feeds.myd_instock_secondary_dealid` as mydsp
on(dp.dealid = mydsp.dealid and ifnull(dp.variantid,001000) = ifnull(mydsp.variantid,001000))
where isdeleted = false 
    and _fivetran_deleted = false 
    and isarchived = false 
    and date(promotionendtime)>= current_date("Australia/Melbourne")
    and mydsp.secondary_dealid is not null
group by all 
)
),
price_change_date as (
select 
*
from(
select
*
from (
select
date(datetimestamp) AS date,
dealid,
variantid,
sellprice,
lag(sellprice,1) over(partition by dealid, ifnull(variantid,001001) order by date(datetimestamp) asc) as pre_sell_price,
lag(date(datetimestamp),1) over(partition by dealid, ifnull(variantid,001001) order by date(datetimestamp) asc) as pre_sell_price_date
from `mydeal-bigquery.dbt_data_studio.price_history_tracker`)
where
pre_sell_price is not null
and abs(sellprice-pre_sell_price)>0
qualify row_number() over (partition by dealid, ifnull(variantid,001001) order by date desc) = 1
)
),
product_oos as (
select  
date_stock_oos,
deal_id,
variant_id
from `mydeal-bigquery.dbt_data_studio.product_oos_history` 
qualify row_number() over (partition by deal_id,ifnull(variant_id,00100)order by date_stock_oos desc)=1
)
select 
current_date("Australia/Melbourne") as report_run_date,
date_sub(current_date("Australia/Melbourne"),interval 4 week) as sales_start_date,
current_date("Australia/Melbourne")-1 as sales_end_date,
cc.*,
ps.promotional_price,
ps.commission_percentage,
ps.promotion_start_date,
ps.promotion_end_date,
round(edm_gmv_tw,3) as edm_gmv_tw,
round(edm_gmv_l4w,3) as edm_gmv_l4w ,
round(edm_gmv_ytd,3) as edm_gmv_ytd,
round(middleware_gmv_l12m,3) as middleware_gmv_l12m,
edm_units_tw,
edm_units_l4w,
edm_units_ytd,
middleware_units_l12m,
pcd.date as last_price_change_date,
po.date_stock_oos as last_stock_oos_date,
case when ps.promotion_start_date is not null then true else false end as is_promo_product,
mps.middleware_subscribed_status,
mps.product_unsubscribed_reason,
mps.bannernote,
mps.last_middleware_updated,
from consolidated_catalog as cc 
left join promo_stats as ps 
on(cc.myd_productid = ps.dealid and cc.myd_seller_id = ps.seller_id and ifnull(cc.myd_variantid,0000011) = ifnull(ps.variantid,0000011))
left join sales_consolidated as sc 
on(cc.myd_productid = sc.dealid and cc.myd_seller_id = sc.companyid and ifnull(cc.myd_variantid,0000011) = ifnull(sc.variantid,0000011))
left join price_change_date as pcd
on(cc.myd_productid = pcd.dealid and ifnull(cc.myd_variantid,0000011) = ifnull(pcd.variantid,0000011))
left join product_oos as po
on(cc.myd_productid = po.deal_id and ifnull(cc.myd_variantid,0000011) = ifnull(po.variant_id,0000011))
left join middleware_product_status as mps
on(cc.myd_productid = mps.dealid)
