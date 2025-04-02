with remove_duplicates_from_order as (
select
*
from `gcp-wow-bigw-digi-analy-prod.internal_marketplace_reporting.order_line_detail` 
qualify row_number() over (partition by order_id,consignment_id,order_line_id  order by date(order_created_timestamp) desc) = 1
)

, 
customer_transaction_stats as (
select 
customer_id,
min(date(order_created_timestamp)) as first_transaction_date,
max(date(order_created_timestamp)) as latest_transaction_date
from remove_duplicates_from_order
group by all 
)

,
customer_base as (
select 
*
except(company_mix)
from(
select 
*,
case when transaction_rank = 1 and company_mix like "%1P%" then 1 else 0 end as first_transaction_contains_1p,
case when transaction_rank = 1 and company_mix not like "%3P%" then 1 else 0 end as first_transaction_1p_only,
from(

select 
*,
string_agg((case when external_seller_id in(47684,44456) then "1P" else "3P" end)) over (partition by customer_id,paymentreciept_id order by transaction_date) as company_mix,
dense_rank() over (partition by customer_id order by transaction_date) as transaction_rank,
sum(product_revenue) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_revenue,
count(product_id) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_products,
count(case when external_seller_id in(47684,44456) then product_id end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_1p_products,
count(case when external_seller_id not in(47684,44456) then product_id end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_3p_products,
sum(case when external_seller_id in(47684,44456) then product_revenue end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_revenue_1p_products,
sum(case when external_seller_id not in(47684,44456) then product_revenue end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction__revenue_3p_products,
-- sum(product_revenue) over (partition by customer_id order by transaction_date) as customer_lifetime_revenue,
-- count(paymentreciept_id) over (partition by customer_id order by transaction_date) as customer_lifetime_transactions,
from(
select 
customer_id,
order_id as paymentreciept_id,
date(order_created_timestamp) as transaction_date,
rmd.product_id,
rmd.product_name,
dcs.level1 as myd_cat_lv1,
rmd.seller_id,
dl.companyid as external_seller_id,
rmd.seller_name,
sum(ordered_gmv) as product_revenue
from remove_duplicates_from_order as rmd
left join `gcp-wow-bigw-digi-analy-prod.internal_marketplace_reporting.product_detail` as pd
on(rmd.product_id = pd.product_id)
left join `mydeal-bigquery.sql_server_rds_dbo.deal` as dl
on(pd.external_product_id = cast(dl.dealid as string))
left join `mydeal-bigquery.dbt_data_studio.deal_category_seller_mw` as dcs
on(dl.dealid = dcs.dealid)
where customer_id in (select customer_id from customer_transaction_stats )
-- customer_id =8801756872708
group by all
)))
)

,onep_new_customers as (
select 
distinct customer_id
from customer_base 
where first_transaction_contains_1p = 1
)

,onep_cl_stats as (
select 
customer_id,
count(distinct paymentreciept_id) as cl_transactions,
round(sum(product_revenue),2) as cl_product_revenue,
round(sum(case when external_seller_id in(47684,44456) then product_revenue end),2) as cl_1p_product_revenue,
round(sum(case when external_seller_id not in(47684,44456) then product_revenue end),2) as cl_3p_product_revenue,
count(distinct product_id ) as cl_unique_products,
count(distinct case when external_seller_id in(47684,44456) then product_id end) as cl_1p_unique_products,
count(distinct case when external_seller_id not in(47684,44456) then product_id end) as cl_3p_unique_products,
count(distinct case when first_transaction_1p_only =1 then paymentreciept_id end) as cl_transactions_1p_only,
round(sum(case when external_seller_id in(47684,44456) and first_transaction_1p_only =1 then product_revenue end),2) as cl_1p_product_revenue_1p_only,
round(sum(case when lower(myd_cat_lv1) = "fashion" then product_revenue end),2) as cl_product_revenue_fashion,
round(sum(case when lower(myd_cat_lv1) = "furniture" then product_revenue end),2) as cl_product_revenue_furniture,
round(sum(case when lower(myd_cat_lv1) = "electronics" then product_revenue end),2) as cl_product_revenue_electronics,
round(sum(case when lower(myd_cat_lv1) = "toys & games" then product_revenue end),2) as cl_product_revenue_toys,
round(sum(case when lower(myd_cat_lv1) = "business & industrial" then product_revenue end),2) as cl_product_revenue_business,
round(sum(case when lower(myd_cat_lv1) = "sports & outdoors" then product_revenue end),2) as cl_product_revenue_sports,
round(sum(case when lower(myd_cat_lv1) = "books & media" then product_revenue end),2) as cl_product_revenue_books,
round(sum(case when lower(myd_cat_lv1) = "hobbies & entertainment" then product_revenue end),2) as cl_product_revenue_entertainment,
round(sum(case when lower(myd_cat_lv1) = "food & beverages" then product_revenue end),2) as cl_product_revenue_food,
round(sum(case when lower(myd_cat_lv1) = "baby & kids" then product_revenue end),2) as cl_product_revenue_baby,
round(sum(case when lower(myd_cat_lv1) = "health & beauty" then product_revenue end),2) as cl_product_revenue_health,
round(sum(case when lower(myd_cat_lv1) = "tools & equipment" then product_revenue end),2) as cl_product_revenue_tools,
round(sum(case when lower(myd_cat_lv1) = "home & garden" then product_revenue end),2) as cl_product_revenue_home,
round(sum(case when lower(myd_cat_lv1) = "appliances" then product_revenue end),2) as cl_product_revenue_appliances,
round(sum(case when lower(myd_cat_lv1) = "vehicles & parts" then product_revenue end),2) as cl_product_revenue_vehicle
from customer_base 
where customer_id in (select customer_id from onep_new_customers)
group by all 
)
,onep_rp_stats as (
select 
customer_id,
count(distinct paymentreciept_id) as rp_transactions,
round(sum(product_revenue),2) as rp_product_revenue,
round(sum(case when external_seller_id in(47684,44456) then product_revenue end),2) as rp_1p_product_revenue,
round(sum(case when external_seller_id not in(47684,44456) then product_revenue end),2) as rp_3p_product_revenue,
count(distinct case when external_seller_id in(47684,44456) then product_id end) as rp_1p_unique_products,
count(distinct case when external_seller_id not in(47684,44456) then product_id end) as rp_3p_unique_products,
round(sum(case when lower(myd_cat_lv1) = "fashion" then product_revenue end),2) as rp_product_revenue_fashion,
round(sum(case when lower(myd_cat_lv1) = "furniture" then product_revenue end),2) as rp_product_revenue_furniture,
round(sum(case when lower(myd_cat_lv1) = "electronics" then product_revenue end),2) as rp_product_revenue_electronics,
round(sum(case when lower(myd_cat_lv1) = "toys & games" then product_revenue end),2) as rp_product_revenue_toys,
round(sum(case when lower(myd_cat_lv1) = "business & industrial" then product_revenue end),2) as rp_product_revenue_business,
round(sum(case when lower(myd_cat_lv1) = "sports & outdoors" then product_revenue end),2) as rp_product_revenue_sports,
round(sum(case when lower(myd_cat_lv1) = "books & media" then product_revenue end),2) as rp_product_revenue_books,
round(sum(case when lower(myd_cat_lv1) = "hobbies & entertainment" then product_revenue end),2) as rp_product_revenue_entertainment,
round(sum(case when lower(myd_cat_lv1) = "food & beverages" then product_revenue end),2) as rp_product_revenue_food,
round(sum(case when lower(myd_cat_lv1) = "baby & kids" then product_revenue end),2) as rp_product_revenue_baby,
round(sum(case when lower(myd_cat_lv1) = "health & beauty" then product_revenue end),2) as rp_product_revenue_health,
round(sum(case when lower(myd_cat_lv1) = "tools & equipment" then product_revenue end),2) as rp_product_revenue_tools,
round(sum(case when lower(myd_cat_lv1) = "home & garden" then product_revenue end),2) as rp_product_revenue_home,
round(sum(case when lower(myd_cat_lv1) = "appliances" then product_revenue end),2) as rp_product_revenue_appliances,
round(sum(case when lower(myd_cat_lv1) = "vehicles & parts" then product_revenue end),2) as rp_product_revenue_vehicle
from customer_base 
where customer_id in (select customer_id from onep_new_customers)
and first_transaction_contains_1p <> 1
group by all 
) 


select 
case when cts.latest_transaction_date<=cts.first_transaction_date then "New Customer" else "Returning Customer" end as customer_type,
ocs.customer_id,
cts.first_transaction_date,
cts.latest_transaction_date,
ocs.cl_transactions,
ocs.cl_product_revenue,
round(safe_divide(ocs.cl_product_revenue,ocs.cl_transactions),2) as cl_aov,
ocs.cl_1p_product_revenue,
round(safe_divide(ocs.cl_1p_product_revenue,ocs.cl_transactions),2) as cl_1p_aov,
ocs.cl_3p_product_revenue,
round(safe_divide(ocs.cl_3p_product_revenue,ocs.cl_transactions),2) as cl_3p_aov,
ocs.cl_transactions_1p_only as cl_transactions_1p_only,
ocs.cl_1p_product_revenue_1p_only as cl_1p_product_revenue_1p_only,
ocs.cl_1p_unique_products,
ocs.cl_3p_unique_products,
ors.rp_transactions,
ors.rp_product_revenue,
round(safe_divide(ors.rp_product_revenue,ors.rp_transactions),2) as rp_aov,
ors.rp_1p_product_revenue,
round(safe_divide(ors.rp_1p_product_revenue,ors.rp_transactions),2) as rp_1p_aov,
ors.rp_3p_product_revenue,
round(safe_divide(ors.rp_product_revenue,ors.rp_transactions),2) as rp_3p_aov,
ors.rp_1p_unique_products,
ors.rp_3p_unique_products,
ocs.cl_product_revenue_fashion,
ocs.cl_product_revenue_furniture,
ocs.cl_product_revenue_electronics,
ocs.cl_product_revenue_toys,
ocs.cl_product_revenue_business,
ocs.cl_product_revenue_sports,
ocs.cl_product_revenue_books,
ocs.cl_product_revenue_entertainment,
ocs.cl_product_revenue_food,
ocs.cl_product_revenue_baby,
ocs.cl_product_revenue_health,
ocs. cl_product_revenue_tools,
ocs.cl_product_revenue_home,
ocs.cl_product_revenue_appliances,
ocs.cl_product_revenue_vehicle,
ors.rp_product_revenue_fashion,
ors.rp_product_revenue_furniture,
ors.rp_product_revenue_electronics,
ors.rp_product_revenue_toys,
ors.rp_product_revenue_business,
ors.rp_product_revenue_sports,
ors.rp_product_revenue_books,
ors.rp_product_revenue_entertainment,
ors.rp_product_revenue_food,
ors.rp_product_revenue_baby,
ors.rp_product_revenue_health,
ors.rp_product_revenue_tools,
ors.rp_product_revenue_home,
ors.rp_product_revenue_appliances,
ors.rp_product_revenue_vehicle
from 
onep_cl_stats as ocs 
left join onep_rp_stats as ors 
on(ocs.customer_id = ors.customer_id)
left join customer_transaction_stats as cts 
on(ocs.customer_id = cts.customer_id) 
