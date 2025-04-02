with customer_transaction_stats as (
/**
Filter customers:
- Who made there first purchase post 1st Jan,2024
- Who made there first purchase in MyDeal
- Remove freight and freight protection as this is at order level not product level
**/
select 
bd.customerid,
min(date(acceptedtime)) as first_transaction_date,
max(date(acceptedtime)) as latest_transaction_date
from `mydeal-bigquery.sql_server_rds_dbo.biddeal` as bd 
left join `mydeal-bigquery.sql_server_rds_dbo.recieptitems` as ri 
on(bd.biddealid = ri.biddealid)
left join (select paymentrecieptid,customermarketplace from `mydeal-bigquery.sql_server_rds_dbo.paymentreciept` group by 1,2) as p 
on(ri.paymentrecieptid = p.paymentrecieptid)
where 
    status in (3,4)
and customermarketplace is null
and totalamount > 0
and isfreight = false 
and isfreightprotection = false
group by all
having first_transaction_date >= "2024-01-01"
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
string_agg((case when company_id in(47684,44456) then "1P" else "3P" end)) over (partition by customer_id,paymentreciept_id order by transaction_date) as company_mix,
dense_rank() over (partition by customer_id order by transaction_date) as transaction_rank,
sum(product_revenue) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_revenue,
count(product_id) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_products,
count(case when company_id in(47684,44456) then product_id end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_1p_products,
count(case when company_id not in(47684,44456) then product_id end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_3p_products,
sum(case when company_id in(47684,44456) then product_revenue end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_revenue_1p_products,
sum(case when company_id not in(47684,44456) then product_revenue end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction__revenue_3p_products,
sum(product_revenue) over (partition by customer_id order by transaction_date) as customer_lifetime_revenue,
count(paymentreciept_id) over (partition by customer_id order by transaction_date) as customer_lifetime_transactions,
from(
select 
bd.customerid as customer_id,
ri.paymentrecieptid as paymentreciept_id,
date(bd.acceptedtime) as transaction_date,
bd.dealid as product_id,
d.maindeal as product_name,
dcs.level1 as myd_cat_lv1,
br.brandname as brandname,
bd.companyid as company_id,
c.description as company_name,
sum(totalamount) as product_revenue
from `mydeal-bigquery.sql_server_rds_dbo.biddeal` as bd 
left join `mydeal-bigquery.sql_server_rds_dbo.recieptitems` as ri 
on(bd.biddealid = ri.biddealid)
left join (select paymentrecieptid,customermarketplace from `mydeal-bigquery.sql_server_rds_dbo.paymentreciept` group by 1,2) as p 
on(ri.paymentrecieptid = p.paymentrecieptid)
left join `mydeal-bigquery.sql_server_rds_dbo.deal` as d 
on(bd.dealid = d.dealid)
left join `mydeal-bigquery.sql_server_rds_dbo.company` as c 
on(d.companyid = c.companyid)
left join customer_transaction_stats as cts
on(bd.customerid = cts.customerid)
left join `mydeal-bigquery.dbt_data_studio.deal_category_seller_mw` as dcs
on(dcs.dealid = d.dealid)
left join `mydeal-bigquery.sql_server_rds_dbo.brand` as br 
on(d.brandid = br.brandid)
where 
    status in (3,4)
and customermarketplace is null
and totalamount > 0
and isfreight = false 
and isfreightprotection = false
and bd.customerid in (select distinct customerid from customer_transaction_stats)
-- Test :and bd.customerid = 11311110
group by all)))
)
,onep_new_customers as (
select 
distinct customer_id
from customer_base 
where first_transaction_contains_1p = 1
)
-- ,onep_top10_brands as (
-- Identify the top performing sku from each brand and order by product revenue desc 
-- select 
-- *
-- from(
-- select 
-- brandname,
-- product_id,
-- product_name,
-- sum(product_revenue) as pr
-- from customer_base 
-- where first_transaction_contains_1p = 1 
-- group by all
-- )
-- qualify row_number() over (partition by brandname order by pr desc) = 1
-- order by pr desc
-- limit 10

-- )

/*
selected skus
Apple - 6797786
Birkenstock - 8524381
Sony - 6754907
Shark - 10800886
Magivaac - 9306781
Winix - 10475408
Midea - 6957623
Healthy Choice - 8413120
DukeLiving - 8086989
Samsung - 8778213
*/
,onep_cl_stats as (
select 
customer_id,
count(distinct paymentreciept_id) as cl_transactions,
round(sum(product_revenue),2) as cl_product_revenue,
round(sum(case when company_id in(47684,44456) then product_revenue end),2) as cl_1p_product_revenue,
round(sum(case when company_id not in(47684,44456) then product_revenue end),2) as cl_3p_product_revenue,
count(distinct product_id ) as cl_unique_products,
count(distinct case when company_id in(47684,44456) then product_id end) as cl_1p_unique_products,
count(distinct case when company_id not in(47684,44456) then product_id end) as cl_3p_unique_products,
count(distinct case when first_transaction_1p_only =1 then paymentreciept_id end) as cl_transactions_1p_only,
round(sum(case when company_id in(47684,44456) and first_transaction_1p_only =1 then product_revenue end),2) as cl_1p_product_revenue_1p_only,
round(sum(case when product_id = 6797786 then product_revenue end),2) as cl_product_revenue_apple_airtags,
round(sum(case when product_id = 8524381 then product_revenue end),2) as cl_product_revenue_birkenstock_8524381,
round(sum(case when product_id = 6754907 then product_revenue end),2) as cl_product_revenue_sony_6754907,
round(sum(case when product_id = 10800886 then product_revenue end),2) as cl_product_revenue_shark_10800886,
round(sum(case when product_id = 9306781 then product_revenue end),2) as cl_product_revenue_magivaac_9306781,
round(sum(case when product_id = 10475408 then product_revenue end),2) as cl_product_revenue_winix_10475408,
round(sum(case when product_id = 6957623 then product_revenue end),2) as cl_product_revenue_midea_6957623,
round(sum(case when product_id = 8413120 then product_revenue end),2) as cl_product_revenue_healthy_choice_8413120,
round(sum(case when product_id = 8086989 then product_revenue end),2) as cl_product_revenue_duke_living_8086989,
round(sum(case when product_id = 8778213 then product_revenue end),2) as cl_product_revenue_samsung_8778213

from customer_base 
where customer_id in (select customer_id from onep_new_customers)
group by all 
),onep_rp_stats as (
select 
customer_id,
count(distinct paymentreciept_id) as rp_transactions,
round(sum(product_revenue),2) as rp_product_revenue,
round(sum(case when company_id in(47684,44456) then product_revenue end),2) as rp_1p_product_revenue,
round(sum(case when company_id not in(47684,44456) then product_revenue end),2) as rp_3p_product_revenue,
count(distinct case when company_id in(47684,44456) then product_id end) as rp_1p_unique_products,
count(distinct case when company_id not in(47684,44456) then product_id end) as rp_3p_unique_products,
round(sum(case when product_id = 6797786 then product_revenue end),2) as rp_product_revenue_apple_airtags,
round(sum(case when product_id = 8524381 then product_revenue end),2) as rp_product_revenue_birkenstock_8524381,
round(sum(case when product_id = 6754907 then product_revenue end),2) as rp_product_revenue_sony_6754907,
round(sum(case when product_id = 10800886 then product_revenue end),2) as rp_product_revenue_shark_10800886,
round(sum(case when product_id = 9306781 then product_revenue end),2) as rp_product_revenue_magivaac_9306781,
round(sum(case when product_id = 10475408 then product_revenue end),2) as rp_product_revenue_winix_10475408,
round(sum(case when product_id = 6957623 then product_revenue end),2) as rp_product_revenue_midea_6957623,
round(sum(case when product_id = 8413120 then product_revenue end),2) as rp_product_revenue_healthy_choice_8413120,
round(sum(case when product_id = 8086989 then product_revenue end),2) as rp_product_revenue_duke_living_8086989,
round(sum(case when product_id = 8778213 then product_revenue end),2) as rp_product_revenue_samsung_8778213

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
ocs.cl_product_revenue_apple_airtags,
ocs.cl_product_revenue_birkenstock_8524381,
ocs.cl_product_revenue_sony_6754907,
ocs.cl_product_revenue_shark_10800886,
ocs.cl_product_revenue_magivaac_9306781,
ocs.cl_product_revenue_winix_10475408,
ocs.cl_product_revenue_midea_6957623,
ocs.cl_product_revenue_healthy_choice_8413120,
ocs.cl_product_revenue_duke_living_8086989,
ocs.cl_product_revenue_samsung_8778213,
ors.rp_product_revenue_apple_airtags,
ors.rp_product_revenue_birkenstock_8524381,
ors.rp_product_revenue_sony_6754907,
ors.rp_product_revenue_shark_10800886,
ors.rp_product_revenue_magivaac_9306781,
ors.rp_product_revenue_winix_10475408,
ors.rp_product_revenue_midea_6957623,
ors.rp_product_revenue_healthy_choice_8413120,
ors.rp_product_revenue_duke_living_8086989,
ors.rp_product_revenue_samsung_8778213
from 
onep_cl_stats as ocs 
left join onep_rp_stats as ors 
on(ocs.customer_id = ors.customer_id)
left join customer_transaction_stats as cts 
on(ocs.customer_id = cts.customerid) 
