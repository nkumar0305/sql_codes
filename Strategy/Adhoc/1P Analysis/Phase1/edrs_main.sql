with remove_duplicates_from_order as (
select  
*
from `gcp-wow-ent-im-wowx-cust-prod.adp_wowx_dm_ecomdp_nrt_online_analytics_view.nrt_edd_order_master_v` 
qualify row_number() over (partition by orderNumber,edrArticleNumber order by date(orderDate) desc) = 1
)
, 
customer_transaction_stats as (
select 
crn,
min(date(orderDate)) as first_transaction_date,
max(date(orderDate)) as latest_transaction_date
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
string_agg((case when seller_id in(47684,44456) then "1P" else "3P" end)) over (partition by customer_id,paymentreciept_id order by transaction_date) as company_mix,
dense_rank() over (partition by customer_id order by transaction_date) as transaction_rank,
sum(product_revenue) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_revenue,
count(product_id) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_products,
count(case when seller_id in(47684,44456) then product_id end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_1p_products,
count(case when seller_id not in(47684,44456) then product_id end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_3p_products,
sum(case when seller_id in(47684,44456) then product_revenue end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction_revenue_1p_products,
sum(case when seller_id not in(47684,44456) then product_revenue end) over (partition by customer_id,paymentreciept_id order by transaction_date) as transaction__revenue_3p_products
from(
select 
crn as customer_id,
orderNumber as paymentreciept_id,
date(orderDate) as transaction_date,
rmd.edrArticleNumber as product_id,
rmd.productName as product_name,
cast(rmd.sellerId as int64) as seller_id,
rmd.sellerName as seller_name,
sum(lineTotal) as product_revenue
from remove_duplicates_from_order as rmd
where crn in (select crn from customer_transaction_stats )
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
round(sum(case when seller_id in(47684,44456) then product_revenue end),2) as cl_1p_product_revenue,
round(sum(case when seller_id not in(47684,44456) then product_revenue end),2) as cl_3p_product_revenue,
count(distinct product_id ) as cl_unique_products,
count(distinct case when seller_id in(47684,44456) then product_id end) as cl_1p_unique_products,
count(distinct case when seller_id not in(47684,44456) then product_id end) as cl_3p_unique_products,
count(distinct case when first_transaction_1p_only =1 then paymentreciept_id end) as cl_transactions_1p_only,
round(sum(case when seller_id in(47684,44456) and first_transaction_1p_only =1 then product_revenue end),2) as cl_1p_product_revenue_1p_only
from customer_base 
where customer_id in (select customer_id from onep_new_customers)
group by all 
)
,onep_rp_stats as (
select 
customer_id,
count(distinct paymentreciept_id) as rp_transactions,
round(sum(product_revenue),2) as rp_product_revenue,
round(sum(case when seller_id in(47684,44456) then product_revenue end),2) as rp_1p_product_revenue,
round(sum(case when seller_id not in(47684,44456) then product_revenue end),2) as rp_3p_product_revenue,
count(distinct case when seller_id in(47684,44456) then product_id end) as rp_1p_unique_products,
count(distinct case when seller_id not in(47684,44456) then product_id end) as rp_3p_unique_products
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
from 
onep_cl_stats as ocs 
left join onep_rp_stats as ors 
on(ocs.customer_id = ors.customer_id)
left join customer_transaction_stats as cts 
on(ocs.customer_id = cts.crn) 
