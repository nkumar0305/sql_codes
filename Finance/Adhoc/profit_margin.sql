/* 
Request : Rolling 52 weeks GP margin 

*/


declare lead_gen_rev array<struct<month_year string, revenue float64>>;

set lead_gen_rev = 

[struct<month_year string, revenue float64>('March-2024', 9743.94), 
struct<month_year string, revenue float64>('April-2024', 14049.56),
struct<month_year string, revenue float64>('May-2024', 13069.96),
struct<month_year string, revenue float64>('June-2024', 15663.48),
struct<month_year string, revenue float64>('July-2024', 9429.88),
struct<month_year string, revenue float64>('August-2024', 9324.43),
struct<month_year string, revenue float64>('September-2024', 8872.72),
struct<month_year string, revenue float64>('October-2024', 10059.52),
struct<month_year string, revenue float64>('November-2024', 15263.99),
struct<month_year string, revenue float64>('December-2024', 13283.42),
struct<month_year string, revenue float64>('January-2025', 8950.45),
struct<month_year string, revenue float64>('February-2025', 7031.12)
];


with lead_gen_rev_cal as (
select 
lead_gen_rev.month_year,
lead_gen_rev.revenue
from unnest(lead_gen_rev) as lead_gen_rev
),

base as (
select
format_date('%B-%Y',sale_date) as sale_month,
extract(month from sale_date) as sale_month_counter,
round(sum(if(total_saleprice_exgst>0,total_saleprice_exgst,0))-sum(if(total_saleprice_exgst<0 or ordertype in(9,10,11),0,net_amount_exgst)),2) as ttv_commission,
round(sum(if(ordertype = 9,commission_fee/1.1,0)),2) as transaction_fee_based_commission_exgst,
round(sum(if(ordertype = 10,commission_fee/1.1,0)),2) as subscription_fee_based_commission_exgst,
round(sum(if(ordertype = 11,commission_fee/1.1,0)),2) as refund_fee_based_commission_exgst,
round(sum(if(ordertype_text = "RefundMoney",commission_fee/1.1,0)),2) as refund_exgst,
round(sum(if(ordertype_text = "FreightProtection",commission_fee/1.1,0)),2) as freightprotection_exgst,
round(sum(if(ordertype_text = "CouponApplied" ,total_saleprice_exgst,0)),2) as coupon_sales_exgst,
round(sum(if(total_saleprice_exgst>0 and ordertype_text <> "FreightProtection",total_saleprice_exgst,0)),2) as gmv_exgst,
round(sum(if(total_saleprice_exgst>0,total_saleprice_exgst,0)),2) as gtv_exgst
from
(
select
date(bd.acceptedtime) as sale_date,
bd.acceptedtime,
bd.biddealid, 
paymentreciepttype,
c.companyid,
c.description,
bd.ordertype,
bd.totalamount,
bd.qty,
bd.companypayprice,
(bd.customerprice-bd.companypayprice)*bd.qty as commission_fee,
bd.totalamount/1.1 as total_saleprice_exgst,
if(c.registeredforgst = true or bd.ordertype in (9, 10, 11), bd.companypayprice * qty / 1.1, bd.companypayprice * qty) as net_amount_exgst,
case
when bd.ordertype = 6 and refundtype = 0 and totalamount < 0 then 'CouponApplied' 
when bd.ordertype = 6 and refundtype = 0 and totalamount > 0 then 'CoupoonReversed' 
when bd.ordertype = 3 and refundtype = 0 and totalamount < 0 then 'CreditApplied'
when bd.ordertype = 3 and refundtype = 0 and totalamount > 0 then 'CreditIssued'
when bd.ordertype = 0 and refundtype = 0 and isfreightprotection = true then 'FreightProtection'
when bd.ordertype = 0 and refundtype = 4 then 'RefundMoney'
when bd.ordertype = 0 and refundtype = 3 then 'RefundCredit'
when bd.ordertype = 1 and refundtype = 0 then 'AddOn'
when bd.ordertype = 2 and refundtype = 0 then 'Special / FreightQuote'
when bd.ordertype = 12 and refundtype = 0 and totalamount < 0 then 'MyMoneyApplied'
when bd.ordertype = 12 and refundtype = 0 and totalamount > 0 then 'MyMoneyReinstated'
when bd.ordertype = 0 and refundtype = 0 then 'Normal'
when bd.ordertype = 0 and refundtype = 1 then 'Normal'
when bd.ordertype = 0 and refundtype = 2 then 'Normal'
when bd.ordertype = 5 and refundtype = 0 then 'SupplierDeduction'
when bd.ordertype = 4 and refundtype = 0 then 'SupplierPayment'
when bd.ordertype = 9 and refundtype = 0 then 'TransactionFee'
when bd.ordertype = 10 and refundtype = 0 then 'SubscriptionFee'
when bd.ordertype = 11 and refundtype = 0 then 'RefundFee'
else 'Missing' end as ordertype_text,
from `mydeal-bigquery.sql_server_rds_dbo.biddeal` bd
left join `mydeal-bigquery.sql_server_rds_dbo.recieptitems` ri 
on(bd.biddealid = ri.biddealid)
left join (select paymentrecieptid,customermarketplace, paymentreciepttype from `mydeal-bigquery.sql_server_rds_dbo.paymentreciept` group by 1,2,3) as p 
on(ri.paymentrecieptid = p.paymentrecieptid)
left join `mydeal-bigquery.sql_server_rds_dbo.company` c 
on (bd.companyid = c.companyid)
where (date(bd.acceptedtime) between date_sub("2025-02-28",interval 52 week)and "2025-02-28") 
and status in (3,4)
and p.customermarketplace is null 

) ab

group by all 
) 

select 
*,
safe_subtract(safe_add(ttv_commission,other_incomes),other_deductions) as gross_profit,
safe_divide(safe_subtract(safe_add(ttv_commission,other_incomes),other_deductions),safe_add(gtv_exgst,other_incomes)) as profit_margin
from(
select 
sale_month,
sale_month_counter,
ttv_commission,
gtv_exgst,
round(safe_divide(ttv_commission,gtv_exgst),3)*100 as margin,
round(revenue + freightprotection_exgst + transaction_fee_based_commission_exgst + subscription_fee_based_commission_exgst+ refund_fee_based_commission_exgst,2) as other_incomes,
round(safe_add(abs(refund_exgst),abs(coupon_sales_exgst)),2) as other_deductions,
transaction_fee_based_commission_exgst,
subscription_fee_based_commission_exgst,
refund_fee_based_commission_exgst,
refund_exgst,
freightprotection_exgst,
coupon_sales_exgst,
gmv_exgst,
revenue as lead_gen_revenue
from base 
left join lead_gen_rev_cal as lgr
on(base.sale_month = lgr.month_year)
)
order by right(sale_month,4) asc,sale_month_counter asc
