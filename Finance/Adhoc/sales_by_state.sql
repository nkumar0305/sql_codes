declare rolling_period_end_date date;
set rolling_period_end_date = "2025-02-28";
---------------------------------------------------------------------------------------------------------------------------------------------------------------
select 

*
from(
select 
format_date("%B-%Y",date(bd.acceptedtime)) as month_year,
case 
    when regexp_contains(lower(postalstate),r"qld|queensland") then "Queensland"
    when regexp_contains(lower(postalstate),r"nsw|newsouthwales|new.south.wales") then "New South Wales"
    when regexp_contains(lower(postalstate),r"vic|victoria") then "Victoria"
    when regexp_contains(lower(postalstate),r"wa|westernaustralia|western.australia") then "Western Australia"
    when regexp_contains(lower(postalstate),r"tas|tasmania") then "Tasmania"
    when regexp_contains(lower(postalstate),r"sa|southaustralia|south.australia") then "South Australia"
    when regexp_contains(lower(postalstate),r"act|australiancapitalterritory|australian.capital.territory") then "Australian Capital Territory"
    when regexp_contains(lower(postalstate),r"nt|northernterritory|northern.territory") then "Northern Territory"
    else "Unknown" end state,
round(sum(case when bd.isfreightprotection = false then totalamount end)/1.1,2) as gmv_exgst,
round(sum(totalamount)/1.1,2) as gtv_exgst,
sum(case when bd.isfreightprotection = false and isfreight = false then qty end) as units_sold
from `mydeal-bigquery.sql_server_rds_dbo.biddeal` as bd
left join `mydeal-bigquery.sql_server_rds_dbo.recieptitems` as ri
on(bd.biddealid = ri.biddealid)
left join (select paymentrecieptid,customermarketplace,paymentreciepttype from `mydeal-bigquery.sql_server_rds_dbo.paymentreciept` group by 1,2,3) as p 
on(ri.paymentrecieptid = p.paymentrecieptid)
where (date(bd.acceptedtime) between date_sub(rolling_period_end_date,interval 52 week)and rolling_period_end_date)
     and status in (3,4)
     and totalamount > 0 
     and customermarketplace is null
     and p.paymentreciepttype in (0,1,2)
group by all 
)
pivot 

(
    sum(gmv_exgst) as gmv_exgst,
    sum(gtv_exgst) as gtv_exgst,
    sum(units_sold) as units_sold
for state in ('Victoria','Western Australia','Australian Capital Territory','Queensland','South Australia','New South Wales','Tasmania','Northern Territory')
)

