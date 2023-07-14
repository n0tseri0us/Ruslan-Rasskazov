with 
first_payments as (
select 
user_id,
min(transaction_datetime)::date as first_payment_date
from skyeng_db.payments 
where status_name = 'success'
group by user_id
)
,all_dates as (
select distinct
date_trunc('day', class_start_datetime)::date as dt
from skyeng_db.classes
where date_trunc('year', class_start_datetime) = '2016-01-01 00:00:00'
)
,all_dates_by_user as (
select 
fp.user_id,
ad.dt 
from first_payments fp 
inner join all_dates ad 
on fp.first_payment_date <= ad.dt
order by fp.user_id,ad.dt
)
,payments_by_dates as (
select 
user_id,
date_trunc('day', transaction_datetime)::date as payment_date,
sum(classes) as transaction_balance_change
from skyeng_db.payments 
where status_name = 'success'
group by 
user_id,
date_trunc('day', transaction_datetime)
)
,payments_by_dates_cumsum as (
select 
adu.user_id,
adu.dt,
coalesce(pbd.transaction_balance_change,0) as transaction_balance_change,
sum(coalesce(pbd.transaction_balance_change,0)) over (partition by adu.user_id order by adu.dt) as transaction_balance_change_cs
from all_dates_by_user adu 
left join payments_by_dates pbd
on adu.user_id = pbd.user_id
and adu.dt = pbd.payment_date
order by 
adu.user_id,
adu.dt
)
,classes_by_dates as (
select 
user_id,
date_trunc('day', class_start_datetime)::date as class_date,
(-1)*count(id_class) as classes
from skyeng_db.classes 
where class_status in ('success','failed_by_student')
and class_type <> 'trial'
group by 
user_id,
date_trunc('day', class_start_datetime)
)
,classes_by_dates_dates_cumsum as (
select 
adu.user_id,
adu.dt,
coalesce(cbd.classes,0) as classes,
sum(coalesce(cbd.classes,0)) over (partition by adu.user_id order by adu.dt) as classes_cs
from all_dates_by_user adu 
left join classes_by_dates cbd
on adu.user_id = cbd.user_id
and adu.dt = cbd.class_date
order by 
adu.user_id,
adu.dt
)
,balances as (
select 
pbd_cum.user_id,
pbd_cum.dt,
pbd_cum.transaction_balance_change,
pbd_cum.transaction_balance_change_cs,
cbd_cum.classes,
cbd_cum.classes_cs,
cbd_cum.classes_cs + pbd_cum.transaction_balance_change_cs as balance
from
payments_by_dates_cumsum as pbd_cum
inner join
classes_by_dates_dates_cumsum cbd_cum
on pbd_cum.user_id = cbd_cum.user_id
and pbd_cum.dt = cbd_cum.dt
order by 
pbd_cum.user_id,
pbd_cum.dt
)

select 
dt,
sum(transaction_balance_change) as sum_transaction,
sum(transaction_balance_change_cs) as sum_transaction_cs,
sum(classes) as sum_classes,
sum(classes_cs) as sum_classes_cs,
sum(balance) as sum_balance
from balances
group by dt 
order by dt
;
