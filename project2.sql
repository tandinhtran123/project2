--1
select 
format_date('%Y %m', created_at) as month_year,
count(user_id) as total_user,
count(order_id) as total_order
from bigquery-public-data.thelook_ecommerce.orders
where status in ('Complete')
and format_date('%Y %m', created_at) >='2019 01' 
and format_date('%Y %m', created_at) <='2020 04'
group by 1
order by 1
--2
select
format_date('%Y %m', created_at) as month_year,
count(distinct user_id) as distinct_users,
(sum(sale_price)/count(order_id)) as avg_order_value
from bigquery-public-data.thelook_ecommerce.order_items
where format_date('%Y %m', created_at) >='2019 01' 
and format_date('%Y %m', created_at) <='2020 04'
group by 1
order by 1
--3
with cte as
(select id,
age,
row_number()over(order by age) as rk
from bigquery-public-data.thelook_ecommerce.users)
select count(age) as count_users,
age as youngest_oldest
from bigquery-public-data.thelook_ecommerce.users
where age=12
group by 2
union all
select count(age),
age
from bigquery-public-data.thelook_ecommerce.users
where age=70
group by 2
--4
with cte as 
(select
format_date('%Y %m', created_at) as month_year,
product_id,
product_category,
product_name,
cost,
product_retail_price as sales
from bigquery-public-data.thelook_ecommerce.inventory_items),
cte2 as
(select *,
(sales-cost) as profit,
dense_rank()over(partition by month_year order by (sales-cost)desc) as rank
from cte)
select * from cte2
where
rank <=5
order by month_year
--5
with cte as
(select 
format_date('%Y %m %d',a.created_at) as dates,
b.category as product_categories,
sum(sale_price) as revenue
from bigquery-public-data.thelook_ecommerce.order_items a
join bigquery-public-data.thelook_ecommerce.products b 
on a.id=b.id
where a.status in ('Complete')
group by 1,2)
select *
from cte
where dates between '2022 04 15' and '2022 07 15'
order by dates

--dataset
create view tabvw_ecommerce_analyst  as 
(with cte as
(Select
format_date('%Y %m',a.created_at) as month,
extract(year from a.created_at) as year,
count(c.order_id) as TPO,
sum(c.sale_price) as TPV,
sum(b.cost) as total_cost,
(sum(c.sale_price)-sum(b.cost)) as total_profit
from bigquery-public-data.thelook_ecommerce.products b
join bigquery-public-data.thelook_ecommerce.order_items c
on c.product_id=b.id
join bigquery-public-data.thelook_ecommerce.orders a
on a.order_id=c.order_id
where c.status in ('Complete')
Group by 1,2
order by month),

ct2 as
(select *,
lead(TPV)over(order by month,year) as next_TPV,
lead(TPO)over(order by month,year) as next_TPO
from cte
order by month)

select month,year,TPO,TPV,total_cost,total_profit,
((next_TPV-TPV)/TPV)*100.0||'%' as revenue_growth,
((next_TPO-TPO)/TPO)*100||'%' as order_growth,
(total_profit/total_cost) as profit_to_cost_ratio
from ct2)
