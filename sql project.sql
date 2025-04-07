create database DataAnalyticsWarehouse;
use dataanalyticswarehouse;

RENAME TABLE `gold.fact_sales` TO gold_fact_sales;
RENAME TABLE `gold.dim_customers` TO gold_dim_customers;
RENAME TABLE `gold.dim_products` TO gold_dim_products;

# analyse sales performance over time as per year
select year(order_date) as order_year, sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold_fact_sales
where order_date is not null 
group by year(order_date) 
order by year(order_date);

# analyse sales performance over time as per months
select month(order_date) as order_month, sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold_fact_sales
where order_date is not null 
group by month(order_date) 
order by month(order_date);

# analyse sales performance over time as per both year and month
select 
date_format(order_date, '%Y-%m-%d') as order_date,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold_fact_sales
where order_date is not null 
group by date_format(order_date, '%Y-%m-%d')
order by date_format(order_date, '%Y-%m-%d');

# calculate the total sales for each month and the running total sales over time
select order_date, total_sales,
sum(total_sales)over(partition by order_date order by order_date) as running_total,
round((avg(avg_price)over(partition by order_date order by order_date)),2) as moving_avg
from( select
date_format(order_date, '%Y-%m-%d') as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold_fact_sales
where order_date is not null
group by date_format(order_date, '%Y-%m-%d')) as temp;

/*  Analyse the yearly performance of products by comparing their sales to both the average sales performance
of the product and previous year's sales  */
with yearly_product_sales as
(
select year(f.order_date) as order_year, p.product_name,
sum(f.sales_amount) as current_sales
from gold_fact_sales f 
left join gold_dim_products p 
on f.product_key = p.product_key
where f.order_date is not null
group by year(f.order_date), p.product_name 
)
select order_year,product_name,current_sales,
round((avg(current_sales)over(partition by product_name)),2) as avg_sales,
current_sales - round((avg(current_sales)over(partition by product_name)),2) as diff_avg,
case when current_sales - round((avg(current_sales)over(partition by product_name)),2) > 0 then 'above average'
     when current_sales - round((avg(current_sales)over(partition by product_name)),2) < 0 then 'below average'
     else 'average' end as avg_change,
lag(current_sales) over (partition by product_name order by order_year) as py_sales,
current_sales - lag(current_sales) over (partition by product_name order by order_year) as diff_of_py,
case when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'increase'
	 when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 then 'decrease'
     else 'no change' end as py_change
from yearly_product_sales
order by product_name,order_year;

# which categories contribute the most to overall sales?
with category_sales as(
select p.category, sum(f.sales_amount) as total_sales
from gold_fact_sales f left join 
gold_dim_products p
using(product_key)
group by p.category )
select category, total_sales,
sum(total_sales) over() as overall_sales,
concat(round((cast(total_sales as float) / sum(total_sales) over()) * 100 ,2), '%') as percentage_of_total
from category_sales
order by total_sales desc;

# segment products into cost ranges and count how many products fall into each segment
with product_segments as (
select product_key, product_name, cost,
case when cost < 100 then 'Below 100'
     when cost between 100 and 500 then '100-500'
     when cost between 500 and 1000 then '500-1000'
     else 'Above 1000' end cost_range	
from gold_dim_products) 
select cost_range, count(product_key) as total_products
from product_segments
group by cost_range
order by total_products desc;

/* group customers based on 3 segments based on their spending behaviour
VIP: at least 12 months of history and spending more than 5000
Regular: at least 12 months of history and spending 5000 or less
New: Lifespan less than 12 months 
Find total number of customers by each group */
with customer_spending as (
select c.customer_key,sum(f.sales_amount) as total_spending,min(order_date) first_order,max(order_date) last_order,
timestampdiff(month, min(order_date), max(order_date)) as lifespan
from gold_fact_sales f left join gold_dim_customers c
using(customer_key)
group by c.customer_key)
select customer_segments, count(customer_key) as total_customers
from(
select customer_key,
case when lifespan >= 12 and total_spending > 5000 then 'VIP'
     when lifespan >= 12 and total_spending <=5000 then 'Regular'
     else 'New' end customer_segments
from customer_spending) as t
group by customer_segments order by total_customers desc;








