# Customer Report

# 1) Base query: Retrive all core columns from the tables gold_dim_customers and gold_fact_sales
create view gold_report_customers as
with base_query as (
# 1) Base query: Retrive all core columns from the tables
select f.order_number, f.product_key, f.order_date, f.sales_amount, f.quantity,
c.customer_key, c.customer_number,concat(c.first_name, ' ', c.last_name) as customer_name,
timestampdiff(year, c.birthdate, curdate()) as age
from gold_fact_sales f left join gold_dim_customers c
using(customer_key)
where order_date is not null),

customer_aggregation as (
# 2) Customer Aggregation: summarizes key metrics at the customer level
select  customer_key, customer_number, customer_name, age,
count(distinct order_number) as total_order,
sum(sales_amount) as total_sales,
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
timestampdiff(month, min(order_date), max(order_date))as lifespan 
from base_query
group by customer_key, customer_number, customer_name, age)

select customer_key, customer_number, customer_name, age,
case when age < 20 then 'Under 20'
     when age between 20 and 29 then '20-29'
     when age between 30 and 39 then '30-39'
     when age between 40 and 49 then '40-49'
     else '50 and above' end as age_group,
case when lifespan >= 12 and total_sales > 5000 then 'VIP'
     when lifespan >= 12 and total_sales <=5000 then 'Regular'
     else 'New' end customer_segments,
last_order_date, timestampdiff(month, last_order_date, curdate()) as recency,
total_order, total_sales, total_products, lifespan,
# compute total average value(AVO)
case when total_order = 0 then '0' 
else total_sales/total_order 
end as  avg_order_value,
# compute average monthly spent
case when lifespan = 0 then total_sales
else total_sales/lifespan 
end as avg_monthly_spent
from customer_aggregation ;

select *
from gold_report_customers;






