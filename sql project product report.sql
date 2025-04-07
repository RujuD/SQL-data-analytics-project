# product report

# sql view to provide product insights

# 1) Base query: Retrive all core columns from the tables gold_dim_products and gold_fact_sales

create view gold_report_products as 
with base_query as (
# 1) Base query: Retrive all core columns from the tables gold_dim_products and gold_fact_sales
select f.order_number, f.order_date, f.customer_key, f.sales_amount, f.quantity,
p.product_key, p.product_name, p.category, p.subcategory, p.cost
from gold_fact_sales f left join gold_dim_products p
using(product_key)
where order_date is not null),

product_aggregation as (
# 2) Product Aggregation: summarizes key metrics at the product level
select  product_key, product_name, category, subcategory, cost,
timestampdiff(month, min(order_date), max(order_date))as lifespan,
max(order_date) as last_sale_date,
count(distinct order_number) as total_order,
count(distinct customer_key) as total_customers,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
round(avg(cast(sales_amount as decimal(10,2)) / Nullif(quantity,0)),1)as avg_selling_price 
from base_query
group by product_key, product_name, category, subcategory,cost)

# 3) Final query: combine all product results into one output
select  product_key, product_name, category, subcategory, cost, last_sale_date,
timestampdiff(month, last_sale_date, curdate())as recency_in_months,
case when total_sales > 50000 then 'high performer'
     when total_sales >= 10000 then 'mid range'
     else 'low performer' end as product_segment, 
     lifespan, total_order, total_sales, total_quantity, total_customers, avg_selling_price,
# Average order revenue (AOR)
case when total_order = 0 then 0
else total_sales/total_order end as avg_order_revenue,
# Average monthly revenue
case when lifespan = 0 then total_sales
else total_sales/lifespan 
end as avg_monthly_revenue
from product_aggregation;

select * from gold_report_products;
