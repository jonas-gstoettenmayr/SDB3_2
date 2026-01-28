select * from orders
order by price_per_unit desc
limit 3;

select product_category, sum(quantity) as quantity
from orders
group by product_category
order by sum(quantity) desc
limit 3;

select product_category, sum(quantity * price_per_unit) as revenue
from orders
group by product_category;

select customer_name, sum(quantity * price_per_unit) as revenue
from orders
group by customer_name
order by revenue desc
limit 5;