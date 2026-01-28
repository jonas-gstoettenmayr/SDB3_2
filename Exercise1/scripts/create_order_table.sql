DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
id SERIAL PRIMARY KEY,
customer_name TEXT,
product_category TEXT,
quantity integer,
price_per_unit NUMERIC(18, 2),
order_date date,
country TEXT
);

\COPY orders(customer_name,product_category,quantity,price_per_unit,order_date,country) FROM '/data/orders_1M.csv' DELIMITER ',' CSV HEADER;

\timing on

SELECT COUNT(*) FROM orders;
SELECT * FROM orders LIMIT 10;