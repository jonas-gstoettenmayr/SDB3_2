# Assignment 1 - Jonas Gstoettenmayr

My **repo:** [https://github.com/jonas-gstoettenmayr/SDB3_2](https://github.com/jonas-gstoettenmayr/SDB3_2)

This contains the ipynb as well as the .sql files for the queries

## Part 2 - EX 1

### Making the new table

\i 'scripts/create_order_table.sql'

### Answering the questions:

\i 'scripts/part2_questions.sql'

**A.** What is the single item with the highest `price_per_unit`?

   id   | customer_name | product_category | quantity | price_per_unit | order_date | country
--------|---------------|------------------|----------|----------------|------------|---------
 841292 | Emma Brown    | Automotive       |        3 |      2000.00 | 2024-10-11 | Italy

**B.** What are the top 3 products with the highest total quantity sold across all orders?

 product_category |  quantity
------------------|--------
 Health & Beauty  | 300842
 Electronics      | 300804
 Toys             | 300598

**C.** What is the total revenue per product category?  
(Revenue = `price_per_unit × quantity`)

 product_category |     revenue
------------------|-----------------
 Automotive       | 306589798.86
 Books            |  12731976.04
 Electronics      | 241525009.45
 Fashion          |  31566368.22
 Grocery          |  15268355.66
 Health & Beauty  |  46599817.89
 Home & Garden    |  78023780.09
 Office Supplies  |  38276061.64
 Sports           |  61848990.83
 Toys             |  23271039.02

**D.** Which customers have the highest total spending?

 customer_name  |   revenue
----------------|-------------
 Carol Taylor   | 991179.18
 Nina Lopez     | 975444.95
 Daniel Jackson | 959344.48
 Carol Lewis    | 947708.57
 Daniel Young   | 946030.14

## Part 2 - EX 2

### Discussion Question

Considering the requirements for **scalability** and **efficiency**, what **approaches and/or optimizations** can be applied to improve the system?

- Scalability  
  - Option 1: The nuclear option\
   Replace the entire DB system and our interface\
   Since our database is not scalable, schedual downtime if possible and replace it with a scalable system
    - For this system we can impliment technology for scalable DB systems, like sharding and replication
    - With this we can scale our database to as many
    - We can plan it better and future proof it this time
  - Option 2: The procrastination option\
    Vertical scaling, we buy a bigger server with more CPU power. It's the future teams problem once it runs into the next performance wall.
- Performance  
  - Option 1: The good option\
    We use indexes to speed up joins, if we know the most frequent queries we can use a primary index to sort by the most impactfull part.\
    We can also use more specific indexes to enable more efficient joins, like merge joins where the DB just concatinates as both are already sorted in the same manner (much more efficient)
  - Option 2: accidential solve \
  (vertical) scalling also improves performance
  - Option 3: The nuclear option, part 2\
  We change our DB schema to be non-normalise eliminating joins entirely, thus speeding up the queries
- Overall efficiency
  - The already mentioned Options
  - Alternative: We create physical DB views so that no joins have to be performed, the views are updated periodicly, or depending on the amount of writes, whenever a new write is recieved by the DB.

## Part 2 - EX 3

### Analysis and Discussion

Now, explain in your own words:

- **What the Spark code does:**  
  Describe the workflow, data loading, and the types of queries executed (aggregations, sorting, self-joins, etc.).

  1. First a spark session is started and configured
  1. Then we use spark with jdbc to connect to the DB
  1. We count and print the amount of rows
  1. For every query we measure how long the query takes and print it out
  1. First query (a) is an aggregadion, the average salary per department, limited to the 10 departments, orderd reverse alphabetically
  1. Second query (b), executes an SQL query not over the spark interface but through a SQL command in text. 
  In this query we first execute a subquery then query the result of the subquery.\
  The subquery gets the average salary per department in each country, when than takt the average salary of the average department salary per country.
  I.e. average salary per country, we only view the top 10 highest
  1. Third query (c), is just the top 10 highest payed people and their information
  1. Fourth query (d), is the table joined with itself on the country, as such for every entry belonging to a country the joined table creates a row for EVERY entry belonging to that country so country²+country²+... amount of rows, we only print the amount of rows.
  1. Fith query (d-safe), here we do the same thing as in the fourth query just smarter, first we count how many rows belong to each country, than we sum up the squares of those amounts 

- **Architectural contrasts with PostgreSQL:**  
  Compare the Spark distributed architecture versus PostgreSQL’s single-node capabilities, including scalability, parallelism, and data processing models.

  - PostgreSQL: vertical scalling (is a relational DBMS, so not horizontially scalable but has ACID)on a single server for reliable transactions\
  Spark: scales horizontally across a cluster for massive data processing.
  - PostgreSQL: parallelism is restricted to the hardware (amount of CPU cores) of **one** machine\
  Spark: can partition data (uses batches/streaming), and run tasks in parallel across **many** machines.
  - PostgreSQL: focused on row-based consistency/updates (ACID)\
  Spark: is a in-memory distributed model, it is optimized for high-speed analytics

- **Advantages and limitations:**  
  Highlight the benefits of using Spark for large-scale data processing (e.g., in-memory computation, distributed processing) and its potential drawbacks (e.g., setup complexity, overhead for small datasets).\
  **Benefits:**
  - in-memory means it's A LOT faster
  - distributed nature means its very easy to scale
  - it is a unified engine for SQL, machine learning and streaming

  **Downsides**
  - in-memory means we need a lot of memory, which requires a hefty budged (especially nowadays thanks openAI :C)
  - distributed nature means that it is quite a lot more complex and harder to setup, it also becomes much harder to set it up "locally" on company hardware
  - distributed nature mans that we have A LOT of network trafic, which could actually make it slower for certain queries where it has to gather data form multiple servers
  - for small qeueries there is a rather big (in proportion) network overhead
  - the complexity is quite a bit higher than "normal" SQL and thus has a rather large learning curve

- **Relation to Exercise 2:**  
  Connect this approach to the concepts explored in Exercise 2, such as performance optimization and scalability considerations.

  We could use this as the improvment I spoke of in the "nuclear option" where we replace our old standard one-server SQL apprach (presumibly) with apache spark, this would only really work after much planning, consideration and most likly testing (if this is too complicated for our usecase or just enough) and maybe even the hiring of a new employee who is skilled in setting up these systems, or training for our own DB team.

  This would significantly **improve our performance** as we could **scale horizontially**.

### Part 2 - EX 4

Can also be found in the notebooks folder in the repo

```python
# ============================================
# 0. Imports & Spark session
# ============================================

import time
import builtins
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    round as spark_round,   # Spark round ONLY for Columns
    count,
    col,
    sum as _sum,
    product
)

spark = (
    SparkSession.builder
    .appName("PostgresVsSparkBenchmark")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/tmp/spark-events")
    .config("spark.history.fs.logDirectory", "/tmp/spark-events")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================
# 1. JDBC connection config
# ============================================

jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
jdbc_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# ============================================
# 2. Load data from PostgreSQL
# ============================================

print("\n=== Loading orders from PostgreSQL ===")

start = time.time()

df_order = spark.read.jdbc(
    url=jdbc_url,
    table="orders",
    properties=jdbc_props
)

# Force materialization
row_count = df_order.count()

print(f"Rows loaded: {row_count}")
print("Load time:", builtins.round(time.time() - start, 2), "seconds")

# Register temp view
df_order.createOrReplaceTempView("orders")

df_order.printSchema()

print("Query (d-safe) time:", builtins.round(time.time() - start, 2), "seconds")

# ============================================
# Query 1 -> A. What is the single item with the highest price_per_unit?
# ============================================

print("\n=== Query (1): Highest unit price ===")

start = time.time()

q_a = (
    df_order
    .orderBy("price_per_unit", ascending=False)
    .limit(3)
)

q_a.collect()
q_a.show(truncate=False)
print("Query (a) time:", builtins.round(time.time() - start, 2), "seconds")

# ============================================
# Query 2 -> B. What are the top 3 products with the highest total quantity sold across all orders?
# ============================================

print("\n=== Query (2): Top 3 products ===")

start = time.time()

q_b= (
    df_order
    .groupBy("product_category")
    .agg(_sum("quantity").alias("sum_quantity"))
    .orderBy("sum_quantity", ascending=False)
    .limit(3)
)

q_b.collect()
q_b.show(truncate=False)
print("Query (2) time:", builtins.round(time.time() - start, 2), "seconds")

# ============================================
# Query 3 -> C. What is the total revenue per product category?  
# ============================================

print("\n=== Query (3): Total revenue ===")

start = time.time()

q_c = (
    df_order
    .select(df_order.quantity, df_order.price_per_unit, df_order.product_category)
    .withColumn("revenue", col("quantity") * col("price_per_unit"))
    .groupBy("product_category")
    .agg(_sum("revenue"))
    .orderBy("product_category")
)

q_c.collect()
q_c.show(truncate=False)
print("Query (3) time:", builtins.round(time.time() - start, 2), "seconds")

# ============================================
# Query 4 -> D. Which customers have the highest total spending?
# ============================================

print("\n=== Query (4): Highest spending ===")

start = time.time()

q_c = spark.sql("""
select customer_name, sum(quantity * price_per_unit) as revenue
from orders
group by customer_name
order by revenue desc
limit 5;
""")

q_c.collect()
q_c.show(truncate=False)
print("Query (4) time:", builtins.round(time.time() - start, 2), "seconds")

# ============================================
# Cleanup
# ============================================

spark.stop()
```
