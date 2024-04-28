# Databricks notebook source
# MAGIC %run "./Configs" 

# COMMAND ----------

# MAGIC %run "./CommonFunctions"

# COMMAND ----------

spark.catalog.setCurrentDatabase(f"{database}")

# COMMAND ----------

authenticate_to_storage()

# COMMAND ----------

#Request 1. Define TOP-5 cities with the largest number of VIP customers and specify the number of such customers for each of the TOP-5 cities

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM gold_customer_status_by_city_table
# MAGIC WHERE status = 'VIP'
# MAGIC ORDER BY customer_count DESC
# MAGIC LIMIT 5;

# COMMAND ----------

#Request 3. Get the total number of customers in the system as well as their number breakdown by customers’ kinds and types

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(customer_count)
# MAGIC FROM gold_customer_breakdown_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     type,
# MAGIC     status,
# MAGIC     customer_count,
# MAGIC     ROUND(customer_count / SUM(customer_count) OVER (PARTITION BY type) * 100, 2) AS relative_frequency_by_type_status
# MAGIC FROM gold_customer_breakdown_table;
# MAGIC

# COMMAND ----------

#5. Get a list of affiliate customers who made less than 5 orders in a week. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_orders_by_customer_week_table
# MAGIC WHERE year = 2023 AND week = 7;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_orders_by_customer_week_table
# MAGIC WHERE year = 2023 AND week = 7 AND order_count < 5 AND customer_type = 'affiliate';

# COMMAND ----------

#7. Find out the average delivery time to a specific city and the average number of orders to specified cities

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     AVG(order_count) AS avg_orders_month,
# MAGIC     SUM(avg_delivery_time * order_count) / SUM(order_count) AS average_delivery_time
# MAGIC FROM gold_orders_by_city_year_month_table
# MAGIC WHERE city = 'Phoenix';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city, ROUND(AVG(order_count), 2) avg_order_count, ROUND(AVG(avg_delivery_time), 2) avg_delivery_time
# MAGIC FROM gold_orders_by_city_year_month_table
# MAGIC GROUP BY city
# MAGIC ORDER BY city;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city, year, ROUND(AVG(order_count), 2) avg_order_count, ROUND(AVG(avg_delivery_time), 2) avg_delivery_time
# MAGIC FROM gold_orders_by_city_year_month_table
# MAGIC GROUP BY city, year
# MAGIC ORDER BY city, year;

# COMMAND ----------

#8. Get the overall number and share of orders, delivery time of which exceeds 7 days as well as broken down by the order type.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH total_orders AS (
# MAGIC   SELECT type, SUM(order_count) AS total_orders
# MAGIC   FROM gold_orders_type_delivery_time
# MAGIC   GROUP BY 1
# MAGIC ),
# MAGIC   late_orders AS (
# MAGIC     SELECT type, SUM(order_count) AS late_orders
# MAGIC   FROM gold_orders_type_delivery_time
# MAGIC   WHERE delivery_time > 7
# MAGIC   GROUP BY 1
# MAGIC   )
# MAGIC
# MAGIC SELECT t_ord.type AS order_type, 
# MAGIC        t_ord.total_orders,
# MAGIC        l_ord.late_orders,
# MAGIC        l_ord.late_orders/t_ord.total_orders AS late_share
# MAGIC FROM total_orders AS t_ord
# MAGIC JOIN late_orders AS l_ord
# MAGIC ON t_ord.type = l_ord.type;

# COMMAND ----------

