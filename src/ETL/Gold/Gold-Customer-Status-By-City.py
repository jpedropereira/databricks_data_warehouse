# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import col, rank, sum, count
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../CommonFunctions" 

# COMMAND ----------

# MAGIC %run "../Configs" 

# COMMAND ----------

spark.catalog.setCurrentDatabase(f"{DATABASE_NAME}")

# COMMAND ----------

authenticate_to_storage()

# COMMAND ----------

#Read required tables
silver_addresses_df = DeltaTable.forName(spark, "silver_addresses_table").toDF().withColumnRenamed("id", "address_id").withColumnRenamed("created_on", "address_created_on").drop("silver_ingestion_time")
silver_customers_df = DeltaTable.forName(spark, "silver_customers_table").toDF().withColumnRenamed("id", "customer_id").withColumnRenamed("created_on", "customer_created_on").drop("silver_ingestion_time")
silver_orders_df = DeltaTable.forName(spark, "silver_orders_table").toDF().withColumnRenamed("id", "order_id").withColumnRenamed("created_on", "order_created_on").drop("silver_ingestion_time")

# COMMAND ----------

# Rank orders for each customer based on timestamp
customer_window = Window.partitionBy("customer_id").orderBy(col("order_created_on").desc())
latest_orders_df = silver_orders_df.withColumn("customer_order_rank", rank().over(customer_window)).filter(col("customer_order_rank") == 1).drop("customer_order_rank")

# COMMAND ----------

# Join silver_customers_df with silver_orders_df for the address id
join_customers_latest_orders_df = silver_customers_df.join(latest_orders_df.select('customer_id', 'address_id'), on="customer_id", how="left")

# COMMAND ----------

# Join with silver_addresses_df for city data
join_customers_addresses_df = join_customers_latest_orders_df.join(silver_addresses_df, on="address_id", how="left")

# COMMAND ----------

#Group number of customers by city and status
gold_customer_status_by_city_df = join_customers_addresses_df.select( "city", "status").groupBy("city", "status").agg(count("*").alias("customer_count"))

# COMMAND ----------

gold_customers_count  = gold_customer_status_by_city_df.select("customer_count").groupBy().sum().collect()[0][0]
silver_customers_count = silver_customers_df.count()

# COMMAND ----------

if silver_customers_count != gold_customers_count:
    raise AssertionError(f"The records in silver_customers_table differs from the sum of customers_count in gold_customer_status_by_city_df. There are {silver_customers_count} customers in silver_customers_table and {gold_customers_count} in gold_customer_status_by_city_df.")

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("gold_customer_status_by_city_table") \
    .addColumns(gold_customer_status_by_city_df.schema) \
    .location(GOLD_CUSTOMER_STATUS_BY_CITY_PATH) \
    .execute()

# COMMAND ----------

gold_customer_status_by_city_table = DeltaTable.forPath(spark, GOLD_CUSTOMER_STATUS_BY_CITY_PATH)

# COMMAND ----------

gold_customer_status_by_city_table.alias('ingested_data') \
  .merge(
      source = gold_customer_status_by_city_df.alias("updates"),
      condition = expr("ingested_data.city = updates.city AND ingested_data.status = updates.status")
  ) \
  .whenMatchedUpdate(set = build_insert_columns_dict(gold_customer_status_by_city_df.columns, "updates")) \
  .whenNotMatchedInsert(values = build_insert_columns_dict(gold_customer_status_by_city_df.columns, "updates")) \
  .execute()

# COMMAND ----------

gold_table_df = DeltaTable.forName(spark, "gold_customer_status_by_city_table").toDF()
gold_table_count  = gold_table_df.select("customer_count").groupBy().sum().collect()[0][0]


if silver_customers_count != gold_table_count:
    raise AssertionError(f"The records in silver_customers_table differs from the sum of customers_count in gold_customer_status_by_city_table. There are {silver_customers_count} customers in silver_customers_table and {gold_table_count} in gold_customer_status_by_city_df.")