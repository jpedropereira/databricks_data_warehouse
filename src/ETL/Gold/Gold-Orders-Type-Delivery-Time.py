# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import col, avg, sum, count, datediff

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
silver_customers_df = DeltaTable.forName(spark, "silver_customers_table").toDF().withColumnRenamed("id", "customer_id").withColumnRenamed("created_on", "customer_created_on").drop("silver_ingestion_time")
silver_orders_df = DeltaTable.forName(spark, "silver_orders_table").toDF().withColumnRenamed("id", "order_id").withColumnRenamed("created_on", "order_created_on").drop("silver_ingestion_time")

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn("delivery_time", datediff(col("delivered_on"), col("order_created_on")))

# COMMAND ----------

join_orders_customers = silver_orders_df.join(silver_customers_df.select("customer_id", "type"), on="customer_id", how="left")

# COMMAND ----------

gold_orders_type_delivery_time_df = join_orders_customers.select("type","year", "month", "delivery_time", "order_id").groupBy("type", "delivery_time", "year", "month").agg(count("order_id").alias("order_count"))

# COMMAND ----------

gold_orders_count  = gold_orders_type_delivery_time_df.select("order_count").groupBy().sum().collect()[0][0]
silver_orders_count = silver_orders_df.count()

# COMMAND ----------

if silver_orders_count != gold_orders_count:
    raise AssertionError(f"The records in silver_order_table differs from the sum of order_count in gold_orders_type_delivery_time_df. There are {silver_orders_count} orders in silver_customers_table and {gold_orders_count} in gold_orders_type_delivery_time_df.")

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("gold_orders_type_delivery_time") \
    .addColumns(gold_orders_type_delivery_time_df.schema) \
    .partitionedBy("year") \
    .location(GOLD_ORDERS_TYPE_DELIVERY_TIME_PATH) \
    .execute()

# COMMAND ----------

gold_orders_type_delivery_time_table = DeltaTable.forPath(spark, GOLD_ORDERS_TYPE_DELIVERY_TIME_PATH)

# COMMAND ----------

gold_orders_type_delivery_time_table.alias('ingested_data') \
  .merge(
      source = gold_orders_type_delivery_time_df.alias("updates"),
      condition = expr(
                        "ingested_data.type = updates.type AND "
                        + "ingested_data.delivery_time = updates.delivery_time AND "
                        + "ingested_data.year = updates.year AND "
                        + "ingested_data.month = updates.month"
                        )
  ) \
  .whenMatchedUpdate(set = build_insert_columns_dict(gold_orders_type_delivery_time_df.columns, "updates")) \
  .whenNotMatchedInsert(values = build_insert_columns_dict(gold_orders_type_delivery_time_df.columns, "updates")) \
  .execute()

# COMMAND ----------

gold_table_df = DeltaTable.forName(spark, "gold_orders_by_customer_week_table").toDF()
gold_table_count  = gold_table_df.select("order_count").groupBy().sum().collect()[0][0]

if silver_orders_count != gold_orders_count:
    raise AssertionError(f"The records in silver_order_table differs from the sum of order_count in gold_orders_by_customer_week_table. There are {silver_orders_count} orders in silver_customers_table and {gold_table_count} in gold_orders_by_customer_week_table.")