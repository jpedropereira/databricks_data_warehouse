# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import col, sum, count, weekofyear
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
silver_customers_df = DeltaTable.forName(spark, "silver_customers_table").toDF().withColumnRenamed("id", "customer_id").withColumnRenamed("created_on", "customer_created_on").drop("silver_ingestion_time")
silver_orders_df = DeltaTable.forName(spark, "silver_orders_table").toDF().withColumnRenamed("id", "order_id").withColumnRenamed("created_on", "order_created_on").drop("silver_ingestion_time")

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn("week", weekofyear(col("order_created_on")))

# COMMAND ----------

join_customers_orders_df = silver_orders_df.join(silver_customers_df.select("customer_id", "type", "status"), on="customer_id", how="left").withColumnRenamed("type", "customer_type").withColumnRenamed("status", "customer_status")

# COMMAND ----------

grouped_orders_customer_week_df = join_customers_orders_df.select("customer_id", "order_id", "year", "week", "customer_type").groupBy("customer_id", "year", "week", "customer_type").agg(count("order_id").alias("order_count"))

# COMMAND ----------

gold_orders_count  = grouped_orders_customer_week_df.select("order_count").groupBy().sum().collect()[0][0]
silver_orders_count = silver_orders_df.count()

# COMMAND ----------

if silver_orders_count != gold_orders_count:
    raise AssertionError(f"The records in silver_order_table differs from the sum of order_count in grouped_orders_customer_week_df. There are {silver_orders_count} orders in silver_customers_table and {gold_orders_count} in grouped_orders_customer_week_df.")

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("gold_orders_by_customer_week_table") \
    .addColumns(grouped_orders_customer_week_df.schema) \
    .partitionedBy(["year", "week"]) \
    .location(GOLD_ORDERS_BY_CUSTOMER_WEEK_PATH) \
    .execute()

# COMMAND ----------

gold_orders_by_customer_week_table = DeltaTable.forPath(spark, GOLD_ORDERS_BY_CUSTOMER_WEEK_PATH)

# COMMAND ----------

gold_orders_by_customer_week_table.alias('ingested_data') \
  .merge(
      source = grouped_orders_customer_week_df.alias("updates"),
      condition = expr(
                        "ingested_data.customer_id = updates.customer_id AND "
                        + "ingested_data.year = updates.year AND "
                        + "ingested_data.week = updates.week AND "
                        + "ingested_data.customer_type = updates.customer_type"
                        )
  ) \
  .whenMatchedUpdate(set = build_insert_columns_dict(grouped_orders_customer_week_df.columns, "updates")) \
  .whenNotMatchedInsert(values = build_insert_columns_dict(grouped_orders_customer_week_df.columns, "updates")) \
  .execute()

# COMMAND ----------

gold_table_df = DeltaTable.forName(spark, "gold_orders_by_customer_week_table").toDF()
gold_table_count  = gold_table_df.select("order_count").groupBy().sum().collect()[0][0]

if silver_orders_count != gold_table_count:
    raise AssertionError(f"The records in silver_order_table differs from the sum of order_count in gold_orders_by_customer_week_table. There are {silver_orders_count} orders in silver_customers_table and {gold_table_count} in gold_orders_by_customer_week_table.")