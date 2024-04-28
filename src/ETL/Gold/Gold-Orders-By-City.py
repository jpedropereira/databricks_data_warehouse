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
silver_orders_df = DeltaTable.forName(spark, "silver_orders_table").toDF().withColumnRenamed("id", "order_id").withColumnRenamed("created_on", "order_created_on").drop("silver_ingestion_time")
silver_addresses_df = DeltaTable.forName(spark, "silver_addresses_table").toDF().withColumnRenamed("id", "address_id").withColumnRenamed("created_on", "address_created_on").drop("silver_ingestion_time")


# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn("delivery_time", datediff(col("delivered_on"), col("order_created_on")))

# COMMAND ----------

join_orders_addresses = silver_orders_df.join(silver_addresses_df, on="address_id", how="left")

# COMMAND ----------

gold_orders_by_city_year_month_df = join_orders_addresses.groupBy("city", "year", "month").agg(count("order_id").alias("order_count"), avg("delivery_time").alias("avg_delivery_time"))

# COMMAND ----------

gold_orders_count  = gold_orders_by_city_year_month_df.select("order_count").groupBy().sum().collect()[0][0]
silver_orders_count = silver_orders_df.count()

# COMMAND ----------

if silver_orders_count != gold_orders_count:
    raise AssertionError(f"The records in silver_orders_table differs from the sum of orders_count in orders_by_city_year_month_df. There are {silver_orders_count} orders in silver_orders_table and {gold_orders_count} in orders_by_city_year_month_df.")

# COMMAND ----------

gold_orders_by_city_year_month_df.schema

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("gold_orders_by_city_year_month_table") \
    .addColumns(gold_orders_by_city_year_month_df.schema) \
    .partitionedBy(["year", "month"]) \
    .location(GOLD_ORDERS_BY_CITY_YEAR_MONTH_PATH) \
    .execute()

# COMMAND ----------

gold_orders_by_city_year_month_table = DeltaTable.forPath(spark, GOLD_ORDERS_BY_CITY_YEAR_MONTH_PATH)

# COMMAND ----------

gold_orders_by_city_year_month_table.alias('ingested_data') \
  .merge(
      source = gold_orders_by_city_year_month_df.alias("updates"),
      condition = expr(
                        "ingested_data.city = updates.city AND "
                        + "ingested_data.year = updates.year AND "
                        + "ingested_data.month = updates.month"
                        )
  ) \
  .whenMatchedUpdate(set = build_insert_columns_dict(gold_orders_by_city_year_month_df.columns, "updates")) \
  .whenNotMatchedInsert(values = build_insert_columns_dict(gold_orders_by_city_year_month_df.columns, "updates")) \
  .execute()

# COMMAND ----------

gold_table_df = DeltaTable.forName(spark, "gold_orders_by_city_year_month_table").toDF()
gold_table_count  = gold_table_df.select("order_count").groupBy().sum().collect()[0][0]

if silver_orders_count != gold_table_count:
    raise AssertionError(f"The records in silver_orders_table differs from the sum of orders_count in orders_by_city_year_month_table. There are {silver_orders_count} orders in silver_orders_table and {gold_table_count} in orders_by_city_year_month_table.")

# COMMAND ----------

