# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, expr, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../CommonFunctions" 

# COMMAND ----------

# MAGIC %run "../Configs" 

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.catalog.setCurrentDatabase(f"{DATABASE_NAME}")

# COMMAND ----------

authenticate_to_storage()

# COMMAND ----------

#read source data
order_details_df = spark.read.parquet(ORDER_DETAILS_SOURCE_PATH)

# COMMAND ----------

# Add an incremental ID partitioned by OrderId, ItemId, and Quantity - required due to duplicates in source data
window = Window().partitionBy("OrderId", "ItemId", "Quantity").orderBy("OrderId", "ItemId", "Quantity")
order_details_df = order_details_df.withColumn("window_id", row_number().over(window))

# COMMAND ----------

#Add ingestion timestamp
order_details_df = order_details_df.withColumn("bronze_ingestion_time", current_timestamp())

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("bronze_order_details_table") \
    .addColumns(order_details_df.schema) \
    .partitionedBy("ItemId") \
    .location(ORDER_DETAILS_BRONZE_PATH) \
    .execute()

# COMMAND ----------

bronze_order_details_table = DeltaTable.forPath(spark, ORDER_DETAILS_BRONZE_PATH)

bronze_order_details_table.alias("ingested_data") \
    .merge(
        source = order_details_df.alias("updates"),
        condition = expr(condition_builder(order_details_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(order_details_df.columns, "updates")
                    ) \
    .execute()