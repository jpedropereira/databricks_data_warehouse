# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import expr, current_timestamp, month, year
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
orders_df = spark.read.parquet(ORDERS_SOURCE_PATH)

# Add an incremental ID to detect duplicates in source data
window = Window().partitionBy(orders_df.columns).orderBy(orders_df.columns)
orders_df = orders_df.withColumn("window_id", row_number().over(window))

#create partition columns
orders_df = orders_df.withColumn("year", year("createdOn")).withColumn("month", month("createdOn"))

#Add ingestion timestamp
orders_df = orders_df.withColumn("bronze_ingestion_time", current_timestamp())

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("bronze_orders_table") \
    .addColumns(orders_df.schema) \
    .partitionedBy(["year", "month"]) \
    .location(ORDERS_BRONZE_PATH) \
    .execute()

# COMMAND ----------

bronze_orders_table = DeltaTable.forPath(spark, ORDERS_BRONZE_PATH)

bronze_orders_table.alias("ingested_data") \
    .merge(
        source = orders_df.alias("updates"),
        condition = expr(condition_builder(orders_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(orders_df.columns, "updates")
                    ) \
    .execute()