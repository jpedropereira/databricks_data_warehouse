# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, expr
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

#Read source data
items_df = spark.read.parquet(ITEMS_SOURCE_PATH)

# Add an incremental ID to detect duplicates in source data
window = Window().partitionBy(items_df.columns).orderBy(items_df.columns)
items_df = items_df.withColumn("window_id", row_number().over(window))

#Add ingestion timestamp
items_df = items_df.withColumn("bronze_ingestion_time", current_timestamp())

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("bronze_items_table") \
    .addColumns(items_df.schema) \
    .location(ITEMS_BRONZE_PATH) \
    .execute()

# COMMAND ----------

bronze_items_table = DeltaTable.forPath(spark, ITEMS_BRONZE_PATH)

bronze_items_table.alias("ingested_data") \
    .merge(
        source = items_df.alias("updates"),
        condition = expr(condition_builder(items_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(items_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

