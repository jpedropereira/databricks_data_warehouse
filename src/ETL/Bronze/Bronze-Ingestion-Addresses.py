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
addresses_df = spark.read.parquet(ADDRESSES_SOURCE_PATH)

# Add an incremental ID to detect duplicates in source data and allow its ingestion
window = Window().partitionBy(addresses_df.columns).orderBy(addresses_df.columns)
addresses_df = addresses_df.withColumn("window_id", row_number().over(window))

#Create column to partition by city
addresses_df = clean_string_in_df(addresses_df, column_to_clean="city", new_column="partition_city")

#Add ingestion timestamp
addresses_df = addresses_df.withColumn("bronze_ingestion_time", current_timestamp())

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("bronze_addresses_table") \
    .addColumns(addresses_df.schema) \
    .partitionedBy("partition_city") \
    .location(ADDRESSES_BRONZE_PATH) \
    .execute()

# COMMAND ----------

bronze_addresses_table = DeltaTable.forPath(spark, ADDRESSES_BRONZE_PATH)

bronze_addresses_table.alias("ingested_data") \
    .merge(
        source = addresses_df.alias("updates"),
        condition = expr(condition_builder(addresses_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(addresses_df.columns, "updates")
                    ) \
    .execute()