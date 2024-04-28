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

#read source data
customers_df = spark.read.parquet(CUSTOMERS_SOURCE_PATH)

# Add an incremental ID to detect duplicates in source data and allow its ingestion
window = Window().partitionBy(customers_df.columns).orderBy(customers_df.columns)
customers_df = customers_df.withColumn("window_id", row_number().over(window))

#create partition columns
customers_df = customers_df.withColumn("year", year("createdOn")).withColumn("month", month("createdOn"))

#Add ingestion timestamp
customers_df = customers_df.withColumn("bronze_ingestion_time", current_timestamp())

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("bronze_customers_table") \
    .addColumns(customers_df.schema) \
    .partitionedBy(["year", "month"]) \
    .location(CUSTOMERS_BRONZE_PATH) \
    .execute()

# COMMAND ----------

bronze_customers_table = DeltaTable.forPath(spark, CUSTOMERS_BRONZE_PATH)

bronze_customers_table.alias("ingested_data") \
    .merge(
        source = customers_df.alias("updates"),
        condition = expr(condition_builder(customers_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(customers_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

