# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import col, expr, lit, when

# COMMAND ----------

# MAGIC %run "../../CommonFunctions" 

# COMMAND ----------

# MAGIC %run "../../Configs" 

# COMMAND ----------

spark.catalog.setCurrentDatabase(f"{DATABASE_NAME}")

# COMMAND ----------

authenticate_to_storage()

# COMMAND ----------

#tables required for dlq cleansing
bronze_items_df = DeltaTable.forName(spark, "bronze_items_table").toDF()
ingested_silver_items_df = DeltaTable.forName(spark, "silver_items_table").toDF()
dlq_items_df = DeltaTable.forName(spark, "dlq_items_table").toDF()

# COMMAND ----------

#update ingestion status
dlq_items_df = check_ingestion_status(dlq_items_df, ingested_silver_items_df, id_columns=["id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#validate ingested non-duplicates
dlq_items_df = dlq_items_df.withColumn("validation_status", when((col("is_duplicate") == False) & (col("silver_ingestion_status") == "ingested"), lit("valid")).otherwise(col("validation_status")))

# COMMAND ----------

#Update DLQ table

dlq_items_table = DeltaTable.forPath(spark, ITEMS_DLQ_PATH)

dlq_items_table.alias("ingested_data") \
    .merge(
        source = dlq_items_df.alias("updates"),
        condition = expr("ingested_data.codes = updates.codes AND " \
            + "ingested_data.descriptions = updates.descriptions AND " \
            + "ingested_data.id = updates.id AND " \
            + "ingested_data.price = updates.price AND " \
            + "ingested_data.window_id = updates.window_id"
         )
    ) \
    .whenMatchedUpdate(set = build_insert_columns_dict(dlq_items_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

#validate number of records
bronze_items_df = DeltaTable.forName(spark, "bronze_items_table").toDF()
silver_items_df = DeltaTable.forName(spark, "silver_items_table").toDF()
dlq_items_df = DeltaTable.forName(spark, "dlq_items_table").toDF()

count_bronze = bronze_items_df.count()
count_invalid_dlq = dlq_items_df.filter(col("validation_status") == "invalid").count()
count_silver =silver_items_df.count()


if (count_silver + count_invalid_dlq) != count_bronze:
    raise AssertionError(f"The number of records ingested in silver and the number of invalid records dead letter queue (DQL) tables do not match the number of records in bronze table. Bronze table has a total of {count_bronze} records. Silver table has {count_silver} records, and DQL table has {count_invalid_dlq} invalid records, with a combined {count_silver + count_invalid_dlq} records.")
