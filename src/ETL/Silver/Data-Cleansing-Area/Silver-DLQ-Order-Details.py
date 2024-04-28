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
bronze_order_details_df = DeltaTable.forName(spark, "bronze_order_details_table").toDF()
ingested_silver_order_details_df = DeltaTable.forName(spark, "silver_order_details_table").toDF()
dlq_order_details_df = DeltaTable.forName(spark, "dlq_order_details_table").toDF()

# COMMAND ----------

#update ingestion status
dlq_order_details_df = check_ingestion_status(dlq_order_details_df, ingested_silver_order_details_df, id_columns=["order_id", "item_id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#validate ingested non-duplicates
dlq_order_details_df = dlq_order_details_df.withColumn("validation_status", when((col("is_duplicate") == False) & (col("silver_ingestion_status") == "ingested"), lit("valid")).otherwise(col("validation_status")))

# COMMAND ----------

#Update DLQ table

dlq_order_details_table = DeltaTable.forPath(spark, ORDER_DETAILS_DLQ_PATH)

dlq_order_details_table.alias("ingested_data") \
    .merge(
        source = dlq_order_details_df.alias("updates"),
        condition = expr("ingested_data.order_id = updates.order_id AND " \
            + "ingested_data.item_id = updates.item_id AND " \
            + "ingested_data.quantity = updates.quantity AND " \
            + "ingested_data.window_id = updates.window_id"
         )
    ) \
    .whenMatchedUpdate(set = build_insert_columns_dict(dlq_order_details_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

#validate number of records
bronze_order_details_df = DeltaTable.forName(spark, "bronze_order_details_table").toDF()
silver_order_details_df = DeltaTable.forName(spark, "silver_order_details_table").toDF()
dlq_order_details_df = DeltaTable.forName(spark, "dlq_order_details_table").toDF()

count_bronze = bronze_order_details_df.count()
count_invalid_dlq = dlq_order_details_df.filter(col("validation_status") == "invalid").count()
count_silver =silver_order_details_df.count()


if (count_silver + count_invalid_dlq) != count_bronze:
    raise AssertionError(f"The number of records ingested in silver and the number of invalid records dead letter queue (DQL) tables do not match the number of records in bronze table. Bronze table has a total of {count_bronze} records. Silver table has {count_silver} records, and DQL table has {count_invalid_dlq} invalid records, with a combined {count_silver + count_invalid_dlq} records.")