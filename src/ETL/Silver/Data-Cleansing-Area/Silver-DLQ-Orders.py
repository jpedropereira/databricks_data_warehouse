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
bronze_orders_df = DeltaTable.forName(spark, "bronze_orders_table").toDF()
ingested_silver_orders_df = DeltaTable.forName(spark, "silver_orders_table").toDF()
dlq_orders_df = DeltaTable.forName(spark, "dlq_orders_table").toDF()

# COMMAND ----------

#update ingestion status
dlq_orders_df = check_ingestion_status(dlq_orders_df, ingested_silver_orders_df, id_columns=["id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#validate ingested non-duplicates
dlq_orders_df = dlq_orders_df.withColumn("validation_status", when((col("is_duplicate") == False) & (col("silver_ingestion_status") == "ingested"), lit("valid")).otherwise(col("validation_status")))

# COMMAND ----------

#Update DLQ table

dlq_orders_table = DeltaTable.forPath(spark, ORDERS_DLQ_PATH)

dlq_orders_table.alias("ingested_data") \
    .merge(
        source = dlq_orders_df.alias("updates"),
        condition = expr("ingested_data.customer_id = updates.customer_id AND " \
            + "ingested_data.created_on = updates.created_on AND " \
            + "ingested_data.address_id = updates.address_id AND " \
            + "ingested_data.delivery_date = updates.delivery_date AND " \
            + "ingested_data.delivered_on = updates.delivered_on AND " \
            + "ingested_data.id = updates.id AND " \
            + "ingested_data.window_id = updates.window_id" \
         )
    ) \
    .whenMatchedUpdate(set = build_insert_columns_dict(dlq_orders_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

#validate number of records
bronze_orders_df = DeltaTable.forName(spark, "bronze_orders_table").toDF()
silver_orders_df = DeltaTable.forName(spark, "silver_orders_table").toDF()
dlq_orders_df = DeltaTable.forName(spark, "dlq_orders_table").toDF()

count_bronze = bronze_orders_df.count()
count_invalid_dlq = dlq_orders_df.filter(col("validation_status") == "invalid").count()
count_silver =silver_orders_df.count()


if (count_silver + count_invalid_dlq) != count_bronze:
    raise AssertionError(f"The number of records ingested in silver and the number of invalid records dead letter queue (DQL) tables do not match the number of records in bronze table. Bronze table has a total of {count_bronze} records. Silver table has {count_silver} records, and DQL table has {count_invalid_dlq} invalid records, with a combined {count_silver + count_invalid_dlq} records.")


# COMMAND ----------

