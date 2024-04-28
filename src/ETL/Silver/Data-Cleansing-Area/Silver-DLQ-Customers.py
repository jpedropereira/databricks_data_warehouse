# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import array, col, current_timestamp, expr, length, lit, lower, upper, when

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
bronze_customers_df = DeltaTable.forName(spark, "bronze_customers_table").toDF()
ingested_silver_customers_df = DeltaTable.forName(spark, "silver_customers_table").toDF()
dlq_customers_df = DeltaTable.forName(spark, "dlq_customers_table").toDF()

# COMMAND ----------

#Reset validation_status, and silver_ingestion_status
dlq_customers_df = dlq_customers_df.withColumn("validation_status", lit("")).withColumn("invalid_columns", array()).withColumn("silver_ingestion_status", lit(""))

# COMMAND ----------

#update ingestion status
dlq_customers_df = check_ingestion_status(dlq_customers_df, ingested_silver_customers_df, id_columns=["id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#invalidate id duplicates
dlq_customers_df = dlq_customers_df.withColumn('validation_status', when(col("is_duplicate"), lit('invalid')).otherwise(col('validation_status'))) \
                    .withColumn('invalid_columns', when(col("is_duplicate"), array(concat_ws(', ', lit("id"), 'invalid_columns'))).otherwise(col('invalid_columns')))

# COMMAND ----------

#perform data cleansing operations

dlq_customers_df = clean_string_in_df(dlq_customers_df, "type").withColumn("type", lower(col("type")))
dlq_customers_df = clean_string_in_df(dlq_customers_df, "status") \
    .withColumn("status", when(length(col("status")) == 3, upper(col("status"))).otherwise(lower(col("status"))))

# COMMAND ----------

#perform validations

dlq_customers_df = validate_column(dlq_customers_df, column_to_validate="id", datatype="numeric")
dlq_customers_df = validate_column(dlq_customers_df, column_to_validate="type", datatype="string", accepted_values=["affiliate", "individual"])
dlq_customers_df = validate_column(dlq_customers_df, column_to_validate="status", datatype="string", accepted_values=["regular", "VIP"])
dlq_customers_df = validate_column(dlq_customers_df, column_to_validate="created_on", datatype="timestamp")

# COMMAND ----------

dlq_customers_df = dlq_customers_df.withColumn("validation_status", when(col("validation_status") == "", lit("valid")).otherwise(col("validation_status")))

# COMMAND ----------

#Ingest valid data into Silver

silver_customers_df = dlq_customers_df.filter(col("validation_status") == "valid").select("id", "type", "status", "created_on", "year", "month").withColumn("silver_ingestion_time", current_timestamp())

silver_customers_table = DeltaTable.forPath(spark, CUSTOMERS_SILVER_PATH)

silver_customers_table.alias("ingested_data") \
    .merge(
        source = silver_customers_df.alias("updates"),
        condition = expr(condition_builder(silver_customers_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(silver_customers_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

#update ingestion status
ingested_silver_customers_df = DeltaTable.forName(spark, "silver_customers_table").toDF()
dlq_customers_df = check_ingestion_status(dlq_customers_df, ingested_silver_customers_df, id_columns=["id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#Update DLQ table

dlq_customers_table = DeltaTable.forPath(spark, CUSTOMERS_DLQ_PATH)

dlq_customers_table.alias("ingested_data") \
    .merge(
        source = dlq_customers_df.alias("updates"),
        condition = expr("ingested_data.id = updates.id AND ingested_data.created_on = updates.created_on AND ingested_data.window_id = updates.window_id AND ingested_data.unclean_type = updates.unclean_type AND ingested_data.unclean_status = updates.unclean_status")
    ) \
    .whenMatchedUpdate(set = build_insert_columns_dict(dlq_customers_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

#validate number of records
bronze_customers_df = DeltaTable.forName(spark, "bronze_customers_table").toDF()
silver_customers_df = DeltaTable.forName(spark, "silver_customers_table").toDF()
dlq_customers_df = DeltaTable.forName(spark, "dlq_customers_table").toDF()

count_bronze = bronze_customers_df.count()
count_invalid_dlq = dlq_customers_df.filter(col("validation_status") == "invalid").count()
count_silver =silver_customers_df.count()


if (count_silver + count_invalid_dlq) != count_bronze:
    raise AssertionError(f"The number of records ingested in silver and the number of invalid records dead letter queue (DQL) tables do not match the number of records in bronze table. Bronze table has a total of {count_bronze} records. Silver table has {count_silver} records, and DQL table has {count_invalid_dlq} invalid records, with a combined {count_silver + count_invalid_dlq} records.")
