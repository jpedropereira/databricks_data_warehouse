# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import array, col, expr, lit, current_timestamp, when

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
bronze_addresses_df = DeltaTable.forName(spark, "bronze_addresses_table").toDF()
ingested_silver_addresses_df = DeltaTable.forName(spark, "silver_addresses_table").toDF()
dlq_addresses_df = DeltaTable.forName(spark, "dlq_addresses_table").toDF()

# COMMAND ----------

#Reset validation_status, and silver_ingestion_status
dlq_addresses_df = dlq_addresses_df.withColumn("validation_status", lit("")).withColumn("invalid_columns", array()).withColumn("silver_ingestion_status", lit(""))

# COMMAND ----------

#update ingestion status
dlq_addresses_df = check_ingestion_status(dlq_addresses_df, ingested_silver_addresses_df, id_columns=["id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#invalidate id duplicates
dlq_addresses_df = dlq_addresses_df.withColumn('validation_status', when(col("is_duplicate"), lit('invalid')).otherwise(col('validation_status'))) \
                    .withColumn('invalid_columns', when(col("is_duplicate"), array(concat_ws(', ', lit("id"), 'invalid_columns'))).otherwise(col('invalid_columns')))

# COMMAND ----------

#perform data cleansing operations
dlq_addresses_df = dlq_addresses_df.withColumn("city", col("partition_city"))
dlq_addresses_df = dlq_addresses_df.withColumn("state", col("clean_state"))
dlq_addresses_df = clean_string_in_df(dlq_addresses_df, "country")
dlq_addresses_df = dlq_addresses_df.withColumn("address_line", col("clean_address_line"))


# COMMAND ----------

#perform validations
dlq_addresses_df = validate_column(dlq_addresses_df, column_to_validate="created_on", datatype="timestamp")
dlq_addresses_df = validate_column(dlq_addresses_df, column_to_validate="city", datatype="string", comparison_column="partition_city")
dlq_addresses_df = validate_column(dlq_addresses_df, column_to_validate="state", datatype="string", comparison_column="clean_state")
dlq_addresses_df = validate_column(dlq_addresses_df, column_to_validate="country", datatype="string", accepted_values=["Us"])
dlq_addresses_df = validate_column(dlq_addresses_df, column_to_validate="id", datatype="numeric")
dlq_addresses_df = validate_column(dlq_addresses_df, column_to_validate="address_line", datatype="string", comparison_column="clean_address_line")


# COMMAND ----------

dlq_addresses_df = dlq_addresses_df.withColumn("validation_status", when(col("validation_status") == "", lit("valid")).otherwise(col("validation_status")))

# COMMAND ----------

#Ingest valid data into Silver

silver_addresses_df = dlq_addresses_df.filter(col("validation_status") == "valid").select("created_on", "city", "state", "country", "id", "address_line").withColumn("silver_ingestion_time", current_timestamp())

silver_addresses_table = DeltaTable.forPath(spark, ADDRESSES_SILVER_PATH)

silver_addresses_table.alias("ingested_data") \
    .merge(
        source = silver_addresses_df.alias("updates"),
        condition = expr(condition_builder(silver_addresses_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(silver_addresses_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

#update ingestion status
ingested_silver_addresses_df = DeltaTable.forName(spark, "silver_addresses_table").toDF()
dlq_addresses_df = check_ingestion_status(dlq_addresses_df, ingested_silver_addresses_df, id_columns=["id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

# Update DLQ table
dlq_addresses_table = DeltaTable.forPath(spark, ADDRESSES_DLQ_PATH)

dlq_addresses_table.alias("ingested_data") \
    .merge(
        source=dlq_addresses_df.alias("updates"),
        condition=expr(
            "ingested_data.id = updates.id AND " +
            "ingested_data.created_on = updates.created_on AND " +
            "ingested_data.window_id = updates.window_id AND " +
            "ingested_data.unclean_city = updates.unclean_city AND " +
            "ingested_data.unclean_state = updates.unclean_state AND " +
            "ingested_data.unclean_country = updates.unclean_country AND " +
            "ingested_data.unclean_address_line = updates.unclean_address_line"
        )
    ) \
    .whenMatchedUpdate(
        set=build_insert_columns_dict(dlq_addresses_df.columns, "updates")
    ) \
    .execute()

# COMMAND ----------

#validate number of records
bronze_addresses_df = DeltaTable.forName(spark, "bronze_addresses_table").toDF()
silver_addresses_df = DeltaTable.forName(spark, "silver_addresses_table").toDF()
dlq_addresses_df = DeltaTable.forName(spark, "dlq_addresses_table").toDF()

count_bronze = bronze_addresses_df.count()
count_invalid_dlq = dlq_addresses_df.filter(col("validation_status") == "invalid").count()
count_silver =silver_addresses_df.count()


if (count_silver + count_invalid_dlq) != count_bronze:
    raise AssertionError(f"The number of records ingested in silver and the number of invalid records dead letter queue (DQL) tables do not match the number of records in bronze table. Bronze table has a total of {count_bronze} records. Silver table has {count_silver} records, and DQL table has {count_invalid_dlq} invalid records, with a combined {count_silver + count_invalid_dlq} records.")


# COMMAND ----------

