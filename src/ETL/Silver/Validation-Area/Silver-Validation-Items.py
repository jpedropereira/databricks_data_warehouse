# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import array, col, expr, lit, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType

# COMMAND ----------

# MAGIC %run "../../CommonFunctions" 

# COMMAND ----------

# MAGIC %run "../../Configs"

# COMMAND ----------

spark.catalog.setCurrentDatabase(f"{DATABASE_NAME}")

# COMMAND ----------

authenticate_to_storage()

# COMMAND ----------

silver_schema = StructType([
    StructField("codes", StringType(), nullable=False),
    StructField("descriptions", StringType(), nullable=False),
    StructField("id", IntegerType(), nullable=False),
    StructField("price", FloatType(), nullable=False),
    StructField("silver_ingestion_time", TimestampType(), nullable=False)
])

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("silver_items_table") \
    .addColumns(silver_schema) \
    .location(ITEMS_SILVER_PATH) \
    .execute()

# COMMAND ----------

bronze_items_df = DeltaTable.forName(spark, "bronze_items_table").toDF()
ingested_silver_items_df = DeltaTable.forName(spark, "silver_items_table").toDF()

# COMMAND ----------

#Rename columns to snake casing
bronze_items_df = bronze_items_df.withColumnRenamed("Codes", "codes").withColumnRenamed("Descriptions", "descriptions")

# COMMAND ----------

bronze_items_df = bronze_items_df.withColumn("validation_status", lit(""))
bronze_items_df = bronze_items_df.withColumn("invalid_columns", array())

# COMMAND ----------

#add ingestion status
bronze_items_df = check_ingestion_status(bronze_items_df, ingested_silver_items_df, id_columns=["id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#identify duplicates
bronze_items_df = identify_duplicates(bronze_items_df, id_columns=["id"], ingestion_timestamp="bronze_ingestion_time", ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#perform other validations
bronze_items_df = validate_column(bronze_items_df, column_to_validate="id", datatype="numeric")
bronze_items_df = validate_column(bronze_items_df, column_to_validate="price", datatype="numeric")

# COMMAND ----------

bronze_items_df = bronze_items_df.withColumn("validation_status", when(col("validation_status") == "", lit("valid")).otherwise(col("validation_status")))

# COMMAND ----------

silver_items_df = bronze_items_df.filter(col("validation_status") == "valid").select("codes", "descriptions", "id", "price").withColumn("silver_ingestion_time", current_timestamp())

# COMMAND ----------

dlq_items_df = bronze_items_df.filter(col("validation_status") == "invalid").withColumn("dlq_ingestion_time", current_timestamp())

# COMMAND ----------

count_bronze = bronze_items_df.count()
count_silver = silver_items_df.count()
count_dlq = dlq_items_df.count()

# COMMAND ----------

if (count_silver + count_dlq) != count_bronze:
    raise AssertionError(f"The number of records to be ingested into silver and dead letter queue (DQL) tables do not match the number of records in bronze table. Bronze table has a total of {count_bronze} records. Silver table has {count_silver} records, and DQL table has {count_dlq} records, with a combined {count_silver + count_dlq} records.")

# COMMAND ----------

print(count_silver)

# COMMAND ----------

silver_items_table = DeltaTable.forPath(spark, ITEMS_SILVER_PATH)

silver_items_table.alias("ingested_data") \
    .merge(
        source = silver_items_df.alias("updates"),
        condition = "ingested_data.id = updates.id"
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(silver_items_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("dlq_items_table") \
    .addColumns(dlq_items_df.schema) \
    .location(ITEMS_DLQ_PATH) \
    .execute()

# COMMAND ----------

dlq_items_table = DeltaTable.forPath(spark, ITEMS_DLQ_PATH)

dlq_items_table.alias("ingested_data") \
    .merge(
        source = dlq_items_df.alias("updates"),
        condition = expr(condition_builder(dlq_items_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(dlq_items_df.columns, "updates")
                    ) \
    .execute()