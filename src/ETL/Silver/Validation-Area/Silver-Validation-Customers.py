# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import array, col, expr, lit,current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType

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
    StructField("id", LongType(), nullable=False),
    StructField("type", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("created_on", TimestampType(), nullable=False),
    StructField("year", IntegerType(), nullable=False),
    StructField("month", IntegerType(), nullable=False),
    StructField("silver_ingestion_time", TimestampType(), nullable=False)
])

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("silver_customers_table") \
    .addColumns(silver_schema) \
    .partitionedBy(["year", "month"]) \
    .location(CUSTOMERS_SILVER_PATH) \
    .execute()

# COMMAND ----------

bronze_customers_df = DeltaTable.forName(spark, "bronze_customers_table").toDF()
ingested_silver_customers_df = DeltaTable.forName(spark, "silver_customers_table").toDF()

# COMMAND ----------

#Rename columns to snake casing
bronze_customers_df = bronze_customers_df.withColumnRenamed("CreatedOn", "created_on")

# COMMAND ----------

bronze_customers_df = bronze_customers_df.withColumn("validation_status", lit(""))
bronze_customers_df = bronze_customers_df.withColumn("invalid_columns", array())

# COMMAND ----------

#add ingestion status
bronze_customers_df = check_ingestion_status(bronze_customers_df, ingested_silver_customers_df, id_columns=["id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#identify duplicates
bronze_customers_df = identify_duplicates(bronze_customers_df, id_columns=["id"], ingestion_timestamp="bronze_ingestion_time", ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#perform other validations
bronze_customers_df = validate_column(bronze_customers_df, column_to_validate="id", datatype="numeric")
bronze_customers_df = validate_column(bronze_customers_df, column_to_validate="type", datatype="string", accepted_values=["affiliate", "individual"])
bronze_customers_df = validate_column(bronze_customers_df, column_to_validate="status", datatype="string", accepted_values=["regular", "VIP"])
bronze_customers_df = validate_column(bronze_customers_df, column_to_validate="created_on", datatype="timestamp")

# COMMAND ----------

bronze_customers_df = bronze_customers_df.withColumn("validation_status", when(col("validation_status") == "", lit("valid")).otherwise(col("validation_status")))

# COMMAND ----------

silver_customers_df = bronze_customers_df.filter(col("validation_status") == "valid").select("id", "type", "status", "created_on", "year", "month").withColumn("silver_ingestion_time", current_timestamp())

# COMMAND ----------

dlq_customers_df = bronze_customers_df.filter(col("validation_status") == "invalid").withColumn("dlq_ingestion_time", current_timestamp())
dlq_customers_df = dlq_customers_df.withColumn("unclean_type", col("type")).withColumn("unclean_status", col("status"))

# COMMAND ----------

count_bronze = bronze_customers_df.count()
count_silver = silver_customers_df.count()
count_dlq = dlq_customers_df.count()

# COMMAND ----------

if (count_silver + count_dlq) != count_bronze:
    raise AssertionError(f"The number of records to be ingested into silver and dead letter queue (DQL) tables do not match the number of records in bronze table. Bronze table has a total of {count_bronze} records. Silver table has {count_silver} records, and DQL table has {count_dlq} records, with a combined {count_silver + count_dlq} records.")

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("dlq_customers_table") \
    .addColumns(dlq_customers_df.schema) \
    .partitionedBy(["year", "month"]) \
    .location(CUSTOMERS_DLQ_PATH) \
    .execute()

# COMMAND ----------

silver_customers_table = DeltaTable.forPath(spark, CUSTOMERS_SILVER_PATH)

silver_customers_table.alias("ingested_data") \
    .merge(
        source = silver_customers_df.alias("updates"),
        condition = "ingested_data.id = updates.id"
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(silver_customers_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

dlq_customers_table = DeltaTable.forPath(spark, CUSTOMERS_DLQ_PATH)

dlq_customers_table.alias("ingested_data") \
    .merge(
        source = dlq_customers_df.alias("updates"),
        condition = expr(condition_builder(dlq_customers_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(dlq_customers_df.columns, "updates")
                    ) \
    .execute()