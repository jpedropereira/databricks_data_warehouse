# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    array,
    col,
    current_timestamp,
    expr,
    lit,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType
)

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
    StructField("created_on", TimestampType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("country", StringType(), nullable=False),
    StructField("id", LongType(), nullable=False),
    StructField("address_line", StringType(), nullable=False),
    StructField("silver_ingestion_time", TimestampType(), nullable=False)
])

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("silver_addresses_table") \
    .addColumns(silver_schema) \
    .partitionedBy("city") \
    .location(ADDRESSES_SILVER_PATH) \
    .execute()

# COMMAND ----------

bronze_addresses_df = DeltaTable.forName(spark, "bronze_addresses_table").toDF()
ingested_silver_addresses_df = DeltaTable.forName(spark, "silver_addresses_table").toDF()

# COMMAND ----------

#Rename columns to snake casing
bronze_addresses_df = bronze_addresses_df.withColumnRenamed("createdOn", "created_on").withColumnRenamed("addressline", "address_line")

# COMMAND ----------

#add columns for validation
bronze_addresses_df = clean_string_in_df(bronze_addresses_df, column_to_clean="state", new_column="clean_state")
bronze_addresses_df = clean_string_in_df(bronze_addresses_df, column_to_clean="address_line", new_column="clean_address_line", is_address=True)
bronze_addresses_df = bronze_addresses_df.withColumn("validation_status", lit(""))
bronze_addresses_df = bronze_addresses_df.withColumn("invalid_columns", array())

# COMMAND ----------

#add ingestion status
bronze_addresses_df = check_ingestion_status(bronze_addresses_df, ingested_silver_addresses_df, id_columns=["id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#identify duplicates
bronze_addresses_df = identify_duplicates(bronze_addresses_df, id_columns=["id"], ingestion_timestamp="bronze_ingestion_time", ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#perform other validations
bronze_addresses_df = validate_column(bronze_addresses_df, column_to_validate="created_on", datatype="timestamp")
bronze_addresses_df = validate_column(bronze_addresses_df, column_to_validate="city", datatype="string", comparison_column="partition_city")
bronze_addresses_df = validate_column(bronze_addresses_df, column_to_validate="state", datatype="string", comparison_column="clean_state")
bronze_addresses_df = validate_column(bronze_addresses_df, column_to_validate="country", datatype="string", accepted_values=["Us"])
bronze_addresses_df = validate_column(bronze_addresses_df, column_to_validate="id", datatype="numeric")
bronze_addresses_df = validate_column(bronze_addresses_df, column_to_validate="address_line", datatype="string", comparison_column="clean_address_line")

# COMMAND ----------

bronze_addresses_df = bronze_addresses_df.withColumn("validation_status", when(col("validation_status") == "", lit("valid")).otherwise(col("validation_status")))

# COMMAND ----------

silver_addresses_df = bronze_addresses_df.filter(col("validation_status") == "valid").select("created_on", "city", "state", "country", "id", "address_line").withColumn("silver_ingestion_time", current_timestamp())

# COMMAND ----------

dlq_addresses_df = bronze_addresses_df.filter(col("validation_status") == "invalid").withColumn("dlq_ingestion_time", current_timestamp())
dlq_addresses_df = dlq_addresses_df.withColumn("unclean_city", col("city")).withColumn("unclean_state", col("state")).withColumn("unclean_country", col("country")).withColumn("unclean_address_line", col("address_line"))

# COMMAND ----------

count_bronze = bronze_addresses_df.count()
count_silver = silver_addresses_df.count()
count_dlq = dlq_addresses_df.count()

# COMMAND ----------

if (count_silver + count_dlq) != count_bronze:
    raise AssertionError(f"The number of records to be ingested into silver and dead letter queue (DQL) tables do not match the number of records in bronze table. Bronze table has a total of {count_bronze} records. Silver table has {count_silver} records, and DQL table has {count_dlq} records, with a combined {count_silver + count_dlq} records.")


# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("dlq_addresses_table") \
    .addColumns(dlq_addresses_df.schema) \
    .partitionedBy("partition_city") \
    .location(ADDRESSES_DLQ_PATH) \
    .execute()

# COMMAND ----------

silver_addresses_table = DeltaTable.forPath(spark, ADDRESSES_SILVER_PATH)

silver_addresses_table.alias("ingested_data") \
    .merge(
        source = silver_addresses_df.alias("updates"),
        condition = "ingested_data.id = updates.id"
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(silver_addresses_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

dlq_addresses_table = DeltaTable.forPath(spark, ADDRESSES_DLQ_PATH)

dlq_addresses_table.alias("ingested_data") \
    .merge(
        source = dlq_addresses_df.alias("updates"),
        condition = expr(condition_builder(dlq_addresses_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(dlq_addresses_df.columns, "updates")
                    ) \
    .execute()