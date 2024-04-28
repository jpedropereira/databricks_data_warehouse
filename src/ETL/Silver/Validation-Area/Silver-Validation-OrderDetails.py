# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import array, col, expr, lit, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

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
    StructField("order_id", LongType(), nullable=False),
    StructField("item_id", LongType(), nullable=False),
    StructField("quantity", LongType(), nullable=False),
    StructField("silver_ingestion_time", TimestampType(), nullable=False)
])

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("silver_order_details_table") \
    .addColumns(silver_schema) \
    .partitionedBy("item_id") \
    .location(ORDER_DETAILS_SILVER_PATH) \
    .execute()

# COMMAND ----------

bronze_order_details_df = DeltaTable.forName(spark, "bronze_order_details_table").toDF()
ingested_silver_order_details_df = DeltaTable.forName(spark, "silver_order_details_table").toDF()

# COMMAND ----------

#Rename columns to snake casing
bronze_order_details_df = bronze_order_details_df.withColumnRenamed("OrderId", "order_id").withColumnRenamed("ItemId", "item_id").withColumnRenamed("Quantity", "quantity")

# COMMAND ----------

bronze_order_details_df = bronze_order_details_df.withColumn("validation_status", lit(""))
bronze_order_details_df = bronze_order_details_df.withColumn("invalid_columns", array())
bronze_order_details_df = bronze_order_details_df.withColumn("invalid_relationship", array())

# COMMAND ----------

#add ingestion status
bronze_order_details_df = check_ingestion_status(bronze_order_details_df, ingested_silver_order_details_df, id_columns=["order_id", "item_id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#identify duplicates
bronze_order_details_df = identify_duplicates(bronze_order_details_df, id_columns=["order_id", "item_id"], ingestion_timestamp="bronze_ingestion_time", ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

bronze_order_details_df = validate_column(bronze_order_details_df, column_to_validate="order_id", datatype="numeric")
bronze_order_details_df = validate_column(bronze_order_details_df, column_to_validate="item_id", datatype="numeric")
bronze_order_details_df = validate_column(bronze_order_details_df, column_to_validate="quantity", datatype="numeric")


# COMMAND ----------

#validate relationships
silver_orders_df = DeltaTable.forName(spark, "silver_orders_table").toDF()
silver_items_df = DeltaTable.forName(spark, "silver_items_table").toDF()

bronze_order_details_df = validate_relationship(bronze_order_details_df, "order_id", silver_orders_df, "id")
bronze_order_details_df = validate_relationship(bronze_order_details_df, "item_id", silver_items_df, "id")


# COMMAND ----------

#label valid records
bronze_order_details_df = bronze_order_details_df.withColumn("validation_status", when(col("validation_status") == "", lit("valid")).otherwise(col("validation_status")))

# COMMAND ----------

silver_order_details_df = bronze_order_details_df.filter(col("validation_status") == "valid").select("order_id", "item_id", "quantity").withColumn("silver_ingestion_time", current_timestamp())

# COMMAND ----------

dlq_order_details_df = bronze_order_details_df.filter(col("validation_status") == "invalid").withColumn("dlq_ingestion_time", current_timestamp())

# COMMAND ----------

count_bronze = bronze_order_details_df.count()
count_silver = silver_order_details_df.count()
count_dlq = dlq_order_details_df.count()

# COMMAND ----------

if (count_silver + count_dlq) != count_bronze:
    raise AssertionError(f"The number of records to be ingested into silver and dead letter queue (DQL) tables do not match the number of records in bronze table. Bronze table has a total of {count_bronze} records. Silver table has {count_silver} records, and DQL table has {count_dlq} records, with a combined {count_silver + count_dlq} records.")

# COMMAND ----------

silver_order_details_table = DeltaTable.forPath(spark, ORDER_DETAILS_SILVER_PATH)

silver_order_details_table.alias("ingested_data") \
    .merge(
        source = silver_order_details_df.alias("updates"),
        condition = expr("ingested_data.order_id = updates.order_id AND ingested_data.item_id = updates.item_id")
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(silver_order_details_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

#avoid invalid_relationship to be of type array<null>
dlq_order_details_df = dlq_order_details_df.withColumn("invalid_relationship", col("invalid_relationship").cast("array<string>"))

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("dlq_order_details_table") \
    .addColumns(dlq_order_details_df.schema) \
    .location(ORDER_DETAILS_DLQ_PATH) \
    .execute()

# COMMAND ----------

dlq_order_details_table = DeltaTable.forPath(spark, ORDER_DETAILS_DLQ_PATH)

dlq_order_details_table.alias("ingested_data") \
    .merge(
        source = dlq_order_details_df.alias("updates"),
        condition = expr(condition_builder(dlq_order_details_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(dlq_order_details_df.columns, "updates")
                    ) \
    .execute()