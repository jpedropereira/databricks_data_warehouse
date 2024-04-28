# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import array, col, expr, lit, current_timestamp, when
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, LongType, DateType

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
    StructField("customer_id", LongType(), nullable=False),
    StructField("created_on", TimestampType(), nullable=False),
    StructField("address_id", LongType(), nullable=False),
    StructField("delivery_date", DateType(), nullable=False),
    StructField("delivered_on", DateType(), nullable=False),
    StructField("id", LongType(), nullable=False),
    StructField("year", IntegerType(), nullable=False),
    StructField("month", IntegerType(), nullable=False),
    StructField("silver_ingestion_time", TimestampType(), nullable=False)
])

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("silver_orders_table") \
    .addColumns(silver_schema) \
    .partitionedBy(["year", "month"]) \
    .location(ORDERS_SILVER_PATH) \
    .execute()

# COMMAND ----------

bronze_orders_df = DeltaTable.forName(spark, "bronze_orders_table").toDF()
ingested_silver_orders_df = DeltaTable.forName(spark, "silver_orders_table").toDF()

# COMMAND ----------

#Rename columns to snake casing
bronze_orders_df = bronze_orders_df.withColumnRenamed("customerId", "customer_id") \
                                   .withColumnRenamed("createdOn", "created_on") \
                                   .withColumnRenamed("addressId", "address_id") \
                                   .withColumnRenamed("deliveryDate", "delivery_date") \
                                   .withColumnRenamed("deliveredOn", "delivered_on")
                                   

# COMMAND ----------

bronze_orders_df = bronze_orders_df.withColumn("validation_status", lit(""))
bronze_orders_df = bronze_orders_df.withColumn("invalid_columns", array())
bronze_orders_df = bronze_orders_df.withColumn("invalid_relationship", array())

# COMMAND ----------

#add ingestion status
bronze_orders_df = check_ingestion_status(bronze_orders_df, ingested_silver_orders_df, id_columns=["id"], ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

#identify duplicates
bronze_orders_df = identify_duplicates(bronze_orders_df, id_columns=["id"], ingestion_timestamp="bronze_ingestion_time", ingestion_status_col="silver_ingestion_status")

# COMMAND ----------

bronze_orders_df = validate_column(bronze_orders_df, column_to_validate="customer_id", datatype="numeric")
bronze_orders_df = validate_column(bronze_orders_df, column_to_validate="created_on", datatype="timestamp")
bronze_orders_df = validate_column(bronze_orders_df, column_to_validate="address_id", datatype="numeric")
bronze_orders_df = validate_column(bronze_orders_df, column_to_validate="delivery_date", datatype="timestamp")
bronze_orders_df = validate_column(bronze_orders_df, column_to_validate="delivered_on", datatype="timestamp")
bronze_orders_df = validate_column(bronze_orders_df, column_to_validate="id", datatype="numeric")

# COMMAND ----------

#validate relationships
silver_customers_df = DeltaTable.forName(spark, "silver_customers_table").toDF()
silver_addresses_df = DeltaTable.forName(spark, "silver_addresses_table").toDF()

bronze_orders_df = validate_relationship(bronze_orders_df, "customer_id", silver_customers_df, "id")
bronze_orders_df = validate_relationship(bronze_orders_df, "address_id", silver_addresses_df, "id")

# COMMAND ----------

#label valid records
bronze_orders_df = bronze_orders_df.withColumn("validation_status", when(col("validation_status") == "", lit("valid")).otherwise(col("validation_status")))

# COMMAND ----------

silver_orders_df = bronze_orders_df.filter(col("validation_status") == "valid").select("customer_id", "created_on", "address_id", "delivery_date", "delivered_on", "id", "year", "month").withColumn("silver_ingestion_time", current_timestamp())

# COMMAND ----------

dlq_orders_df = bronze_orders_df.filter(col("validation_status") == "invalid").withColumn("dlq_ingestion_time", current_timestamp())

# COMMAND ----------

count_bronze = bronze_orders_df.count()
count_silver = silver_orders_df.count()
count_dlq = dlq_orders_df.count()

# COMMAND ----------

if (count_silver + count_dlq) != count_bronze:
    raise AssertionError(f"The number of records to be ingested into silver and dead letter queue (DQL) tables do not match the number of records in bronze table. Bronze table has a total of {count_bronze} records. Silver table has {count_silver} records, and DQL table has {count_dlq} records, with a combined {count_silver + count_dlq} records.")

# COMMAND ----------

silver_orders_table = DeltaTable.forPath(spark, ORDERS_SILVER_PATH)

silver_orders_table.alias("ingested_data") \
    .merge(
        source = silver_orders_df.alias("updates"),
        condition = "ingested_data.id = updates.id"
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(silver_orders_df.columns, "updates")
                    ) \
    .execute()

# COMMAND ----------

#avoid invalid_relationship to be of type array<null>
dlq_orders_df = dlq_orders_df.withColumn("invalid_relationship", col("invalid_relationship").cast("array<string>"))

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("dlq_orders_table") \
    .addColumns(dlq_orders_df.schema) \
    .location(ORDERS_DLQ_PATH) \
    .execute()

# COMMAND ----------

dlq_orders_table = DeltaTable.forPath(spark, ORDERS_DLQ_PATH)

dlq_orders_table.alias("ingested_data") \
    .merge(
        source = dlq_orders_df.alias("updates"),
        condition = expr(condition_builder(dlq_orders_df.columns, "ingested_data", "updates"))
    ) \
    .whenNotMatchedInsert(values = build_insert_columns_dict(dlq_orders_df.columns, "updates")
                    ) \
    .execute()