# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import col, sum, count
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../CommonFunctions" 

# COMMAND ----------

# MAGIC %run "../Configs" 

# COMMAND ----------

spark.catalog.setCurrentDatabase(f"{DATABASE_NAME}")

# COMMAND ----------

authenticate_to_storage()

# COMMAND ----------

#Read required table
silver_customers_df = DeltaTable.forName(spark, "silver_customers_table").toDF().withColumnRenamed("id", "customer_id").withColumnRenamed("created_on", "customer_created_on").drop("silver_ingestion_time")

# COMMAND ----------

customer_breakdown_df = silver_customers_df.select(["type", "status"]).groupBy("type", "status").agg(count("*").alias("customer_count"))

# COMMAND ----------

gold_customers_count  = customer_breakdown_df.select("customer_count").groupBy().sum().collect()[0][0]
silver_customers_count = silver_customers_df.count()

# COMMAND ----------

if silver_customers_count != gold_customers_count:
    raise AssertionError(f"The records in silver_customers_table differs from the sum of customers_count in customer_breakdown_df. There are {silver_customers_count} customers in silver_customers_table and {gold_customers_count} in customer_breakdown_df.")

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("gold_customer_breakdown_table") \
    .addColumns(customer_breakdown_df.schema) \
    .location(GOLD_CUSTOMER_BREAKDOWN_PATH) \
    .execute()

# COMMAND ----------

gold_customer_breakdown_table = DeltaTable.forPath(spark, GOLD_CUSTOMER_BREAKDOWN_PATH)

# COMMAND ----------

gold_customer_breakdown_table.alias('ingested_data') \
  .merge(
      source = customer_breakdown_df.alias("updates"),
      condition = expr("ingested_data.type = updates.type AND ingested_data.status = updates.status")
  ) \
  .whenMatchedUpdate(set = build_insert_columns_dict(customer_breakdown_df.columns, "updates")) \
  .whenNotMatchedInsert(values = build_insert_columns_dict(customer_breakdown_df.columns, "updates")) \
  .execute()

# COMMAND ----------

gold_table_df = DeltaTable.forName(spark, "gold_customer_breakdown_table").toDF()
gold_table_count  = gold_table_df.select("customer_count").groupBy().sum().collect()[0][0]


if silver_customers_count != gold_table_count:
    raise AssertionError(f"The records in silver_customers_table differs from the sum of customers_count in customer_breakdown_table. There are {silver_customers_count} customers in silver_customers_table and {gold_table_count} in customer_breakdown_table.")