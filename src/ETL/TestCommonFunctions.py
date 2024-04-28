# Databricks notebook source
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, TimestampType, LongType, BooleanType

# COMMAND ----------

# MAGIC %run "./CommonFunctions"

# COMMAND ----------

#test condition_builder

condition = condition_builder(["OrderId", "ItemId", "Quantity", "WindowId", "bronze_ingestion_time"], "ingested_data", "updates")

expected_condition ="ingested_data.OrderId = updates.OrderId AND ingested_data.ItemId = updates.ItemId AND ingested_data.Quantity = updates.Quantity AND ingested_data.WindowId = updates.WindowId"

assert condition == expected_condition

# COMMAND ----------

#test build_insert_columns_dict
test_dict = build_insert_columns_dict(["column1", "column2", "column3"], "updates")
expected_dict = {
                'column1': 'updates.column1', 
                'column2': 'updates.column2', 
                'column3': 'updates.column3'
                }

assert test_dict == expected_dict


# COMMAND ----------

#test clean_string_in_df

test_data = [Row(id=1, string="  test #   1231phrase 1/2   /"),
             Row(id=2, string="   test!!!! 989   ")]
test_df = spark.createDataFrame(test_data)

expected_default_data = [Row(id=1, string="Test Phrase"),
                         Row(id=2, string="Test")]
expected_default_df = spark.createDataFrame(expected_default_data)

expected_new_col_data = [Row(id=1, string="  test #   1231phrase 1/2   /", new_col="Test Phrase"),
                         Row(id=2, string="   test!!!! 989   ", new_col="Test")]
expected_new_col_df = spark.createDataFrame(expected_new_col_data)

expected_address_data = [Row(id=1, string="Test 1231phrase 1/2"),
                         Row(id=2, string="Test 989")]
expected_address_df = spark.createDataFrame(expected_address_data)


default_df = clean_string_in_df(test_df, "string")
new_col_df = clean_string_in_df(test_df, column_to_clean="string", new_column="new_col")
address_df = clean_string_in_df(test_df, column_to_clean="string", is_address=True)

assert default_df.toPandas().equals(expected_default_df.toPandas())
assert new_col_df.toPandas().equals(expected_new_col_df.toPandas())
assert address_df.toPandas().equals(expected_address_df.toPandas())

# COMMAND ----------

#test validate_column

schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("correct_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("validation_status", StringType(), True),
    StructField("invalid_columns", ArrayType(StringType()), True)
])

data = [Row(id=1, name="Alice", correct_name="Alice", age=25, date=5, valitation_status="", invalid_columns=[]),
        Row(id=2, name="Bob", correct_name="Bob", age=30, date="test", valitation_status="", invalid_columns=[]),
        Row(id="a", name="Chartie", correct_name="Charlie", age=35, date="2013-06-15T18:14:57.000+0000", valitation_status="", invalid_columns=[])]

test_df = spark.createDataFrame(data, schema)

data_numeric_test = [Row(id=1, name="Alice", correct_name="Alice", age=25, date=5, valitation_status="", invalid_columns=[]),
        Row(id=2, name="Bob", correct_name="Bob", age=30, date="test", valitation_status="", invalid_columns=[]),
        Row(id="a", name="Chartie", correct_name="Charlie", age=35, date="2013-06-15T18:14:57.000+0000", valitation_status="invalid", invalid_columns=["id"])]

expected_numeric_df = spark.createDataFrame(data_numeric_test, schema)
numeric_df = validate_column(test_df, "id", "numeric")

assert expected_numeric_df.toPandas().equals(numeric_df.toPandas())

data_string_sequencial_test = [Row(id=1, name="Alice", correct_name="Alice", age=25, date=5, valitation_status="", invalid_columns=[]),
        Row(id=2, name="Bob", correct_name="Bob", age=30, date="test", valitation_status="", invalid_columns=[]),
        Row(id="a", name="Chartie", correct_name="Charlie", age=35, date="2013-06-15T18:14:57.000+0000", valitation_status="invalid", invalid_columns=["name, id"])]

expected_string_column_df = spark.createDataFrame(data_string_sequencial_test, schema)
data_string_column_df = validate_column(numeric_df, column_to_validate="name", datatype="string", comparison_column="correct_name")

assert expected_string_column_df.toPandas().equals(data_string_column_df.toPandas())

data_string_column_df = validate_column(numeric_df, column_to_validate="name", datatype="string", accepted_values=["Alice", "Bob", "Charlie"])
assert expected_string_column_df.toPandas().equals(data_string_column_df.toPandas())


data_timestamp_test = [Row(id=1, name="Alice", correct_name="Alice", age=25, date=5, valitation_status="invalid", invalid_columns=["date"]),
        Row(id=2, name="Bob", correct_name="Bob", age=30, date="test", valitation_status="invalid", invalid_columns=["date"]),
        Row(id="a", name="Chartie", correct_name="Charlie", age=35, date="2013-06-15T18:14:57.000+0000", valitation_status="", invalid_columns=[])]

expected_timestamp_test_df = spark.createDataFrame(data_timestamp_test, schema)

timestamp_test_df = validate_column(test_df, column_to_validate="date", datatype="timestamp")

assert expected_timestamp_test_df.toPandas().equals(timestamp_test_df.toPandas())



# COMMAND ----------

#test validate_relationship

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("value", StringType(), True),
    StructField("validation_status", StringType(), True),
    StructField("invalid_relationship", ArrayType(StringType()), True)
])

# Sample data for dataframe1
dataframe1 = spark.createDataFrame([
    (1, "A", "", []),
    (2, "B", "", []),
    (3, "C", "", []),
    (4, "D", "", [])
], schema)

# Sample data for dataframe2
dataframe2 = spark.createDataFrame([
    (1, "X", "", []),
    (2, "Y", "", []),
    (3, "Z", "", [])
], schema)

expected_df = spark.createDataFrame([
    (1, "A", "", []),
    (2, "B", "", []),
    (3, "C", "", []),
    (4, "D", "invalid", ["id"])
], schema)

# Call the validate_relationship function
validated_df = validate_relationship(dataframe1, "id", dataframe2, "id")

assert validated_df.toPandas().equals(expected_df.toPandas())

# COMMAND ----------

# test check_ingestion_status

schema = StructType([
    StructField("id1", IntegerType(), True),
    StructField("id2", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("validation_status", StringType(), True),
    StructField("invalid_columns", ArrayType(StringType()), True),
    StructField("silver_ingestion_status", StringType(), True),
])

# Sample data for testing
data_to_ingest = [
    (1, 1, 'John', 22, 'New York', '2024-02-13 12:00:00', '', [], ""),
    (2, 1, 'Doe', 28, 'Seattle', '2024-02-14 12:01:00', '', [], ""),
    (3, 1, 'Smith', 22, 'San Francisco', '2024-02-14 12:02:00', '', [], ""),
    (1, 1, 'John', 22, 'New York', '2024-02-14 12:03:00', '', [], ""),
    (1, 2, 'Tony', 22, 'New York', '2024-02-14 12:00:00', '', [], ""),
    (4, 1, 'John', 22, 'New York', '2024-02-14 12:03:00', 'invalid', ['id1'], "")
]

ingested_data = [
    (1, 1, 'John', 22, 'New York', '2024-02-13 12:00:00', '', [], ""),
    (2, 1, 'Doe', 28, 'Seattle', '2024-02-14 12:01:00', '', [], ""),
]

expected_result_1_col = [
    (1, 1, 'John', 22, 'New York', '2024-02-13 12:00:00', '', [], "ingested"),
    (2, 1, 'Doe', 28, 'Seattle', '2024-02-14 12:01:00', '', [], "ingested"),
    (3, 1, 'Smith', 22, 'San Francisco', '2024-02-14 12:02:00', '', [], "not_ingested"),
    (1, 1, 'John', 22, 'New York', '2024-02-14 12:03:00', '', [], "ingested"),
    (1, 2, 'Tony', 22, 'New York', '2024-02-14 12:00:00', '', [], "ingested"),
    (4, 1, 'John', 22, 'New York', '2024-02-14 12:03:00', 'invalid', ['id1'], "not_ingested")
]

expected_result_2_col = [
    (1, 1, 'John', 22, 'New York', '2024-02-13 12:00:00', '', [], "ingested"),
    (2, 1, 'Doe', 28, 'Seattle', '2024-02-14 12:01:00', '', [], "ingested"),
    (3, 1, 'Smith', 22, 'San Francisco', '2024-02-14 12:02:00', '', [], "not_ingested"),
    (1, 1, 'John', 22, 'New York', '2024-02-14 12:03:00', '', [], "ingested"),
    (1, 2, 'Tony', 22, 'New York', '2024-02-14 12:00:00', '', [], "not_ingested"),
    (4, 1, 'John', 22, 'New York', '2024-02-14 12:03:00', 'invalid', ['id1'], "not_ingested")
]

# Create a test DataFrame with the defined schema

test_df = spark.createDataFrame(data_to_ingest, schema=schema)

ingested_df = spark.createDataFrame(ingested_data, schema=schema)

expected_df_1_col = spark.createDataFrame(expected_result_1_col, schema=schema)

expected_df_2_col = spark.createDataFrame(expected_result_2_col, schema=schema)

df_1_col = check_ingestion_status(test_df, ingested_df, ["id1"], "silver_ingestion_status")

df_2_col = check_ingestion_status(test_df, ingested_df, ["id1", "id2"], "silver_ingestion_status")

assert df_1_col.toPandas().equals(expected_df_1_col.toPandas())
assert df_2_col.toPandas().equals(expected_df_2_col.toPandas())



# COMMAND ----------

#test identify_duplicates
schema = StructType([
    StructField("id1", IntegerType(), True),
    StructField("id2", IntegerType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("validation_status", StringType(), True),
    StructField("invalid_columns", ArrayType(StringType()), True),
    StructField("silver_ingestion_status", StringType(), True),
    StructField("is_duplicate", StringType(), True),
])

# Sample data for testing

test_data_1_col = [
    (1, 1, '2024-01-13 12:00:00', '', [], "ingested", ""),
    (2, 1, '2024-02-14 12:01:00', '', [], "not_ingested", ""),
    (3, 1, '2024-02-14 12:02:00', '', [], "not_ingested", ""),
    (1, 1, '2024-02-14 12:03:00', '', [], "ingested", ""),
    (1, 2, '2024-02-14 12:00:00', '', [], "ingested", ""),
    (4, 1, '2024-02-14 12:03:00', 'invalid', ['name'], "not_ingested", ""),
    (2, 1, '2024-02-14 12:01:00', '', [], "not_ingested", ""),
]

expected_data_1_col = [
    (1, 1, '2024-01-13 12:00:00', '', [], "ingested", "False"),
    (2, 1, '2024-02-14 12:01:00', 'invalid', ["id1"], "not_ingested", "True"),
    (3, 1, '2024-02-14 12:02:00', '', [], "not_ingested", "False"),
    (1, 1, '2024-02-14 12:03:00', 'invalid', ['id1'], "ingested", "True"),
    (1, 2, '2024-02-14 12:00:00', 'invalid', ['id1'], "ingested", "True"),
    (4, 1, '2024-02-14 12:03:00', 'invalid', ['name'], "not_ingested", "False"),
    (2, 1, '2024-02-14 12:01:00', 'invalid', ['id1'], "not_ingested", "True"),
]


test_data_2_col = [
    (1, 1, '2024-02-13 12:00:00', '', [], "ingested", ""),
    (2, 1, '2024-02-14 12:01:00', '', [], "not_ingested", ""),
    (3, 1, '2024-02-14 12:02:00', '', [], 'not_ingested', ""),
    (1, 1, '2024-02-14 12:03:00', '', [], 'ingested', ""),
    (1, 2, '2024-02-14 12:00:00', '', [], 'ingested', ""),
    (4, 1, '2024-02-14 12:03:00', 'invalid', ['name'], 'not_ingested', ""),
    (2, 1, '2024-02-14 12:01:00', '', [], "not_ingested", ""),
]

expected_data_2_col = [
    (1, 1, '2024-02-13 12:00:00', '', [], 'ingested', "False"),
    (2, 1, '2024-02-14 12:01:00', 'invalid', ['id1, id2'], 'not_ingested', "True"),
    (3, 1, '2024-02-14 12:02:00', '', [], 'not_ingested', "False"),
    (1, 1, '2024-02-14 12:03:00', 'invalid', ['id1, id2'], 'ingested', "True"),
    (1, 2, '2024-02-14 12:00:00', '', [], 'ingested', "False"),
    (4, 1, '2024-02-14 12:03:00', 'invalid', ['name'], 'not_ingested', "False"),
    (2, 1, '2024-02-14 12:01:00', 'invalid', ["id1, id2"], 'not_ingested', "True"),
]

test_df_1_col = spark.createDataFrame(test_data_1_col, schema=schema)
test_df_1_col = test_df_1_col.withColumn("is_duplicate", col("is_duplicate").cast(BooleanType()))
expected_df_1_col = spark.createDataFrame(expected_data_1_col, schema=schema)
expected_df_1_col = expected_df_1_col.withColumn("is_duplicate", col("is_duplicate").cast(BooleanType()))


test_df_2_col = spark.createDataFrame(test_data_2_col, schema=schema)
test_df_2_col = test_df_2_col.withColumn("is_duplicate", col("is_duplicate").cast(BooleanType()))
expected_df_2_col = spark.createDataFrame(expected_data_2_col, schema=schema)
expected_df_2_col = expected_df_2_col.withColumn("is_duplicate", col("is_duplicate").cast(BooleanType()))


df_1_col = identify_duplicates(validation_df=test_df_1_col, id_columns=["id1"], ingestion_timestamp="ingestion_timestamp", ingestion_status_col="silver_ingestion_status")
df_2_col = identify_duplicates(validation_df=test_df_2_col, id_columns=["id1", "id2"], ingestion_timestamp="ingestion_timestamp", ingestion_status_col="silver_ingestion_status")

df_1_col_sorted = df_1_col.orderBy(["id1", "id2", "ingestion_timestamp"])
expected_df_1_col_sorted = expected_df_1_col.orderBy(["id1", "id2", "ingestion_timestamp"])

df_2_col_sorted = df_2_col.orderBy(["id1", "id2", "ingestion_timestamp"])
expected_df_2_col_sorted = expected_df_2_col.orderBy(["id1", "id2", "ingestion_timestamp"])


assert df_1_col_sorted.toPandas().equals(expected_df_1_col_sorted.toPandas())
assert df_2_col_sorted.toPandas().equals(expected_df_2_col_sorted.toPandas())



# COMMAND ----------

