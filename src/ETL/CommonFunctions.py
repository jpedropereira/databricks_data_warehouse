# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import (
    array,
    array_contains,
    broadcast,
    col,
    concat,
    concat_ws,
    count,
    desc,
    expr,
    initcap,
    lit,
    month,
    regexp_replace,
    round,
    row_number,
    trim,
    udf,
    upper,
    weekofyear,
    when,
    year,
)
from pyspark.sql.types import BooleanType, LongType, StringType, TimestampType
from pyspark.sql.window import Window
import re

# COMMAND ----------

def authenticate_to_storage():
    spark.conf.set(f"fs.azure.account.auth.type.{SA_NAME}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{SA_NAME}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{SA_NAME}.dfs.core.windows.net", SAS_TOKEN)

# COMMAND ----------

def condition_builder(columns, ingested_alias, updates_alias):
    """Builds condition for ingesting records. It ignores columns containing the ingestion time
        Args:
        columns (list): Columns in the dataframe for which to build the condition
        ingested_alias (str): Alias used for the table containing the ingested data.
        updates_alias (str): Alias used for the dataframe containing the updates.

    Returns:
        str: A string containing the condition required to ingest the data. 
    
    """

    condition = ""

    for column in columns:
        if "ingestion_time" in column:
            pass
        else:
            condition += f"{ingested_alias}.{column} = {updates_alias}.{column} AND "
    
    return condition[:-5]


# COMMAND ----------

def build_insert_columns_dict(columns, source_alias):
    """
    Builds the dictionary required for inserting or updating records into delta table.
        Args:
            columns: list of columns in source dataframe.
            source_alias: string used as alias for source dataframa
    """
    columns_dict = {column: f"{source_alias}.{column}" for column in columns}

    return columns_dict

# COMMAND ----------

def clean_string_in_df(dataframe, column_to_clean, new_column=None, is_address=False):
    """Clean and standardize strings in a DataFrame column.

    This function removes leading, trailing, and consecutive spaces, special characters, and capitalizes the first letter of each word in a column containing strings. 
    Optionally, it can create a new column to store the cleaned strings.

    Args:
        dataframe (DataFrame): The dataFrame containing the column to be cleaned.
        column_to_clean (str): The name of the column to be cleaned.
        new_column (str, optional): The name of the new column to store the cleaned strings. If not provided, the original column will be modified. Defaults to None.
        is_address (bool, optional): Specifies whether the column contains addresses. If True, additional cleaning specific to addresses is performed. Defaults to False.

    Returns:
        DataFrame: A new Spark DataFrame with the specified column cleaned and standardized.

    Notes:
        - If 'new_column' is not provided, the original column will be modified.
        - If 'is_address' is True, additional cleaning specific to addresses is performed, such as replacing special characters with spaces and handling slashes (/).

    """
    if new_column is None:
        new_column = column_to_clean
    
    dataframe = dataframe.withColumn(new_column, col(column_to_clean))    
    dataframe = dataframe.withColumn(new_column, regexp_replace(col(new_column), "\t", " "))  # Replaces tabs with spaces
    
    #Replaces special signs with spaces
    if is_address:
        #replaces any charather non alphanumeric or / with a space
        dataframe = dataframe.withColumn(new_column, regexp_replace(col(new_column), "[^a-zA-Z0-9 /]", " "))
        #replaces any / that is not in between two number with a space
        dataframe = dataframe.withColumn(new_column, regexp_replace(col(new_column), "(?<![0-9])/(?![0-9])", " "))

    else:
        dataframe = dataframe.withColumn(new_column, regexp_replace(col(new_column), "[^a-zA-Z ]", " ")) 
    
    dataframe = dataframe.withColumn(new_column, initcap(col(new_column)))  # Capitalizes first letter in each word
    dataframe = dataframe.withColumn(new_column, regexp_replace(col(new_column), "\\s+", " ")) #Removes consecutive spaces
    dataframe = dataframe.withColumn(new_column, trim(dataframe[new_column])) 

    return dataframe

    

# COMMAND ----------

def validate_column(dataframe, column_to_validate, datatype, comparison_column=None, accepted_values=None):
    """Validates records for a specific column in a DataFrame.

    Args:
        dataframe (DataFrame): The DataFrame containing the records to be validated.
        column_to_validate (str): The name of the column to be validated.
        datatype (str): The datatype of the column to be validated. Supported datatypes are "string", "numeric", or "timestamp".
        comparison_column (str, optional): The name of the column against which the values of 'column_to_validate' will be compared for validation. Defaults to None.
        accepted_values (list, optional): A list of accepted values against which the values of 'column_to_validate' will be validated. Defaults to None.
        
    Returns:
        DataFrame: A new DataFrame with updated validation status and invalid columns.
        
    Raises:
        ValueError: If the specified 'datatype' is not supported or if invalid arguments are provided.

    Notes:
        - For strings, validation can be performed either against another column containing correct values ('comparison_column') or against a list of accepted values ('accepted_values').
        - For numeric columns, validation checks if column contains any record that cannot be cast as a number.
        - For timestamp columns, validation checks  if column contains any record that cannot be cast as a timestamp.
    """
    
    invalid_condition = None

    if datatype == "string":

        if comparison_column is None and accepted_values is None:
            raise ValueError("For strings, you need to pass as argument either a column with correct values or a list of accepted values")

        if comparison_column:
            invalid_condition = (col(column_to_validate) == "") | (col(column_to_validate) != col(comparison_column))
        elif accepted_values:
            invalid_condition = (~col(column_to_validate).isin(accepted_values))

    elif datatype == "numeric":
        invalid_condition = col(column_to_validate).cast(LongType()).isNull()

    elif datatype == "timestamp":
        invalid_condition = col(column_to_validate).cast(TimestampType()).isNull()
    
    else:
        raise ValueError("datatype passed as argument is not allowed. Please use: 'string', 'numeric' or 'timestamp'")

    dataframe = dataframe.withColumn('validation_status', when(invalid_condition, lit('invalid')).otherwise(dataframe['validation_status'])) \
       .withColumn('invalid_columns', when(invalid_condition, array(concat_ws(', ', lit(column_to_validate), 'invalid_columns'))).otherwise(dataframe['invalid_columns']))

    return dataframe


# COMMAND ----------

def validate_relationship(dataframe1, id_col_1, dataframe2, id_col_2):
    """
    Validates that id_col_1 of dataframe1 contains ids that exist in id_col_2 of dataframe2.
    
    Args:
        dataframe1 (DataFrame): The first DataFrame.
        id_col_1 (str): The name of the ID column in dataframe1.
        dataframe2 (DataFrame): The second DataFrame.
        id_col_2 (str): The name of the ID column in dataframe2.
    
    Returns:
        DataFrame: The validated DataFrame with additional columns for validation status and invalid relationship.
    """
    id_col_2_list = dataframe2.select(id_col_2).distinct().rdd.flatMap(lambda x: x).collect()

    invalid_condition = ~col(id_col_1).isin(id_col_2_list)

    # Apply the validation and update the DataFrame
    verified_dataframe = dataframe1.withColumn('validation_status',
                                               when(invalid_condition, lit('invalid')).otherwise(dataframe1['validation_status'])) \
                                   .withColumn('invalid_relationship',
                                               when(invalid_condition, array(concat_ws(', ', lit(id_col_1))))
                                               .otherwise(dataframe1['invalid_relationship']))

    return verified_dataframe


# COMMAND ----------

def check_ingestion_status(validation_df, ingested_df, id_columns, ingestion_status_col="silver_ingestion_status"):
    """
    Checks if records have been ingested based on id column(s). It labels each record as 'ingested' or 'not_ingested'. 
    
    Args:
        validation_df (DataFrame): The Spark DataFrame to validate for ingestion.
        ingested_df (DataFrame): The Spark DataFrame containing ingested records.
        id_columns (list): A list of column names or a single column name containing the records id.
        status_column_name (str): A string with the name of the column containing ingestion status. Defaults to 'silver_ingestion_status'.

    Returns:
        DataFrame: A DataFrame validated for ingestion status of records based on the specified id column(s).

    """

    #build column with concatinated ids
    validation_df = validation_df.withColumn("concat_id", lit(""))
    ingested_df = ingested_df.withColumn("concat_id", lit(""))

    # Concatenate id columns into a single column
    for id_column in id_columns:
        validation_df = validation_df.withColumn("concat_id", concat_ws("|", col("concat_id"), col(id_column)))
        ingested_df = ingested_df.withColumn("concat_id", concat_ws("|", col("concat_id"), col(id_column)))

    # Collect ingested_ids_list from ingested_df
    ingested_ids_list = ingested_df.select("concat_id").rdd.flatMap(lambda x: x).collect()

    # Broadcast ingested_ids_list to improve join performance
    ingested_ids_broadcast = spark.sparkContext.broadcast(ingested_ids_list)

    # Use a UDF to check if concat_id is in ingested_ids_list
    check_ingested_udf = udf(lambda x: x in ingested_ids_broadcast.value, BooleanType())

    # Label ingestion_status based on concat_id membership in ingested_ids_list
    validation_df = validation_df.withColumn(ingestion_status_col, when(check_ingested_udf(col("concat_id")), "ingested").otherwise("not_ingested"))

    validation_df = validation_df.drop("concat_id")

    return validation_df





# COMMAND ----------

def identify_duplicates(validation_df, id_columns, ingestion_timestamp, ingestion_status_col="silver_ingestion_status"):
    """
    Validates if function is duplicate based on id column(s).
    Records with unique ids are all labeled as valid.
    For duplicated records, function checks the ingestion status. If the record id has been ingested, the earliest record does not have any update in validation_status and remaining as invalid, with id columns added to invalid columns. If id has not been ingested, all records containing it are labeled as invalid.
    For invalid records, id column(s) are added to the invalid_columns list.
    A column named is_duplicate is added with status True and False.

    Args:
        validation_df (Dataframe): The Spark DataFrame to validate duplicates based on id column(s).
        id_columns (list): A list of column names or a single column name containing the records id.
        ingestion_timestamp (str): A string with the name of the column containing the ingestion timestamp.
        ingestion_status_col (str): A string with the name of the column containing the ingestion status. Defaults to 'silver_ingestion_status'.

    Returns:
        DataFrame: A DataFrame validated for record uniqueness based on id column(s).

    """
    
    id_cols_str = ", ".join(id_columns) 

    #add is_duplicate column
    validation_df = validation_df.withColumn("is_duplicate", lit(""))

    # build window by id
    id_window = Window.partitionBy(id_columns).orderBy(col(ingestion_timestamp))

    # Add a new column containing the running count of an id ordered by ingestion time. 
    validation_df = validation_df.withColumn("running_id_count", row_number().over(id_window))

    # Sum running_id_count for each id
    count_window = Window.partitionBy(id_columns)
    
    validation_df = validation_df.withColumn("count_id", count("*").over(count_window))

    # Label invalid records
    validation_df = validation_df.withColumn('validation_status',
                                            when((col(ingestion_status_col) == "not_ingested") & (col("count_id") > 1), lit('invalid'))
                                            .otherwise(col('validation_status'))) \
                                 .withColumn('invalid_columns',
                                            when((col(ingestion_status_col) == "not_ingested") & (col("count_id") > 1),
                                                array(concat_ws(', ', lit(id_cols_str))))
                                            .otherwise(col('invalid_columns'))) \
                                 .withColumn('is_duplicate',
                                            when((col(ingestion_status_col) == "not_ingested") & (col("count_id") > 1), lit("true"))
                                            .otherwise(col('is_duplicate')))

    validation_df = validation_df.withColumn('validation_status',
                                            when((col(ingestion_status_col) == "ingested") & (col("count_id") > 1) & (col("running_id_count") > 1), lit('invalid'))
                                            .otherwise(col('validation_status'))) \
                                 .withColumn('invalid_columns',
                                            when((col(ingestion_status_col) == "ingested") & (col("count_id") > 1) & (col("running_id_count") > 1),
                                                array(concat_ws(', ', lit(id_cols_str))))
                                            .otherwise(col('invalid_columns'))) \
                                 .withColumn('is_duplicate',
                                            when((col(ingestion_status_col) == "ingested") & (col("count_id") > 1) & (col("running_id_count") > 1), lit("true"))
                                            .otherwise(col('is_duplicate')))

    validation_df = validation_df.drop("count_id")
    validation_df = validation_df.drop("running_id_count")
    
    validation_df = validation_df.withColumn("is_duplicate", when(col("is_duplicate")=="true", col("is_duplicate")).otherwise("false"))
    validation_df = validation_df.withColumn("is_duplicate", col("is_duplicate").cast(BooleanType()))

    return validation_df

# COMMAND ----------

