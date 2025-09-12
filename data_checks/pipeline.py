# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType 

# Prepare schema for sample data
schema = StructType([
    StructField("id",           IntegerType(),  True),
    StructField("firstname",    StringType(),   True),
    StructField("lastname",     StringType(),   True),
    StructField("email",        StringType(),   True),
])

# Prepare sample data
data = [
    (1,  "Alice",  "Smith",  "alice@example.com"),   # good
    (2,  None,     "Doe",    "john@example.com"),    # BAD: firstname is NULL
    (3,  "",       "Empty",  ""),                    # BAD: firstname is empty. email is empty
    (4,  "Carla",  "Stone",  None),                  # good (email not validated)
    (5,  None,     "Ng",     "ng@example.com"),      # BAD: firstname is NULL
    (6,  "Diego",  "Lopez",  "diego@example.com"),   # good
]

# Create sample DataFrame
input_df = spark.createDataFrame(data, schema)


# COMMAND ----------

from data_checks import apply_data_checks, load_data_checks

cfg = load_data_checks(
    spark, 
    "erp", 
    "silver", 
    "erp", 
    "customer"
)

df_bad_records = apply_data_checks(input_df, cfg)

df_correct = (
    input_df
    .join(
        df_bad_records.select("id").distinct(),
        on=input_df["id"].eqNullSafe(df_bad_records["id"]),  # null-safe join
        how="left_anti"
    )
)

# Here we display it. In production you would rather write it to 
display(df_correct)
display(df_bad_records)