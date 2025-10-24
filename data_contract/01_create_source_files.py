# Databricks notebook source
# MAGIC %md
# MAGIC ### Prepare widgets to parametrize notebook

# COMMAND ----------

dbutils.widgets.dropdown(
    "param_format",
    "parquet",
    ["parquet", "json", "avro"],
    "Format"
)
param_format = dbutils.widgets.get("param_format")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate random customer data in selected format in 00_src_data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import random
from uuid import uuid4
import os
from schemas import customer_schema

# Use file: to be able to work with workspace files
src_data = f"file:{os.path.abspath("./00_src_data/")}"

# Schema for breaking the contract
schema_with_city = StructType(customer_schema.fields + [
    StructField("city", StringType(), True),
])

names = ["Alice","Bob","Charlie","Diana","Ethan","Fiona","George","Hannah","Ivan","Julia"]
countries = ["USA","UK","Canada","Germany","France","Spain","Australia","India","Japan","Brazil"]
cities = ["New York","London","Paris","Tokyo","Sydney","Toronto","Berlin","Madrid","Rome","Dubai"]

for day in range(1, 10):
    if day == 6:
        # Add a city to customer schema. Column not in target schema - breaking the contract
        data = [
            (str(uuid4()), random.choice(names), random.randint(18, 70),
             random.choice(countries), random.choice(cities))
            for _ in range(10)
        ]
        df = spark.createDataFrame(data, schema_with_city)
    else:
        # All other days have correct data
        data = [
            (str(uuid4()), random.choice(names), random.randint(18, 70),
             random.choice(countries))
            for _ in range(10)
        ]
        df = spark.createDataFrame(data, customer_schema)

    # Write to a sink in workspace
    (
        df.write
          .mode("overwrite")
          .format(param_format)
          .save(f"{src_data}/object=customer/year=2025/month=10/day={day:02d}/{uuid4()}")
    )
