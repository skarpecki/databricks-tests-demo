# Databricks notebook source
# MAGIC %md
# MAGIC ### Prepare widgets to parametrize notebook

# COMMAND ----------

param_obj_name = 'customer' # hard-coded object name

dbutils.widgets.dropdown(
    "param_format",
    "parquet",
    ["parquet", "json", "avro"],
    "Format"
)
param_format = dbutils.widgets.get("param_format")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect files in directory to later apply a contract to them

# COMMAND ----------

from pyspark.testing.utils import assertSchemaEqual
from pyspark.sql.functions import col
import re
import os

# Base listing to support text formats.
# This can be done parallel via ThreadPoolExecutor,
# but here kept simple on purpose
def _detect_files_directories(
    obj_name,
    fmt,
    start_year,
    start_month,
    start_day
):
    root_dir = f"file:{os.path.abspath('./00_src_data/')}"
    start_dir = f"{root_dir}/object={obj_name}/"
    stack = [ start_dir ]
    files = []
    start_date = int(f"{start_year}{str(start_month).zfill(2)}{str(start_day).zfill(2)}")
    hive_date_pattern = re.compile(r"/year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})(?:/|$)")
    while stack:
        curr = stack.pop()
        try:
            for entry in dbutils.fs.ls(curr):
                if entry.isDir():
                    stack.append(entry.path)
                elif entry.name.endswith(f".{fmt}"):
                    m = hive_date_pattern.search(entry.path)
                    y, mo, d = m.group(1), m.group(2), m.group(3)
                    curr_date = int(f"{y}{mo.zfill(2)}{d.zfill(2)}")
                    if curr_date < start_date:
                        continue
                    files.append(entry.path)
        except Exception as e:
                print(f"[WARN] Could not list {curr}: {e}")

    return files

# Spark will be much faster than previous single threaded listing
def _detect_files_spark(
    obj_name,
    fmt,
    start_year,
    start_month,
    start_day
):
    root_dir = f"file:{os.path.abspath('./00_src_data/')}"
    start_dir = f"{root_dir}/object={obj_name}/"

    df = spark.read.format(fmt).load(start_dir)

    # Prune partitions only - no data read at all here
    cond = (
        (col("year") >= start_year)
        | ((col("year") == start_year) & (col("month") >= start_month))
        | ((col("year") == start_year) & (col("month") == start_month) & (col("day") >= start_day))
    )
    df_pruned = df.filter(cond)

    return df_pruned.inputFiles()


def detect_files(
    obj_name,
    fmt,
    start_year,
    start_month,
    start_day
):
    # For text formats Spark must read data to detect schema
    # thus it's faster to just go over dictionaris
    if fmt in ['csv', 'json']:
        return _detect_files_directories(
            obj_name,
            fmt,
            start_year,
            start_month,
            start_day
        )
    
    return _detect_files_spark(
            obj_name,
            fmt,
            start_year,
            start_month,
            start_day
        )

files = detect_files(
    param_obj_name,
    param_format,
    2000,
    1,
    1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply data contract to detected files

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from schemas import customer_schema

# Usually we'll take schema for bronze
# bronze_df = spark.read.table(f"cdp_temporary_dev.tmp.bronze_customer")
# contract_schema = bronze_df.schema

# Bet here we'll use the one that is already defined
contract_schema = customer_schema

def verify_file(entry_path, format, contract_schema):
    try:
        df = spark.read.format(format).load(entry_path)
        # Force calculation - check if can read
        df.take(1)

        # Check schema equal
        assertSchemaEqual(df.schema, contract_schema)
    except Exception as e:
        return {entry_path: str(e)}
    
with ThreadPoolExecutor() as executor:
    incorrect_files = [
        res
        for res
        in list(executor.map(lambda x: verify_file(x, "parquet", contract_schema), files))
        if res is not None
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Move files to _corrupted dir

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

def move_corrupted_files(item, object_name):
    src_file_path, reason = item
    try:
        tgt_file_path = src_file_path.replace(
            f"/object={object_name}/", 
            f"/object={object_name}_corrupted/",
            1)
        
        # Make sure directory exists. rsplit to remove filename
        dbutils.fs.mkdirs(tgt_file_path.rsplit("/", 1)[0])
        dbutils.fs.mv(src_file_path, tgt_file_path)

        return {
            "old_file_path": src_file_path,
            "new_file_path": tgt_file_path,
            "corrupted_message": reason,
            "move_status": "OK"
        }
    except Exception as e:
        return {
            "old_file_path": src_file_path,
            "new_file_path": tgt_file_path,
            "corrupted_message": reason,
            "move_status": e
        }


items = [next(iter(d.items())) for d in incorrect_files]

with ThreadPoolExecutor() as executor:
    mv_results = list(executor.map(lambda x: move_corrupted_files(x, "customer"), items))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display results of data contract apply and movement of files. In production you want to write that into a log table

# COMMAND ----------

from pyspark import StorageLevel
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp
from schemas import log_schema

results_df = spark.createDataFrame(mv_results, schema=log_schema)
results_df.persist()

try:
    not_ok_df = results_df.where("move_status != 'OK'")

    has_errors = not_ok_df.limit(1).count() > 0
    if has_errors:
        display(not_ok_df)
        raise Exception("Some files couldn't be moved")

    display(
        results_df
        .withColumn("inserted_at", current_timestamp())
    )
finally:
    results_df.unpersist()
