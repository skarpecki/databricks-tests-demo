# Databricks notebook source

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window

def deduplicate_df(df, partition_by_cols, order_by_cols):
    # Keep latest record per id
    w = Window.partitionBy(partition_by_cols).orderBy(F.col(order_by_cols).desc())
    df_dedup = (
        df
            .withColumn("rn", F.row_number().over(w))
            .where(F.col("rn") == 1)
            .drop("rn")
    )

    return df_dedup
