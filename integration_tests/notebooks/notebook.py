# Databricks notebook source
# Notebook before refactor - usage of %run, code all around places

# COMMAND ----------

# MAGIC %run "../utils"

# COMMAND ----------

dbutils.widgets.text("src_table",  defaultValue="")
src_table = dbutils.widgets.get("src_table")


# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable

# Source tables
df_src      = spark.read.table(src_table)
df_city_cfg = spark.read.table("raw.sap.city_mappings")

df_dedup = deduplicate_df(
   df_src,
   partition_by_cols="id",
   order_by_cols="effective_date"
)

df_dedup = (
    df_dedup.where(
        (F.col("id").isNotNull()) &
        (F.col("name").isNotNull())
    )
)

cols_except_city = [
    c for c
    in df_dedup.columns
    if c != "city"
]

df_gold = (
    df_dedup.alias("src")
    .join(
        df_city_cfg.alias("cfg"),
        on=F.col("src.city") == F.col("cfg.city_abb"),
        how="left"
    )
    .select(
        *[ F.col(f"src.{c}") for c in cols_except_city ],
        F.coalesce(
            F.col("cfg.city"),
            F.col("src.city")
        ).alias("city")
    )
)

df_gold.show()

(
    df_gold
        .write
        .mode("overwrite")
        .saveAsTable("gold.sap.users")
)

# COMMAND ----------

target_table = DeltaTable.forName(spark, "gold.sap.users")

(
    target_table.alias("trg")
        .merge(
            df_gold.alias("src"),
            "src.merge_key = trg.merge_key"
        )
        .whenMatchedUpdate(
            condition = "condition",
            set = {"src_col": "tgt_col"}
        )
        .whenNotMatchedInsertAll()
        .execute()
)
