from dataclasses import dataclass
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import os

@dataclass
class DataCheckClause:
    sql: str
    message: str


@dataclass
class DataCheck:
    catalog: str
    schema: str
    table: str
    data_checks: list[DataCheckClause]


def load_data_checks(
    spark, 
    source_system: str, 
    catalog: str, 
    schema: str, 
    table: str
) -> DataCheck:
    """
    Reads data check configuration for a specific table from JSON files.

    :param spark - SparkSession object.
    :param source_system - Name of the source system.
    :param catalog - Catalog name.
    :param schema - Schema name.
    :param table - Table name.

    :returns Datacheck - DataCheck configuration object for the specified table.
    """
    # If using the code deploy it to Volume
    cfg_path = f"file:{os.path.abspath(f'./data_checks_cfg/{source_system}')}"
    df = spark.read.option("multiline", "true").json(f"{cfg_path}/*.json")

    df = df.filter(
        (F.col("catalog") == catalog) &
        (F.col("schema") == schema) &
        (F.col("table") == table)
    )

    # Assuming there is exactly a single row for a config
    # To productionize make some checks here and verbose exceptions
    row = df.first()

    data_check = DataCheck(
        catalog = row["catalog"],
        schema = row["schema"],
        table = row["table"],
        data_checks = row["data_checks"]
    )

    return data_check

def apply_data_checks(df: DataFrame, cfg: DataCheck) -> DataFrame:
    """
    Apply data quality checks to a DataFrame and capture failure messages.

    Each data check is represented by a SQL expression and an associated 
    message. If a row fails a check, the corresponding message is appended 
    to the `fail_messages` column. Only rows with at least one failure 
    message are returned.

    :param df (DataFrame): Input Spark DataFrame.
    :param cfg (DataCheck): Configuration object containing a list of 
        data checks (each with `sql` and `message`).

    :returns DataFrame: DataFrame containing only rows that failed one or more checks, 
        with a `fail_messages` column listing the reasons.
    """

    # Prepare array of checks
    checks = F.array(*[
        F.struct(
            F.expr(check.sql).alias("fail"),
            F.lit(check.message).alias("msg")
        )
        for check in cfg.data_checks
    ])
    
    # Base empty arrary for fail messafes
    empty = F.expr("cast(array() as array<string>)")

    # Aggegate will go over checks array and apply it
    # with when(...).otherwise(...). If clause is checked
    # it adds message to the messages array.
    fail_messages = F.aggregate(
        checks,
        empty,
        lambda acc, x: F.when(
                x["fail"],
                F.concat(acc, F.array(x["msg"]))
            ).otherwise(acc)
    )

    # Return incorrect rows - where fail_message is filled
    return (
        df.withColumn("fail_messages", fail_messages)
          .where(F.size("fail_messages") > 0)
    )