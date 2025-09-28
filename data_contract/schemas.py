from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Customer (bronze layer) schema
# Usually you would get this via spark.read.table("bronze.schema.customer").schema
# But here we simplify the process
customer_schema = StructType([
    StructField("id",       StringType(),   False),
    StructField("name",     StringType(),   True),
    StructField("age",      IntegerType(),  True),
    StructField("country",  StringType(),   True),
])

# Log of files movement and issues
log_schema = StructType([
    StructField("old_file_path",     StringType(),      True),
    StructField("new_file_path",     StringType(),      True),
    StructField("corrupted_message", StringType(),      True),
    StructField("inserted_at",       TimestampType(),   True),
    StructField("move_status",       StringType(),      True),
])