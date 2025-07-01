from pyspark.testing.utils import assertDataFrameEqual 
from data_checks import apply_data_checks

def test_apply_data_checks_appends_messages_and_filters(spark):
    df = spark.createDataFrame(
        [
            {"id": 1, "val": 10},     
            {"id": 2, "val": -5},     
            {"id": None, "val": 7},   
            {"id": 3, "val": -1},     
        ]
    )

    # Duck-typing - "If it walks like a duck and it quacks like a duck, then it must be a duck"
    class Clause:
        def __init__(self, sql: str, message: str):
            self.sql = sql
            self.message = message

    class FakeDataCheck:
        def __init__(self, clauses):
            self.data_checks = clauses

    cfg = FakeDataCheck(
        [
            Clause("val < 0", "val is negative"),
            Clause("id IS NULL", "id is null"),
        ]
    )

    out = apply_data_checks(df, cfg).select("id", "val", "fail_messages")

    expected = spark.createDataFrame(
        [
            {"id": None, "val": 7,  "fail_messages": ["id is null"]},
            {"id": 2,    "val": -5, "fail_messages": ["val is negative"]},
            {"id": 3,    "val": -1, "fail_messages": ["val is negative"]},
        ]
    )

    assertDataFrameEqual(out, expected, ignoreColumnOrder=True, checkRowOrder=False)