from pyspark.testing.utils import assertDataFrameEqual 
from utils import deduplicate_df


def test_deduplicate_df_keeps_latest_per_partition(spark):
    df = spark.createDataFrame(
        [
            {"id": 1, "ts": 1, "val": "a"},
            {"id": 1, "ts": 2, "val": "b"},  # latest for id=1
            {"id": 2, "ts": 3, "val": "x"},
        ]
    )

    out = deduplicate_df(df, partiton_by_cols=["id"], order_by_cols="ts")

    expected = spark.createDataFrame(
        [
            {"id": 1, "ts": 2, "val": "b"},
            {"id": 2, "ts": 3, "val": "x"},
        ]
    )
    
    assertDataFrameEqual(out, expected, ignoreColumnOrder=True, checkRowOrder=False)


def test_deduplicate_df_multiple_partitions(spark):
    df = spark.createDataFrame(
        [
            {"group": "A", "id": 1, "ts": 1, "val": "a1"},
            {"group": "A", "id": 1, "ts": 3, "val": "a3"},  
            {"group": "A", "id": 2, "ts": 2, "val": "a2"}, 
            {"group": "B", "id": 1, "ts": 4, "val": "b4"},  
            {"group": "B", "id": 1, "ts": 2, "val": "b2"},
        ]
    )

    out = deduplicate_df(df, partiton_by_cols=["group", "id"], order_by_cols="ts")

    expected = spark.createDataFrame(
        [
            {"group": "A", "id": 1, "ts": 3, "val": "a3"},
            {"group": "A", "id": 2, "ts": 2, "val": "a2"},
            {"group": "B", "id": 1, "ts": 4, "val": "b4"},
        ]
    )

    assertDataFrameEqual(out, expected, ignoreColumnOrder=True, checkRowOrder=False)