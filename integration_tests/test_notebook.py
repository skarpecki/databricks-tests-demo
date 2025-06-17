from pyspark.sql import connect
from pyspark.testing.utils import assertDataFrameEqual
from importlib import import_module
import pytest
from registry import DataframesRegistry
from mocks import (
    DBUtilsMock,
    SparkReaderMock,
    DeltaTableMockGenerator,
    DataStreamWriterMock
)

# Import notebook as a module
module = import_module("notebooks.utils")

@pytest.mark.parametrize(
    "notebook_file",
    ["notebooks/notebook.py"],
)
def test_ntbk_py(mocker, spark, notebook_file):
    """
    Sample test method

    :param mocker - pytest mocker - to extend mocking functionality
    :param spark - fixture defined in coftests.py
    :param notebook_file - .py notebook to be tested
    """
    # ---- Prepare testing context start ---- 
    # Can be abstracted into a seperate prepare class

    # Classes to be patched based on the Spark library in use
    # It's since tests can be called from Spark/SparkConnect/DatabricksConnect
    # Temp DataFrame to obtain classes
    df_tmp = spark.createDataFrame([], "x int")

    SparkCls                 = type(spark)
    SparkConfCls             = type(spark.conf)
    DataFrameCls             = type(df_tmp)
    DataFrameWriterCls       = type(df_tmp.write)

    # No longer needed
    del df_tmp

    # Mock classes
    df_registry = DataframesRegistry()
    dbutils_mock = DBUtilsMock()
    spark_reader_mock = SparkReaderMock(df_registry)
    dt_mock_gen = DeltaTableMockGenerator(df_registry)

    # Collections to track calls
    writes = {}
    spark_conf = {}
    sql_commands = list()

    # Save DataFrame interceptor
    def _save_patch(df_writer, name: str, mode=None):
        plan = getattr(df_writer, "_df", None)              # Get DataFrame from plan attribute
        df = connect.dataframe.DataFrame(plan, spark)       # Create DataFrame out of plan
        writes[name] = df                                   # Save into a dictionary
        return None                                         # saveAsTable returns None

    # Patch DeltaTable 
    mocker.patch("delta.tables.DeltaTable.forPath",     new=dt_mock_gen.forPath)
    mocker.patch("delta.tables.DeltaTable.forName",     new=dt_mock_gen.forName)

    # Patch spark.conf.set
    mocker.patch.object(
        SparkConfCls,
        "set",
        autospec=True,
        side_effect=lambda _, param, val: spark_conf.update({param: val})
    )

    # spark.readStream
    mocker.patch.object(
        SparkCls,             
        "read",
        new=spark_reader_mock,
    )

    # spark.readStream
    mocker.patch.object(
        SparkCls,             
        "readStream",
        new=spark_reader_mock,
    )

    # df.writeStream
    mocker.patch.object(
        DataFrameCls,         
        "writeStream",              
        new=property(lambda self: DataStreamWriterMock(self))
    )

    # df.saveAsTable
    mocker.patch.object(
        DataFrameWriterCls,
        "saveAsTable",
        autospec=True,
        side_effect=_save_patch
    )

    # df.save
    mocker.patch.object(
        DataFrameWriterCls,
        "save",
        autospec=True,
        side_effect=_save_patch
    )

    # df.sql
    mocker.patch.object(
        SparkCls,
        "sql",
        autospec=True,
        side_effect=lambda cmd, *a, **k: sql_commands.append(cmd)
    )

    # df.(un)persist
    persist_mock   = mocker.patch.object(DataFrameCls, "persist",   return_value=None)
    unpersist_mock = mocker.patch.object(DataFrameCls, "unpersist", return_value=None)

    # ---- Prepare testing context end ----
     
    # ---- Prepare testing 
    # Raw data to be processed in notebook
    df_users_raw  = spark.createDataFrame([
        {"id": 1,     "name": "Alice",      "city": "NY",       "effective_date": "2023-01-01"},
        {"id": 1,     "name": "Alice",      "city": "Boston",   "effective_date": "2023-02-01"},    # City updated for #1
        {"id": 2,     "name": "Bob",        "city": "LA",       "effective_date": "2023-01-15"},
        {"id": 2,     "name": "Bob",        "city": "SF",       "effective_date": "2023-03-01"},    # City updated for #2
        {"id": 3,     "name": "Charlie",    "city": "Chicago",  "effective_date": "2023-02-10"},
        {"id": 4,     "name": None,         "city": "Chicago",  "effective_date": "2023-02-12"},    # Bad record on Id
        {"id": None,  "name": None,         "city": "Chicago",  "effective_date": "2023-02-10"},    # Bad record on ID and Name
    ])

    # Expected 
    df_users_expected = spark.createDataFrame([
        {"id": 1, "name": "Alice",      "city": "Boston",           "effective_date": "2023-02-01"},
        {"id": 2, "name": "Bob",        "city": "San Francisco",    "effective_date": "2023-03-01"},
        {"id": 3, "name": "Charlie",    "city": "Chicago",          "effective_date": "2023-02-10"}
    ])

    df_users_bad_records = spark.createDataFrame([
        {"id": 4,     "name": None,         "city": "Chicago",  "effective_date": "2023-02-12", "fail_messages": ["Name cannot be NULL"]},
        {"id": None,  "name": None,         "city": "Chicago",  "effective_date": "2023-02-10", "fail_messages": ["Name cannot be NULL", "ID cannot be NULL"]},
    ],
    schema = "id LONG, name STRING, city STRING, effective_date STRING, fail_messages ARRAY<STRING>")

    df_city_cfg = spark.createDataFrame([
        {"city_abb": "SF", "city": "San Francisco"},
    ])

    df_registry.register_mock_dataframe(df_users_raw, "raw.sap.users")
    df_registry.register_mock_dataframe(df_city_cfg, "raw.sap.city_mappings")

    parameters = {
        "src_table": "raw.sap.users",
    }

    dbutils_mock.update_widgets_parameters(parameters)

    # Prepare _globals for a notebook
    _globals = {
        "__name__": "__main__",                         # Default one
        "spark": spark,                                 # Spark instance
        "dbutils": dbutils_mock,                        # Mock dbutils
        "deduplicate_df": module.deduplicate_df,        # Method from magic %run
    }

    with open(notebook_file) as f:
        code = compile(f.read(), notebook_file, "exec") # (optional) Compile code into an object 
        exec(code, _globals)                         # Exec compiled code

    # Show written DataFrames - just for demo purpose
    for tab, df in writes.items():
        print(f"Written table: {tab}")
        df.show(truncate=False)
    
    # Check if no DataFrame hangs in memory
    assert persist_mock.call_count == unpersist_mock.call_count, \
            "Persist and Unpersist should be called the same number of times"

    # Assert if written DataFrame is correct
    assertDataFrameEqual(
        writes["gold.sap.users"],   # Actual - intercepted during saveAsTable  
        df_users_expected,          # Expected - prepared in the beginning
        checkRowOrder=False,        # Row order doesn't matter
        ignoreColumnOrder=True      # Check only column names
    )

    actual_dt_df = None

    for dt in dt_mock_gen.instances:
        if dt.identifier == "gold.sap.users":
            actual_dt_df = dt.merges[0].source

        # Show merged DeltaTables - for demo purposes
        for merge in dt.merges:
            print(f"Merged {dt.identifier}")
            merge.source.show(truncate=False) 

    assert actual_dt_df is not None, "No merges for gold.sap.users"

    # Assert if DataFrame to merge is correct
    assertDataFrameEqual(
        actual_dt_df,     
        df_users_expected,          
        checkRowOrder=False,        
        ignoreColumnOrder=True
    )