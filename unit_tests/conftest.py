import os
import pytest
from databricks.connect import DatabricksSession

# Set variable so PyArrow behaves in same way as Spark does (drop timezone info)
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

@pytest.fixture(scope="session")
def spark():
    """
    Base fixture to build Spark session. This can be tweaked outside of 
    code tests to fit the environment requirments.
    """
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    yield spark
    spark.stop()