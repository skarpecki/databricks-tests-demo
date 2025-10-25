# Integration tests
---

## Content

This catalog contains sample integrations tests of Databricks notebooks. You'll need Python in version at least 3.11 to run provided code. Additionaly
to use code without any further modifications, Databricks workspace is required.

### Files

- **conftest.py** - PyTest configuration
- **mocks.py** - mocks needed to run integration tests
- **readme.md** - this file
- **registry.py** - helper class for tests, to mock up DataFrames
- **requirements.txt** - packages needed to run tests
- **test_notebook.py** - tests of a notebook
- **notebooks\notebook.py** - sample Databricks notebook being tested
- **notebooks\utils.py** - additional utilites defined as a Databricks notebook, used by the notebook.py

## How to run it

1. Make sure Azure CLI is available
2. `az login` - authenticate azure
3. Define DEFAULT Databricks host
   - With ~/.databrickscfg in Linux or C:/Users/username/.databricksfg in Windows
   ```ini
   [DEFAULT]
   host = https://adb-123456789.1.azuredatabricks.net
   ```
   - Through Enviornment variable `DATABRICKS_HOST=https://adb-123456789.1.azuredatabricks.net`
4. Install required depdencies (best in venv) - `pip install -r requirements.txt`.
5. Run `pytest --disable-warnings` - we disable warnings since pyspark itself creates many of them.

Those tests by default run on Serverless cluster. This behaviour can be changed in **conftest.py** by changing the cluster option. 