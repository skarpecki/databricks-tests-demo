# Unit tests
---

## Content

This catalog contains sample unit tests of a spark functions. You'll need Python in version at least 3.11 to run provided code. Additionaly
to use code without any further modifications, Databricks workspace is required.

### Files

- **conftest.py** - PyTest configuration
- **data_checks.py** - library with data checks. It's duplicate of the one you can find in *data_checks* catalog
- **readme.md** - this file
- **requirements.txt** - packages needed to run these tests
- **test_data_checks.py** - tests of data_checks module
- **test_utils.py** - tests of common utils
- **utils.py** - common utilities used within many projects.


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