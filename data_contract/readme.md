# Data Contracts


## Content

This catalog contain sample idea on how to use data contracts within Databricks platform for loading data from 
landing zone to bronez layer in Lakehouse. These code is intended to be run on Databricks.

### Files

- **00_src_data\\** - catalog with sample files. They are stored in repo for simplification
- **01_create_source_files** - Databricks notebook to generate some sample data
- **02_apply_data_contract** - Databricks notebook to apply data contract logic
- **readme.md** - this file
- **schemas.py** - schemas defining a Data Contract. Usually this would be schema of a Delta table.

## Hot to use it

This code is intended to be run in Databricks workspace on an interactive cluster. Run the notebooks in order:

1. 01_create_source_files
2. 02_apply_data_contract