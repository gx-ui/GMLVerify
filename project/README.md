# GMLVerify - Data Validation Tool
GMLVerify is a data validation framework that helps you easily perform quality checks on data. Whether it's CSV files, database tables, or DataFrames, you can validate them with simple code.
## Core Features
- Multiple Data Source Support: Supports data validation for Pandas DataFrames, Spark DataFrames, CSV/Parquet files, and various databases (PostgreSQL, MySQL).
- Easy to Extend: New data sources and custom validation rules can be easily added.
- Automated Documentation: Validation results can automatically generate clear and easy-to-understand data documentation.
- Declarative Expectations: Define your data expectations using a simple declarative syntax.

## Quick Start
### 1. Environment Preparation
Ensure your environment meets the following requirements:
- Python 3.9+
- Great Expectations
### 2. Install Dependencies
```bash
pip install great-expectations python
```
(optional)If you need to process large amounts of data, you can use Spark: 
```bash
pip install pyspark
```
### 3. Validate Data (Example)
We use the tax_data.csv file in the data folder for demonstration.

```python
cd GMLVverify/project
python example_usage.py
```
- "expectations": [
    {
      "type": "expect_column_values_to_be_between",
      "kwargs": {
        "column": "passenger_count",
        "min_value": 1.0,
        "max_value": 1000.0
      },
      "meta": {},
      "id": "d813f833-f2fc-456d-aa86-2f588b7b6873"
    }
  ]  //Expectation Information: Values in the "passenger_count" column must be between 1.0 and 1000.0

- Validation Result:
"result": {
"element_count": 10000, // Number of validated elements
"unexpected_count": 0, // Number of unexpected values
"unexpected_percent": 0.0, // Percentage of unexpected values
"partial_unexpected_list": [],
"missing_count": 0,
"missing_percent": 0.0,
"unexpected_percent_total": 0.0, // Percentage of unexpected values (including missing values)
"unexpected_percent_nonmissing": 0.0,
"partial_unexpected_counts": [],
"partial_unexpected_index_list": [],
"unexpected_list": [],
"unexpected_index_list": [],
"unexpected_index_query": "df.filter(items=[], axis=0)"
}

## Core Components
- DataFrameValidator: Used to validate Pandas and Spark DataFrames.
- FileValidator: Used to validate file data (e.g., CSV, Parquet).
- DatabaseValidator: Used to validate table data in databases (e.g., PostgreSQL, MySQL).
## Custom Expectations
Expectation configuration is the core of this system and the only part that requires manual configuration by users. You can use great_expectations.expectations (usually aliased as gxe) to create rich validation rules. Here are examples of some common expectations:

Value range validation:
```python
expect_column_values_to_be_between(
    column="passenger_count",
    min_value=1.0,
    max_value=1000.0
)
```
缺失值验证：
```python
expect_column_values_to_not_be_null(
    column="passenger_count"
)
```
长度范围验证：
```python
expect_column_values_to_be_between(
    column="name",
    min_value=1,
    max_value=20
)
```
空值检查：
```python
expect_column_values_to_not_be_null(
    column="name"
)
```
## Typical Application Scenarios

- Data cleaning and preprocessing: Before importing data into an analysis platform, filter outliers and missing values through validation rules to ensure the quality of raw data.
- ETL process monitoring: Perform real-time validation during data extraction, transformation, and loading to promptly capture data errors in the pipeline, preventing downstream tasks from being affected by contaminated data.
- Data analysis and modeling: Validate DataFrames output by feature engineering to ensure model input data meets expectations, thereby enhancing modeling reliability.
- Data reporting and auditing: Automatically generate validation documents to provide technical support for data compliance audits and business report verification.
