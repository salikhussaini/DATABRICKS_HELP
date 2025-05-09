### Module 3: Building Your First Delta Live Table Pipeline

#### 3.1 **Setting Up Your First DLT Pipeline**

##### **Understanding the DLT Pipeline Structure**

A typical DLT pipeline structure consists of three stages:

1. **Bronze** – Raw, unrefined data (often ingested from external sources).
2. **Silver** – Cleaned and transformed data (removes errors, duplicates, etc.).
3. **Gold** – Business-level aggregated data (prepared for reporting or downstream analysis).

**Example**:

```python
import dlt

@dlt.table
def bronze_table():
    return spark.read.parquet("dbfs:/path/to/raw_data/*.parquet")

@dlt.table
def silver_table():
    return (
        dlt.read("bronze_table")
        .filter("column_name IS NOT NULL")
        .dropDuplicates()
    )

@dlt.table
def gold_table():
    return (
        dlt.read("silver_table")
        .groupBy("business_key")
        .agg({"amount": "sum"})
        .withColumnRenamed("sum(amount)", "total_amount")
    )
```

* **Bronze** table ingests raw data.
* **Silver** table cleans the data by removing null values and duplicates.
* **Gold** table aggregates the data by `business_key`.

##### **Creating a Bronze Table for Raw Data Ingestion**

A Bronze table is typically used for raw, unprocessed data ingestion. The table can ingest data from a variety of sources, such as files, databases, or streaming sources.

**Example**:

```python
@dlt.table
def bronze_data():
    # Ingest data from CSV files
    return (
        spark.read.option("header", "true")
        .csv("dbfs:/path/to/raw_data/*.csv")
    )
```

* This example reads raw CSV data into a DLT pipeline.

##### **Defining Silver Tables for Data Cleansing and Transformation**

Silver tables are used to perform data transformations like filtering, deduplication, and basic cleaning.

**Example**:

```python
@dlt.table
def silver_data():
    # Perform basic cleaning on the Bronze data
    return (
        dlt.read("bronze_data")
        .filter("age > 18")
        .dropDuplicates(["user_id"])
        .withColumn("processed_date", current_date())
    )
```

* Cleans the data by filtering out records where age is less than 18.
* Drops duplicate records based on `user_id`.
* Adds a new column, `processed_date`, with the current date.

##### **Creating Gold Tables for Business-Level Aggregation and Reporting**

Gold tables contain aggregated, business-level data that can be used for reporting, dashboards, or analytics.

**Example**:

```python
@dlt.table
def gold_data():
    return (
        dlt.read("silver_data")
        .groupBy("country")
        .agg({"total_amount": "sum"})
        .withColumnRenamed("sum(total_amount)", "total_sales")
    )
```

* Aggregates the sales data from the Silver table by `country` and computes the sum of the `total_amount`.

#### 3.2 **Defining DLT Tables**

##### **Using @dlt.table Decorator to Define Tables**

In DLT, the `@dlt.table` decorator is used to define tables within the pipeline. The decorator provides a clean and concise way to declare tables.

**Example**:

```python
import dlt

@dlt.table
def my_table():
    return (
        spark.read.parquet("dbfs:/path/to/data")
        .filter("column1 IS NOT NULL")
    )
```

* The `@dlt.table` decorator defines a new Delta Live Table (`my_table`).
* It reads data from a Parquet file, filters rows where `column1` is `NULL`, and prepares it for downstream processing.

##### **Loading Data with dlt.read()**

Use `dlt.read()` to read data from previously defined tables or external sources within the DLT pipeline.

**Example**:

```python
@dlt.table
def processed_data():
    return (
        dlt.read("bronze_data")
        .filter("column_name > 100")
        .withColumn("new_column", col("existing_column") * 2)
    )
```

* Reads from the `bronze_data` table and applies a filter and transformation.

##### **Writing Transformations in Delta Live Tables**

You can apply various transformations to the data within a DLT table definition, such as filtering, aggregation, and joining.

**Example**:

```python
@dlt.table
def transformed_data():
    return (
        dlt.read("bronze_data")
        .filter("column_value > 100")
        .groupBy("category")
        .agg({"sales": "sum"})
        .withColumnRenamed("sum(sales)", "total_sales")
    )
```

* This table performs an aggregation on the `bronze_data` table by summing the `sales` by `category`.

#### 3.3 **Scheduling and Automating Pipelines**

##### **Scheduling Your DLT Pipeline for Regular Execution**

Databricks allows you to schedule pipelines to run automatically at specified intervals.

**Example**:

1. Go to the **Pipelines** tab in Databricks.
2. Create a new pipeline and select the **schedule** option.
3. Configure the schedule (e.g., run every 15 minutes, hourly, daily, etc.).

##### **Setting Up Automated and Incremental Data Pipelines**

DLT supports **incremental data pipelines**, where only new or updated data is processed. You can specify incremental processing with `@dlt.expect` or other configurations.

**Example**:

```python
@dlt.table
@dlt.expect("new_data_check", "last_updated > '2021-01-01'")
def incremental_data():
    return (
        spark.read.parquet("dbfs:/path/to/new_data/")
        .filter("last_updated > '2021-01-01'")
    )
```

* This table will only process new or updated data that has a `last_updated` value greater than `'2021-01-01'`.

##### **Automating Pipelines Through Databricks Jobs**

You can automate the execution of DLT pipelines through **Databricks Jobs**.

1. Create a new **Job** in the **Jobs** section of Databricks.
2. Select the **Notebook** containing your DLT pipeline.
3. Set the desired schedule for the job to run.

**Example**:

* Once your DLT pipeline is defined, you can create a job that will trigger the pipeline execution on a set schedule.

---

### Additional Concepts to Add to Module 3:

* **Managing Dependencies Between Tables**: Ensure that Silver tables wait for data from Bronze tables before processing, and Gold tables wait for Silver tables.

  **Example**:

  ```python
  @dlt.table
  def gold_data():
      return (
          dlt.read("silver_data")
          .groupBy("country")
          .agg({"sales": "sum"})
          .withColumnRenamed("sum(sales)", "total_sales")
      )
  ```

* **Data Validation with Expectations**: Use DLT’s expectations feature to ensure data quality during pipeline execution.

  **Example**:

  ```python
  @dlt.table
  @dlt.expect("no_null_values", "column_name IS NOT NULL")
  def clean_data():
      return (
          dlt.read("bronze_data")
          .filter("column_name > 100")
      )
  ```

---
