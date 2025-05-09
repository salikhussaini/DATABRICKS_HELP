### **Tutorial: Getting Started with PySpark in Databricks for Delta Live Tables (DLT)**

---

### **Module 2: Getting Started with PySpark in Databricks**

In this module, you will learn how to get started with **PySpark** in **Databricks**, which is essential for data engineering tasks, including working with Delta Live Tables (DLT). We will cover key concepts like **DataFrames**, **RDDs**, and **Datasets**, followed by essential operations and transformations.

---

### **2.1 Introduction to PySpark**

#### **PySpark Overview and Setup**

**PySpark** is the Python API for **Apache Spark**, a powerful open-source distributed computing system used for big data processing and analytics. It provides easy-to-use APIs for manipulating large datasets in a distributed environment. Databricks integrates **PySpark** for seamless big data operations with scalable cloud infrastructure.

**Setting Up PySpark in Databricks**:

* Databricks comes pre-configured with **Apache Spark** and **PySpark**, so you don't need to install anything.
* You can simply use **Databricks Notebooks** to run PySpark code.

To use PySpark in Databricks:

1. Open your Databricks workspace.
2. Create a **New Notebook**.
3. Select **Python** as the language for the notebook.
4. Start writing your PySpark code in the notebook cells.

Here’s a simple example of reading data with PySpark in Databricks:

```python
# Importing PySpark
from pyspark.sql import SparkSession

# Creating a Spark session
spark = SparkSession.builder.appName("DLT PySpark Example").getOrCreate()

# Reading data into a DataFrame
df = spark.read.csv("/mnt/data/sample_data.csv", header=True, inferSchema=True)

# Showing the first few rows of the DataFrame
df.show(5)
```

#### **Key PySpark Components**

1. **RDDs (Resilient Distributed Datasets)**:

   * RDDs are the lowest-level abstraction in Spark, representing an immutable distributed collection of objects.
   * While RDDs provide more control over data operations, they are less optimized than DataFrames.

2. **DataFrames**:

   * **DataFrames** are distributed collections of data organized into named columns, making them easier to work with than RDDs.
   * They offer high-level operations (e.g., `select()`, `filter()`, `groupBy()`) and optimize execution with Catalyst (Spark’s query optimizer).

   Example of creating a DataFrame from a CSV file:

   ```python
   df = spark.read.csv("/mnt/data/sample_data.csv", header=True, inferSchema=True)
   df.show(5)
   ```

3. **Datasets**:

   * Datasets are a type-safe version of DataFrames, typically used in **Scala**. In PySpark, DataFrames are more commonly used.

---

### **2.2 PySpark DataFrame Operations**

#### **Basic DataFrame Creation and Operations**

Creating and working with **DataFrames** in PySpark involves various operations like `select()`, `filter()`, `groupBy()`, and `join()`. Here are some essential operations:

1. **Select Columns**:

   * `select()` allows you to choose specific columns from a DataFrame.

   ```python
   df.select("column_name").show()
   ```

2. **Filter Rows**:

   * `filter()` or `where()` allows you to filter rows based on a condition.

   ```python
   df.filter(df["age"] > 30).show()
   ```

3. **Group By**:

   * `groupBy()` is used for aggregating data based on certain columns.

   ```python
   df.groupBy("age").count().show()
   ```

4. **Join**:

   * `join()` is used to combine two DataFrames on a common column.

   ```python
   df1 = spark.read.csv("/mnt/data/df1.csv", header=True, inferSchema=True)
   df2 = spark.read.csv("/mnt/data/df2.csv", header=True, inferSchema=True)

   joined_df = df1.join(df2, df1["id"] == df2["id"]).select("df1.name", "df2.age")
   joined_df.show()
   ```

#### **DataFrame Transformations and Actions**

* **Transformations** are operations that return a new DataFrame (e.g., `select()`, `filter()`, `groupBy()`).
* **Actions** trigger computation and return results (e.g., `show()`, `collect()`).

Examples:

1. **Transformation**:

   * `withColumn()` adds a new column or transforms an existing one.

   ```python
   df.withColumn("new_column", df["age"] + 10).show()
   ```

2. **Action**:

   * `show()` displays the first few rows.

   ```python
   df.show(10)
   ```

#### **Handling Missing Data**

You often need to handle missing data when working with large datasets. PySpark provides several functions to deal with **null** values:

1. **Fill Missing Data** (`fillna()`):

   * `fillna()` fills **null** values with a specified value.

   ```python
   df.fillna({"age": 0, "name": "Unknown"}).show()
   ```

2. **Drop Missing Data** (`dropna()`):

   * `dropna()` removes rows that contain **null** values.

   ```python
   df.dropna().show()
   ```

3. **Check for Nulls** (`isNull()`):

   * `isNull()` checks if a column has null values.

   ```python
   df.filter(df["age"].isNull()).show()
   ```

#### **Understanding Lazy Evaluation in Spark**

Spark operates under **lazy evaluation**, meaning that transformations are not executed immediately. Instead, they are **recorded** and only executed when an **action** is called. This is done for optimization, allowing Spark to apply query optimizations (e.g., pipelining transformations).

For example, in the code below, the transformations (`select`, `filter`) won't be executed until `show()` (an action) is called:

```python
df = df.select("name", "age").filter(df["age"] > 30)
df.show()  # This triggers the actual computation
```

---

### **2.3 PySpark SQL**

#### **Running SQL Queries with PySpark**

PySpark allows you to run SQL queries directly on your DataFrames, leveraging SparkSQL. To do so, you must first register your DataFrame as a temporary view using `createOrReplaceTempView()`.

Example:

```python
# Register the DataFrame as a temporary view
df.createOrReplaceTempView("customer_data")

# Run SQL queries on the DataFrame
spark.sql("SELECT name, age FROM customer_data WHERE age > 30").show()
```

#### **Registering DataFrames as Temporary Views for SQL Querying**

A **temporary view** allows you to run SQL queries on a DataFrame. The view will exist only for the duration of the Spark session.

Example:

```python
df.createOrReplaceTempView("temp_view")

# SQL query on the DataFrame
result = spark.sql("SELECT * FROM temp_view WHERE age > 25")
result.show()
```

#### **Using SparkSQL with DataFrames**

You can use **SparkSQL** in PySpark just as you would in a standard SQL environment. After registering your DataFrame as a temporary view, you can perform operations like `SELECT`, `JOIN`, `GROUP BY`, etc.

Example of joining two DataFrames using SparkSQL:

```python
df1.createOrReplaceTempView("df1_view")
df2.createOrReplaceTempView("df2_view")

query = """
    SELECT df1.name, df2.age
    FROM df1_view df1
    JOIN df2_view df2 ON df1.id = df2.id
"""
result = spark.sql(query)
result.show()
```

---

### **Conclusion**

In this tutorial, you have learned:

* The fundamentals of **PySpark** and how to set up PySpark in **Databricks**.
* Key PySpark concepts such as **RDDs**, **DataFrames**, and **Datasets**.
* How to perform basic DataFrame operations such as `select()`, `filter()`, `groupBy()`, and `join()`.
* Techniques for handling missing data, including `fillna()`, `dropna()`, and `isNull()`.
* The importance of **lazy evaluation** in Spark and how it optimizes transformations.
* How to run SQL queries using **SparkSQL** with PySpark, including how to register DataFrames as temporary views.
