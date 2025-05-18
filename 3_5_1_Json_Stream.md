---

# 🛠️ Data Engineering Tutorial: Streaming JSON Ingestion & Processing with PySpark

---

## 🎯 Objective

Build a **robust streaming data pipeline** in PySpark that:

* Reads evolving **nested JSON files** from a raw source directory (Bronze)
* Parses and flattens the data into structured tables (Silver)
* Performs basic aggregations (Gold)
* Uses **checkpointing** and **schema management** to ensure reliability and recovery

---

## 📁 Folder Structure

```bash
project/
│
├── stream_data/         # Incoming raw JSON files (Bronze)
├── checkpoints/         # Checkpoint directories
├── output/
│   ├── silver_data/     # Flattened and clean output
│   └── gold_data/       # Aggregated insights
└── stream_pipeline.py   # Main pipeline script
```

---

## 🧾 Sample Evolving JSON (in `stream_data/`)

```json
[
  {
    "id": 1001,
    "name": "Alice",
    "timestamp": "2025-05-17T12:30:00Z",
    "contact": {
      "email": "alice@example.com",
      "phones": [
        {"type": "home", "number": "111-222-3333"}
      ]
    },
    "meta": {
      "source": "api",
      "version": 1
    }
  }
]
```

Later files may contain:

```json
{
  "id": 1002,
  "name": "Bob",
  "timestamp": "2025-05-17T12:45:00Z",
  "age": 34,
  "is_active": true,
  "contact": {
    "email": "bob@example.com"
  },
  "meta": {
    "source": "web",
    "version": 2
  }
}
```

---

## ⚙️ Step 1: Define the Schema

```python
from pyspark.sql.types import *

json_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("contact", StructType([
        StructField("email", StringType(), True),
        StructField("phones", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("number", StringType(), True)
        ])), True)
    ]), True),
    StructField("meta", StructType([
        StructField("source", StringType(), True),
        StructField("version", IntegerType(), True)
    ]), True)
])
```

---

## 🚀 Step 2: Ingest Raw JSON (Bronze)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StreamingRawJsonIngestion") \
    .getOrCreate()

df_bronze = spark.readStream \
    .format("json") \
    .schema(json_schema) \
    .option("multiline", True) \
    .load("stream_data/")
```

---

## 🔄 Step 3: Transform to Flattened Silver Table

```python
from pyspark.sql.functions import col, explode, to_timestamp

df_silver = df_bronze \
    .withColumn("event_time", to_timestamp("timestamp")) \
    .withColumn("phone", explode("contact.phones")) \
    .select(
        "id",
        "name",
        "age",
        "is_active",
        "event_time",
        col("contact.email").alias("email"),
        col("phone.type").alias("phone_type"),
        col("phone.number").alias("phone_number"),
        col("meta.source").alias("source"),
        col("meta.version").alias("version")
    )
```

---

## 💽 Step 4: Write Silver Table to Disk

```python
silver_query = df_silver.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/silver") \
    .start("output/silver_data")
```

---

## 🪙 Step 5: Gold Aggregation (e.g. Active Users by Source)

```python
from pyspark.sql.functions import window, count

df_gold = df_silver \
    .filter(col("is_active") == True) \
    .groupBy(
        window("event_time", "10 minutes"),
        "source"
    ).agg(
        count("id").alias("active_user_count")
    )

gold_query = df_gold.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "checkpoints/gold") \
    .start("output/gold_data")
```

---

## 🧼 Step 6: Best Practices

| Best Practice                       | Why                                        |
| ----------------------------------- | ------------------------------------------ |
| **Use schema inference off**        | Prevents latency and inconsistency         |
| **Checkpoint every write**          | Enables recovery on restart                |
| **Flatten nested structures early** | Makes data easier to use downstream        |
| **Write to Delta Lake**             | Supports ACID, merge, and schema evolution |
| **Use `multiline=true`**            | Required for JSON arrays in a single file  |

---

## ✅ Run the Pipeline

Save this code in `stream_pipeline.py` and run:

```bash
spark-submit stream_pipeline.py
```

---

## 📊 Example Output: Silver Table

| id   | name  | age | is\_active | event\_time      | email                                         | phone\_type | phone\_number | source | version |
| ---- | ----- | --- | ---------- | ---------------- | --------------------------------------------- | ----------- | ------------- | ------ | ------- |
| 1001 | Alice |     |            | 2025-05-17 12:30 | [alice@example.com](mailto:alice@example.com) | home        | 111-222-3333  | api    | 1       |

---

## 🔄 Optional: Autoloader Alternative for Scalability

Use **Auto Loader** if reading from cloud storage like S3/ADLS:

```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .schema(json_schema) \
    .load("s3://bucket/raw/")
```

---
