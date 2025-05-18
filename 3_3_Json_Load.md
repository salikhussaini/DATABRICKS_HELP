Excellent idea! Let's expand the **PySpark Nested JSON Tutorial** to address **schema evolution** — i.e., when different JSON files have variations in structure. This is common in data lakes where APIs evolve or sources change formats.

---

# 🧰 PySpark Tutorial: Handling Evolving Schemas in Nested JSON

---

## 💡 Problem Scenario

You receive multiple JSON files with similar, but not identical, schemas:

### `people_v1.json`

```json
[
  {
    "id": 1,
    "name": "Alice",
    "contact": {
      "email": "alice@example.com",
      "phones": [{"type": "home", "number": "111-222-3333"}]
    },
    "address": {
      "city": "New York",
      "state": "NY"
    }
  }
]
```

### `people_v2.json` (Schema Change: `address.geo` and new `birth_date`)

```json
[
  {
    "id": 2,
    "name": "Bob",
    "birth_date": "1990-05-01",
    "contact": {
      "email": "bob@example.com",
      "phones": [{"type": "mobile", "number": "999-888-7777"}]
    },
    "address": {
      "city": "San Francisco",
      "state": "CA",
      "geo": {"lat": 37.7749, "lon": -122.4194}
    }
  }
]
```

---

## ⚙️ Step 1: Load Multiple JSONs with Evolving Schema

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SchemaEvolution").getOrCreate()

# Load JSON files (they have different schemas)
df = spark.read.option("mergeSchema", "true").json("data/people_v*.json")
df.printSchema()
df.show(truncate=False)
```

### 🔍 Schema Merging

Setting `option("mergeSchema", "true")` tells Spark to reconcile all fields across files. Spark promotes nulls for missing fields.

---

## 🧪 Step 2: Safely Access Optional Fields

Use `.getField()` and `col("...").isNotNull()` defensively when fields may or may not exist:

```python
from pyspark.sql.functions import col, when, lit

df_flat = df.select(
    "id",
    "name",
    col("birth_date").alias("birth_date"),
    col("contact.email").alias("email"),
    when(col("address.city").isNotNull(), col("address.city")).otherwise(lit("Unknown")).alias("city"),
    col("address.state").alias("state"),
    col("address.geo.lat").alias("latitude"),
    col("address.geo.lon").alias("longitude")
)
df_flat.show(truncate=False)
```

---

## 💥 Step 3: Handle Missing or Null Arrays When Exploding

Before `explode`, guard against nulls:

```python
from pyspark.sql.functions import explode_outer

df_phones = df.withColumn("phone", explode_outer("contact.phones")) \
    .select(
        "id", "name",
        col("contact.email").alias("email"),
        col("phone.type").alias("phone_type"),
        col("phone.number").alias("phone_number")
    )

df_phones.show(truncate=False)
```

---

## 🧹 Step 4: Clean & Standardize Across Versions

You may normalize fields across all versions:

```python
from pyspark.sql.functions import to_date

df_standardized = df_phones.withColumn("birth_date", to_date("birth_date", "yyyy-MM-dd")) \
    .fillna({"phone_type": "unknown", "phone_number": "n/a", "email": "n/a"})

df_standardized.show(truncate=False)
```

---

## 🧩 Step 5: Optional – Unify Fields into a Nested Output

You can reassemble standardized fields into unified structs, even when input JSONs differ:

```python
from pyspark.sql.functions import struct

df_final = df_standardized.withColumn(
    "contact",
    struct(col("email"), col("phone_type"), col("phone_number"))
).drop("email", "phone_type", "phone_number")

df_final.show(truncate=False)
```

---

## 💾 Step 6: Save as Delta for Schema Evolution Support

Delta Lake supports automatic schema evolution:

```python
df_final.write.format("delta").mode("overwrite").save("output/people_delta")
```

And when appending more evolved schemas:

```python
df_new = spark.read.json("data/people_v3.json")

df_new.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("output/people_delta")
```

---

## ✅ Summary of Key Concepts

| Topic                | Description                                                            |
| -------------------- | ---------------------------------------------------------------------- |
| `mergeSchema = true` | Enables Spark to combine fields across schema versions.                |
| `explode_outer()`    | Safely explodes arrays, even if null.                                  |
| `when().otherwise()` | Protects against missing fields.                                       |
| `fillna()`           | Fills missing values for cleaner output.                               |
| Delta Lake           | Provides scalable support for evolving schemas and append-safe writes. |

---

## 🛠 Bonus: Automatically Infer Union Schema with File Discovery

```python
df = spark.read.option("mergeSchema", "true").json("data/people_*.json")
```

This avoids hardcoding fields or worrying about which version you're loading.

---

Would you like me to generate a fully executable **Jupyter notebook** with this walkthrough and mock data generation code?
