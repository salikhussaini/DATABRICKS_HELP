Here’s a full tutorial on **creating a schema before loading JSON data in PySpark**, and techniques for **continuously updating (evolving) the schema** as new fields appear.

---

# 📘 PySpark Tutorial: Defining and Evolving Schema for JSON Files

---

## ✅ Why Predefine a Schema?

Predefining the schema:

* Prevents schema inference overhead
* Enforces data types and structure
* Helps catch malformed records early
* Allows explicit control over evolving schemas

---

## 🧱 Step 1: Define an Initial Schema Using `StructType`

Let’s say you expect your JSON to look like this:

```json
{
  "id": 1,
  "name": "Alice",
  "contact": {
    "email": "alice@example.com",
    "phones": [
      { "type": "home", "number": "111-222-3333" }
    ]
  }
}
```

### 🔧 Define Schema in PySpark

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

schema_v1 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("contact", StructType([
        StructField("email", StringType(), True),
        StructField("phones", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("number", StringType(), True)
        ])), True)
    ]), True)
])
```

---

## 📥 Step 2: Load JSON with the Defined Schema

```python
df = spark.read.schema(schema_v1).json("data/people_v1.json")
df.printSchema()
df.show(truncate=False)
```

This will **not infer** the schema — it uses your definition exactly.

---

## 🔄 Step 3: Evolve the Schema Manually (Version 2)

Say `people_v2.json` adds two new fields:

* `birth_date` (string)
* `contact.linkedin` (string)

You’ll extend the schema like this:

```python
schema_v2 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("birth_date", StringType(), True),  # New field
    StructField("contact", StructType([
        StructField("email", StringType(), True),
        StructField("linkedin", StringType(), True),  # New field
        StructField("phones", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("number", StringType(), True)
        ])), True)
    ]), True)
])
```

Then load the new file:

```python
df_v2 = spark.read.schema(schema_v2).json("data/people_v2.json")
df_v2.printSchema()
```

---

## 🧠 Step 4: Automatically Evolve Schema at Runtime (Advanced)

### Option 1: Infer Schema Dynamically

```python
df_inferred = spark.read.option("inferSchema", "true").json("data/people_v2.json")
df_inferred.printSchema()
```

> ❗ Not ideal for production. Expensive and inconsistent when fields change across files.

---

### Option 2: Combine Existing and New Schema Programmatically

```python
from pyspark.sql.functions import schema_of_json, lit
from pyspark.sql.types import StructType

sample_json = spark.read.text("data/people_v2.json").limit(1).collect()[0][0]

# Infer schema from new data
inferred_schema_str = spark.range(1).select(schema_of_json(lit(sample_json))).collect()[0][0]
inferred_schema = StructType.fromJson(eval(inferred_schema_str))

# Compare and merge with previous schema
# (You can implement a recursive schema merge function or use Delta Lake for easier handling)
```

---

## 💾 Step 5: Use Delta Lake for Auto Schema Evolution (Recommended for Production)

```python
# First write with initial schema
df.write.format("delta").mode("overwrite").save("output/people_delta")

# Now write a new dataframe with new schema
df_v2.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("output/people_delta")
```

Delta will automatically merge schemas.

---

## 🛡 Step 6: Validate Schema Consistency (Optional Utility)

```python
def validate_schema(expected: StructType, actual: StructType) -> bool:
    return expected.json() == actual.json()

# Example usage
print(validate_schema(schema_v1, df.schema))
```

---

## 🧩 Bonus: Store Schema Separately for Auditing

You can serialize and save your schema for versioning:

```python
with open("schema_v1.json", "w") as f:
    f.write(schema_v1.json())
```

And later load it:

```python
import json
from pyspark.sql.types import StructType

with open("schema_v1.json", "r") as f:
    schema_loaded = StructType.fromJson(json.load(f))
```

---

## ✅ Summary

| Task                     | Technique                                        |
| ------------------------ | ------------------------------------------------ |
| Define schema            | `StructType` manually                            |
| Load JSON with schema    | `.read.schema(...).json(...)`                    |
| Evolve schema            | Extend `StructType` or use Delta Lake            |
| Merge schema             | `mergeSchema = true`                             |
| Auto infer schema        | `option("inferSchema", "true")` (non-production) |
| Version schema           | Save schema as JSON                              |
| Validate schema          | Compare with `.json()` method                    |
| Persist schema evolution | Use Delta Lake for versioned writes              |

---

Would you like this tutorial exported to a notebook or Python script with mock data generation?
