# =============================================================
# PYSPARK CHEAT SHEET - Keep this file open as a reference!
# =============================================================
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession, Window

# ---- READ DATA ----
df = spark.table("glue_catalog.dvdrental_mart.dim_film")     # From Iceberg/Glue
df = spark.read.csv("path.csv", header=True, inferSchema=True)  # From CSV
df = spark.read.parquet("path.parquet")                         # From Parquet

# ---- EXPLORE ----
df.printSchema()            # Show column names and types
df.show(10)                 # Show first 10 rows
df.show(10, truncate=False) # Show full content (no truncation)
df.count()                  # Total row count
df.columns                  # List of column names
df.describe("amount").show() # Statistics (mean, stddev, min, max)
df.dtypes                   # List of (column_name, type) tuples

# ---- SELECT ----
df.select("col1", "col2")
df.select(F.col("col1"), F.col("col2").alias("renamed"))
df.select("*")              # All columns
df.drop("unwanted_column")  # All columns EXCEPT this one

# ---- FILTER ----
df.filter(F.col("amount") > 5)
df.filter((F.col("amount") > 5) & (F.col("store_id") == 1))  # AND
df.filter((F.col("rating") == "R") | (F.col("rating") == "PG"))  # OR
df.filter(F.col("title").contains("LOVE"))
df.filter(F.col("title").like("%LOVE%"))
df.filter(F.col("return_date").isNull())
df.filter(F.col("return_date").isNotNull())
df.filter(F.col("rating").isin(["R", "PG", "PG-13"]))

# ---- ADD/MODIFY COLUMNS ----
df.withColumn("new_col", F.lit("constant_value"))
df.withColumn("amount_x2", F.col("amount") * 2)
df.withColumn("name_upper", F.upper(F.col("first_name")))
df.withColumn("tier", 
    F.when(F.col("amount") >= 10, "VIP")
     .when(F.col("amount") >= 5, "Gold")
     .otherwise("Normal")
)
df.withColumnRenamed("old_name", "new_name")

# ---- CAST (Change Data Type) ----
df.withColumn("amount_int", F.col("amount").cast(T.IntegerType()))
df.withColumn("date_col", F.col("date_string").cast("date"))

# ---- AGGREGATION ----
df.groupBy("store_id").agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("customer_id").alias("unique_customers"),
    F.sum("amount").alias("total_revenue"),
    F.avg("amount").alias("avg_amount"),
    F.min("amount").alias("min_amount"),
    F.max("amount").alias("max_amount"),
    F.round(F.avg("amount"), 2).alias("avg_rounded")
)

# ---- JOIN ----
# Simple join (same column name on both sides → use list to avoid duplicates!)
df1.join(df2, on=["customer_id"], how="left")

# Join on different column names
df1.join(df2, on=df1["cust_id"] == df2["customer_id"], how="inner")

# Multi-column join
df1.join(df2, on=["store_id", "payment_date_day"], how="left")

# Broadcast join (for tiny tables → avoids shuffle!)
df1.join(F.broadcast(tiny_df), on=["store_id"], how="left")

# Cross join
df1.crossJoin(df2)

# Join types: "inner", "left", "right", "outer", "cross", "semi", "anti"

# ---- SORT ----
df.orderBy("amount")                         # ASC
df.orderBy(F.col("amount").desc())           # DESC
df.orderBy(F.col("store_id").asc(), F.col("amount").desc())  # Multi

# ---- WINDOW FUNCTIONS ----
window_spec = Window.partitionBy("store_id").orderBy(F.col("amount").desc())

df.withColumn("rank", F.rank().over(window_spec))
df.withColumn("row_num", F.row_number().over(window_spec))
df.withColumn("dense_rank", F.dense_rank().over(window_spec))
df.withColumn("prev_amount", F.lag("amount", 1).over(window_spec))
df.withColumn("next_amount", F.lead("amount", 1).over(window_spec))

# Running total
window_running = Window.partitionBy("store_id").orderBy("payment_date_day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", F.sum("amount").over(window_running))

# Moving average (7-day)
window_moving = Window.partitionBy("store_id").orderBy("payment_date_day").rowsBetween(-6, Window.currentRow)
df.withColumn("moving_avg_7d", F.avg("amount").over(window_moving))

# ---- DATE FUNCTIONS ----
df.withColumn("year", F.year("rental_date"))
df.withColumn("month", F.month("rental_date"))
df.withColumn("day", F.dayofmonth("rental_date"))
df.withColumn("day_of_week", F.dayofweek("rental_date"))
df.withColumn("date_formatted", F.date_format("rental_date", "yyyyMMdd"))
df.withColumn("days_diff", F.datediff(F.col("return_date"), F.col("rental_date")))
df.withColumn("today", F.current_date())

# ---- STRING FUNCTIONS ----
df.withColumn("upper", F.upper("name"))
df.withColumn("lower", F.lower("name"))
df.withColumn("trimmed", F.trim("name"))
df.withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
df.withColumn("first_3", F.substring("name", 1, 3))
df.withColumn("name_length", F.length("name"))

# ---- NULL HANDLING ----
df.withColumn("filled", F.coalesce(F.col("amount"), F.lit(0)))
df.na.fill(0)                    # Fill all nulls with 0
df.na.fill({"amount": 0, "name": "Unknown"})  # Fill per column
df.na.drop()                     # Drop rows with ANY null
df.na.drop(subset=["amount"])    # Drop rows where amount is null

# ---- DISTINCT & DEDUP ----
df.distinct()                    # Remove exact duplicate rows
df.dropDuplicates(["customer_id"])  # Keep first row per customer_id

# ---- UNION ----
df1.union(df2)                   # Stack rows (must have same columns)
df1.unionByName(df2)             # Stack by column name (safer!)

# ---- PIVOT ----
df.groupBy("customer_id").pivot("month").agg(F.sum("amount"))

# ---- WRITE ----
df.write.parquet("output/path")
df.write.mode("overwrite").parquet("output/path")
df.write.format("iceberg").mode("overwrite").saveAsTable("glue_catalog.db.table")
df.write.partitionBy("date_col").parquet("output/path")

# ---- COLLECT (Use with caution!) ----
rows = df.collect()              # Returns List[Row] → to Driver RAM
row = df.first()                 # Returns first Row
value = df.collect()[0]["col"]   # Extract a single value

# ---- EXPLAIN (Debug performance) ----
df.explain()                     # Show physical plan
df.explain(True)                 # Show ALL plans (parsed → optimized → physical)

# ---- CACHE ----
df.cache()                       # Cache in RAM for reuse
df.unpersist()                   # Remove from cache
