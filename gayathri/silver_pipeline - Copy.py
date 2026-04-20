import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_timestamp

# -------------------------------
# PATH CONFIG
# -------------------------------
BRONZE_EV = "data_lake/bronze/ev_population/"
SILVER_EV = "data_lake/silver/ev_population/"

# -------------------------------
# SPARK SESSION
# -------------------------------
spark = SparkSession.builder \
    .appName("Silver_EV_Processing") \
    .getOrCreate()

print("🚀 Silver layer started...")

# ==========================================================
# READ BRONZE DATA
# ==========================================================
try:
    df = spark.read.json(BRONZE_EV)
    print("✅ Bronze data loaded")
except Exception as e:
    print("❌ Error reading Bronze:", e)
    exit()

# ==========================================================
# DATA CLEANING
# ==========================================================

# Drop completely null rows
df = df.dropna(how="all")

# Drop duplicates
df = df.dropDuplicates()

# ==========================================================
# SELECT REQUIRED COLUMNS (safe handling)
# ==========================================================
columns_needed = [
    "model_year",
    "make",
    "model",
    "electric_vehicle_type",
    "electric_range",
    "county",
    "city",
    "state",
    "postal_code",
    "ingestion_timestamp"
]

existing_cols = [c for c in columns_needed if c in df.columns]
df = df.select(existing_cols)

# ==========================================================
# DATA TYPE STANDARDIZATION
# ==========================================================

# Convert numeric fields
if "model_year" in df.columns:
    df = df.withColumn("model_year", col("model_year").cast("int"))

if "electric_range" in df.columns:
    df = df.withColumn("electric_range", col("electric_range").cast("int"))

# Convert timestamp
if "ingestion_timestamp" in df.columns:
    df = df.withColumn(
        "ingestion_timestamp",
        to_timestamp("ingestion_timestamp")
    )

# ==========================================================
# ADD PARTITION COLUMNS
# ==========================================================
df = df.withColumn("year", year("ingestion_timestamp"))
df = df.withColumn("month", month("ingestion_timestamp"))

# ==========================================================
# DATA QUALITY CHECKS
# ==========================================================

# Remove invalid values
if "electric_range" in df.columns:
    df = df.filter(col("electric_range") >= 0)

if "model_year" in df.columns:
    df = df.filter(col("model_year") > 2000)

# ==========================================================
# WRITE TO SILVER (PARQUET + PARTITION)
# ==========================================================

try:
    df.write \
        .mode("append") \
        .partitionBy("year", "month", "state") \
        .parquet(SILVER_EV)

    print("✅ Silver layer completed successfully")

except Exception as e:
    print("❌ Error writing Silver:", e)

# ==========================================================
# END
# ==========================================================
spark.stop()