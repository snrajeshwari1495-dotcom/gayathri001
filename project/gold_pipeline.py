from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

# -------------------------------
# PATH CONFIG
# -------------------------------
SILVER_EV = "data_lake/silver/ev_population/"
GOLD_PATH = "data_lake/gold/"

# -------------------------------
# SPARK SESSION
# -------------------------------
spark = SparkSession.builder \
    .appName("Gold_EV_Analytics") \
    .getOrCreate()

print("🚀 Gold layer started...")

# ==========================================================
# READ SILVER DATA
# ==========================================================
try:
    df = spark.read.parquet(SILVER_EV)
    print("✅ Silver data loaded")
except Exception as e:
    print("❌ Error reading Silver:", e)
    exit()

# ==========================================================
# 1️⃣ EV ADOPTION BY REGION
# ==========================================================
print("📊 Processing: EV Adoption by Region")

df_region = df.groupBy("state", "city") \
    .agg(
        count("*").alias("vehicle_count"),
        avg("electric_range").alias("avg_electric_range")
    )

df_region.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH + "ev_adoption_by_region")

print("✅ ev_adoption_by_region done")

# ==========================================================
# 2️⃣ EV MODEL POPULARITY
# ==========================================================
print("🚗 Processing: EV Model Popularity")

df_model = df.groupBy("make", "model") \
    .agg(
        count("*").alias("vehicle_count")
    ) \
    .orderBy(col("vehicle_count").desc())

df_model.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH + "ev_model_popularity")

print("✅ ev_model_popularity done")

# ==========================================================
# 3️⃣ EV ADOPTION TREND (TIME SERIES)
# ==========================================================
print("📈 Processing: EV Adoption Trend")

df_trend = df.groupBy("year", "month") \
    .agg(
        count("*").alias("vehicle_count")
    ) \
    .orderBy("year", "month")

df_trend.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH + "ev_adoption_trend")

print("✅ ev_adoption_trend done")

# ==========================================================
# END
# ==========================================================
print("🎉 Gold layer completed successfully")

spark.stop()