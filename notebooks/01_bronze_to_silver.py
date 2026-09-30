# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime,timezone
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType
)

# COMMAND ----------

BRONZE_PATH = "/Volumes/smart_manufacturing_classic/lakehouse/bronze_volume"
SILVER_PATH = "/Volumes/smart_manufacturing_classic/lakehouse/silver_volume"
GOLD_PATH   = "/Volumes/smart_manufacturing_classic/lakehouse/gold_volume"

# COMMAND ----------

# MAGIC %md
# MAGIC Watermark section

# COMMAND ----------

metadata_schema = StructType([
    StructField("dataset_name", StringType(), False),
    StructField("watermark_column", StringType(), False),
    StructField("last_watermark", TimestampType(), True),
    StructField("last_successful_run", TimestampType(), True),
    StructField("rows_processed", LongType(), True),
    StructField("status", StringType(), True)
])

# COMMAND ----------

def upsert(df, path, key_cols):
    if DeltaTable.isDeltaTable(spark, path):
        keys = " AND ".join([f"t.{k} = s.{k}" for k in key_cols])
        (
            DeltaTable.forPath(spark, path).alias("t")
            .merge(df.alias("s"), keys)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

METADATA_PATH = f"{SILVER_PATH}/pipeline_metadata"
def get_watermark(dataset_name):
    metadata = spark.read.format("delta").load(METADATA_PATH)

    row = (
        metadata
        .filter(F.col("dataset_name") == dataset_name)
        .select("last_watermark")
        .first()
    )

    if row is None:
        return None

    return row["last_watermark"]

# COMMAND ----------

def update_watermark(
    dataset_name,
    watermark_column,
    last_watermark,
    rows_processed,
    status="SUCCESS"
):
    
    current_time = datetime.now(timezone.utc)

    new_data = spark.createDataFrame(
        [(
            dataset_name,
            watermark_column,
            last_watermark,
            current_time,
            rows_processed,
            status
        )],
        metadata_schema
    )

    metadata = DeltaTable.forPath(spark, METADATA_PATH)

    (
        metadata.alias("target")
        .merge(
            new_data.alias("source"),
            "target.dataset_name = source.dataset_name"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

FACTORIES_PATH   = f"{BRONZE_PATH}/factory_data/factories.csv"
MACHINES_PATH    = f"{BRONZE_PATH}/factory_data/machines.csv"
TELEMETRY_PATH   = f"{BRONZE_PATH}/factory_data/telemetry.csv"
PRODUCTION_PATH  = f"{BRONZE_PATH}/factory_data/production_orders.csv"
MAINTENANCE_PATH = f"{BRONZE_PATH}/factory_data/maintenance.csv"
QUALITY_PATH     = f"{BRONZE_PATH}/factory_data/quality_inspections.csv"
WEATHER_PATH	=f"{BRONZE_PATH}/weather_data"

# COMMAND ----------

# MAGIC %md
# MAGIC FACTORIES

# COMMAND ----------

factories = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(FACTORIES_PATH)
)

silver_factories = (
    factories
    .select(
        F.col("factory_id").cast("string"),
        F.col("factory_name").cast("string"),
        F.col("city").cast("string"),
        F.col("country").cast("string"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double")
    )
    .dropDuplicates(["factory_id"])
)

(
    silver_factories.write
    .format("delta")
    .mode("overwrite")
    .save(f"{SILVER_PATH}/factories")
)


# COMMAND ----------

# MAGIC %md
# MAGIC MACHINES

# COMMAND ----------

machines = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(MACHINES_PATH)
)

silver_machines = (
    machines
    .select(
        F.col("machine_id").cast("string"),
        F.col("factory_id").cast("string"),
        F.col("machine_type").cast("string"),
        F.col("installation_date").alias("installation_date"),
        F.col("rated_power_kw").cast("double"),
        F.col("status").cast("string")
    )
    .dropDuplicates(["machine_id"])
)

(
    silver_machines.write
    .format("delta")
    .mode("overwrite")
    .save(f"{SILVER_PATH}/machines")
)

# COMMAND ----------

# MAGIC %md
# MAGIC TELEMETRY - INCREMENTAL LOAD
# MAGIC

# COMMAND ----------

telemetry = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(TELEMETRY_PATH)
)

telemetry = (
    telemetry
    .select(
        F.to_timestamp("timestamp").alias("timestamp"),
        F.col("machine_id").cast("string"),
        F.col("temperature_c").cast("double"),
        F.col("vibration_mm_s").cast("double"),
        F.col("pressure_bar").cast("double"),
        F.col("power_consumption_kw").cast("double"),
        F.col("machine_status").cast("string")
    )
    .filter(F.col("timestamp").isNotNull())
    .filter(F.col("machine_id").isNotNull())
)
last_watermark = get_watermark("telemetry")
print("Previous watermark:", last_watermark)


# COMMAND ----------

if last_watermark is not None:
    telemetry_incremental = telemetry.filter(
        F.col("timestamp") > F.lit(last_watermark)
    )
else:
    telemetry_incremental = telemetry

# COMMAND ----------

silver_telemetry = (
    telemetry_incremental
    .dropDuplicates(["machine_id", "timestamp"])
)

# COMMAND ----------

rows_processed = silver_telemetry.count()
if rows_processed > 0:

    upsert(silver_telemetry, f"{SILVER_PATH}/telemetry", ["machine_id", "timestamp"])

    new_watermark = (
        silver_telemetry
        .agg(F.max("timestamp").alias("max_timestamp"))
        .first()["max_timestamp"]
    )

    update_watermark(
        dataset_name="telemetry",
        watermark_column="timestamp",
        last_watermark=new_watermark,
        rows_processed=rows_processed
    )

else:

    print("No new telemetry data.")

    update_watermark(
        dataset_name="telemetry",
        watermark_column="timestamp",
        last_watermark=last_watermark,
        rows_processed=0
    )

# COMMAND ----------

# MAGIC %md
# MAGIC PRODUCTION

# COMMAND ----------

production = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(PRODUCTION_PATH)
)
last_watermark = get_watermark("production")
print(last_watermark)

# COMMAND ----------

if last_watermark is not None:
    production = production.filter(
        F.col("production_start") > F.lit(last_watermark)
    )
print(production.count())

# COMMAND ----------

silver_production = (
    production
    .select(
        F.col("production_id").cast("string"),
        F.col("machine_id").cast("string"),
        F.col("product_id").cast("string"),
        F.to_timestamp("production_start").alias("production_start"),
        F.to_timestamp("production_end").alias("production_end"),
        F.col("quantity_produced").cast("long")
    )
    .filter(F.col("production_id").isNotNull())
    .filter(F.col("machine_id").isNotNull())
    .filter(F.col("production_start").isNotNull())
    .filter(F.col("production_end").isNotNull())
    .filter(F.col("quantity_produced") > 0)
    .dropDuplicates(["production_id"])
)

# COMMAND ----------

rows_processed = silver_production.count()

if rows_processed > 0:

    upsert(silver_production, f"{SILVER_PATH}/production", ["production_id"])

    new_watermark = (
        silver_production
        .agg(F.max("production_start").alias("max_production_start"))
        .first()["max_production_start"]
    )

    update_watermark(
        dataset_name="production",
        watermark_column="production_start",
        last_watermark=new_watermark,
        rows_processed=rows_processed
    )

else:

    print("No new production data.")

    update_watermark(
        dataset_name="production",
        watermark_column="production_start",
        last_watermark=last_watermark,
        rows_processed=0
    )

# COMMAND ----------

# MAGIC %md
# MAGIC MAINTENANCE

# COMMAND ----------

maintenance = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(MAINTENANCE_PATH)
)
last_watermark = get_watermark("maintenance")
print(last_watermark)

# COMMAND ----------

if last_watermark is not None:
    maintenance = maintenance.filter(
        F.col("maintenance_date") > F.lit(last_watermark)
    )
print(maintenance.count())

# COMMAND ----------

silver_maintenance = (
    maintenance
    .select(
        F.col("maintenance_id").cast("string"),
        F.col("machine_id").cast("string"),
        F.to_timestamp("maintenance_date").alias("maintenance_date"),
        F.col("maintenance_type").cast("string"),
        F.col("failure_type").cast("string"),
        F.col("downtime_minutes").cast("integer"),
        F.col("technician").cast("string"),
        F.col("cost").cast("double")
    )
    .filter(F.col("maintenance_id").isNotNull())
    .filter(F.col("machine_id").isNotNull())
    .filter(F.col("maintenance_date").isNotNull())
    .filter(F.col("downtime_minutes") >= 0)
    .filter(F.col("cost") >= 0)
    .dropDuplicates(["maintenance_id"])
)

# COMMAND ----------

rows_processed = silver_maintenance.count()

if rows_processed > 0:

    upsert(silver_maintenance, f"{SILVER_PATH}/maintenance", ["maintenance_id"])

    new_watermark = (
        silver_maintenance
        .agg(F.max("maintenance_date").alias("max_maintenance_date"))
        .first()["max_maintenance_date"]
    )

    update_watermark(
        dataset_name="maintenance",
        watermark_column="maintenance_date",
        last_watermark=new_watermark,
        rows_processed=rows_processed
    )

else:

    print("No new maintenance data.")

    update_watermark(
        dataset_name="maintenance",
        watermark_column="maintenance_date",
        last_watermark=last_watermark,
        rows_processed=0
    )

# COMMAND ----------

# MAGIC %md
# MAGIC QUALITY

# COMMAND ----------

quality = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(QUALITY_PATH)
)

# COMMAND ----------

last_watermark = get_watermark("quality")
print(last_watermark)   

# COMMAND ----------

if last_watermark is not None:
    quality = quality.filter(
        F.col("inspection_timestamp") > F.lit(last_watermark)
    )
print(quality.count())

# COMMAND ----------

silver_quality = (
    quality
    .select(
        F.col("inspection_id").cast("string"),
        F.col("production_id").cast("string"),
        F.col("machine_id").cast("string"),
        F.to_timestamp("inspection_timestamp").alias("inspection_timestamp"),
        F.col("quality_score").cast("double"),
        F.col("defect_type").cast("string"),
        F.col("passed").cast("boolean")
    )
    .filter(F.col("inspection_id").isNotNull())
    .filter(F.col("production_id").isNotNull())
    .filter(F.col("machine_id").isNotNull())
    .filter(F.col("quality_score").between(0, 100))
    .dropDuplicates(["inspection_id"])
)



# COMMAND ----------

rows_processed = silver_quality.count()

if rows_processed > 0:

    upsert(silver_quality, f"{SILVER_PATH}/quality", ["inspection_id"])

    new_watermark = (
        silver_quality
        .agg(F.max("inspection_timestamp").alias("max_inspection_timestamp"))
        .first()["max_inspection_timestamp"]
    )

    update_watermark(
        dataset_name="quality",
        watermark_column="inspection_timestamp",
        last_watermark=new_watermark,
        rows_processed=rows_processed
    )

else:

    print("No new quality data.")

    update_watermark(
        dataset_name="quality",
        watermark_column="inspection_timestamp",
        last_watermark=last_watermark,
        rows_processed=0
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Weather

# COMMAND ----------

weather_files = []

for factory_dir in dbutils.fs.ls(WEATHER_PATH):
    factory_id = factory_dir.name.rstrip("/")
    
    weather_file = f"{factory_dir.path}weather.json"
    
    weather_files.append(
        (factory_id, weather_file)
    )

# COMMAND ----------

weather_dfs = []

for factory_id, file_path in weather_files:
    df = (
        spark.read
        .option("multiLine", True)
        .json(file_path)
        .withColumn("factory_id", F.lit(factory_id))
    )
    
    weather_dfs.append(df)

from functools import reduce
weather_raw = reduce(lambda a, b: a.unionByName(b), weather_dfs)


# COMMAND ----------

weather_silver = (
    weather_raw
    .select(
        "factory_id",
        F.posexplode(F.col("hourly.time")).alias("position", "timestamp"),
        F.col("hourly.temperature_2m").alias("temperature_array"),
        F.col("hourly.relative_humidity_2m").alias("humidity_array"),
        F.col("hourly.precipitation").alias("precipitation_array"),
        F.col("hourly.surface_pressure").alias("pressure_array"),
        F.col("hourly.wind_speed_10m").alias("wind_array")
    )
    .select(
        "factory_id",
        F.to_timestamp("timestamp").alias("timestamp"),
        F.col("temperature_array")[F.col("position")].cast("double").alias("temperature_c"),
        F.col("humidity_array")[F.col("position")].cast("double").alias("relative_humidity_pct"),
        F.col("precipitation_array")[F.col("position")].cast("double").alias("precipitation_mm"),
        F.col("pressure_array")[F.col("position")].cast("double").alias("surface_pressure_hpa"),
        F.col("wind_array")[F.col("position")].cast("double").alias("wind_speed_kmh")
    )
)


# COMMAND ----------

last_watermark = get_watermark("weather")
print(last_watermark)

# COMMAND ----------

if last_watermark is not None:
    weather_silver = weather_silver.filter(
        F.col("timestamp") > F.lit(last_watermark)
    )
print(weather_silver.count())

# COMMAND ----------

rows_processed = weather_silver.count()

if rows_processed > 0:

    upsert(
        weather_silver,
        f"{SILVER_PATH}/weather",
        ["factory_id", "timestamp"],
    )

    new_watermark = (
        weather_silver
        .agg(F.max("timestamp").alias("max_timestamp"))
        .first()["max_timestamp"]
    )

    update_watermark(
        dataset_name="weather",
        watermark_column="timestamp",
        last_watermark=new_watermark,
        rows_processed=rows_processed
    )

else:

    print("No new weather data.")

    update_watermark(
        dataset_name="weather",
        watermark_column="timestamp",
        last_watermark=last_watermark,
        rows_processed=0
    )

# COMMAND ----------

