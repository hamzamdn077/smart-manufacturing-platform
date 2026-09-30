# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType
)
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS smart_manufacturing_classic.lakehouse
# MAGIC COMMENT 'Schema for Bronze, Silver, and Gold layers';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS smart_manufacturing_classic.lakehouse.bronze_volume
# MAGIC LOCATION 'abfss://bronze@smartmanufacturing.dfs.core.windows.net/';
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS smart_manufacturing_classic.lakehouse.silver_volume
# MAGIC LOCATION 'abfss://silver@smartmanufacturing.dfs.core.windows.net/';
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS smart_manufacturing_classic.lakehouse.gold_volume
# MAGIC LOCATION 'abfss://gold@smartmanufacturing.dfs.core.windows.net/';

# COMMAND ----------

SILVER_PATH = "/Volumes/smart_manufacturing_classic/lakehouse/silver_volume"
metadata_schema = StructType([
    StructField("dataset_name", StringType(), False),
    StructField("watermark_column", StringType(), False),
    StructField("last_watermark", TimestampType(), True),
    StructField("last_successful_run", TimestampType(), True),
    StructField("rows_processed", LongType(), True),
    StructField("status", StringType(), True)
])



# COMMAND ----------

metadata_df = spark.createDataFrame([], metadata_schema)
METADATA_PATH = f"{SILVER_PATH}/pipeline_metadata"
if not DeltaTable.isDeltaTable(spark, METADATA_PATH):
    (
        spark.createDataFrame([], metadata_schema)
        .write
        .format("delta")
        .mode("overwrite")
        .save(METADATA_PATH)
    )
    print("Metadata table created.")
else:
    print("Metadata table already exists — skipping.")

# COMMAND ----------

