# Databricks notebook source
# COMMAND ----------
import subprocess
import sys

dbutils.widgets.text("hw_whl_volume_path", "OVERRIDE_ME")
hw_whl_volume_path = dbutils.widgets.get("hw_whl_volume_path")
subprocess.check_call([sys.executable, "-m", "pip", "install", hw_whl_volume_path, "--quiet"])

# COMMAND ----------
import hw_transformations
from pyspark.sql import functions as F

# COMMAND ----------
dbutils.widgets.text("catalog", "OVERRIDE_ME")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

# COMMAND ----------
storm_events_df = spark.read.table(f"{catalog}.{bronze_schema}.storm_events_bt")

# COMMAND ----------
storm_events_df = storm_events_df.dropDuplicates(["EVENT_ID"])

# COMMAND ----------
storm_events_df = storm_events_df.select(
    F.col("EVENT_ID").cast("long").alias("event_id"),
    F.col("STATE").alias("state_name"),
    F.col("STATE_FIPS").cast("int").alias("state_fips"),
    F.col("CZ_FIPS").cast("int").alias("cz_fips"),
    F.col("EVENT_TYPE").alias("event_type"),
    F.to_timestamp("BEGIN_DATE_TIME", "dd-MMM-yy HH:mm:ss").alias("begin_time"),
    F.to_timestamp("END_DATE_TIME", "dd-MMM-yy HH:mm:ss").alias("end_time"),
    F.col("MAGNITUDE").cast("double").alias("magnitude"),
    F.col("DEATHS_DIRECT").cast("int").alias("deaths_direct"),
    F.col("DEATHS_INDIRECT").cast("int").alias("deaths_indirect"),
    F.col("INJURIES_DIRECT").cast("int").alias("injuries_direct"),
    F.col("INJURIES_INDIRECT").cast("int").alias("injuries_indirect"),
    F.col("DAMAGE_PROPERTY").alias("damage_property_raw"),
    F.col("DAMAGE_CROPS").alias("damage_crops_raw"),
    F.col("EPISODE_NARRATIVE").alias("episode_narrative"),
)

# COMMAND ----------
storm_events_df = hw_transformations.parse_damage_amount(storm_events_df, "damage_property_raw", "damage_property_usd")
storm_events_df = hw_transformations.parse_damage_amount(storm_events_df, "damage_crops_raw", "damage_crops_usd")
storm_events_df = storm_events_df.withColumn(
    "damage_total_usd", F.col("damage_property_usd") + F.col("damage_crops_usd")
)

# COMMAND ----------
storm_events_df = hw_transformations.severity_score(
    storm_events_df, "deaths_direct", "injuries_direct", "damage_total_usd", "severity_score"
)

# COMMAND ----------
storm_events_df = storm_events_df.withColumn("event_year", F.year("begin_time"))
storm_events_df = storm_events_df.withColumn("event_month", F.month("begin_time"))
storm_events_df = storm_events_df.filter(F.col("begin_time").isNotNull())
storm_events_df = storm_events_df.filter(F.col("begin_time") <= F.current_timestamp())
storm_events_df = storm_events_df.filter((F.col("end_time").isNull()) | (F.col("end_time") >= F.col("begin_time")))

# COMMAND ----------
storm_events_df = storm_events_df.withColumn("_ingestion_timestamp", F.current_timestamp())

# COMMAND ----------
(
    storm_events_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_year")
    .saveAsTable(f"{catalog}.{silver_schema}.storm_events_st")
)
