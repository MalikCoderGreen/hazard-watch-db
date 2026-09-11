# Databricks notebook source
# Lakeflow Declarative Pipeline (DLT): NWS/NOAA active alerts bronze -> silver.
# Bronze is genuine incremental streaming via Auto Loader (cloudFiles) with
# schema evolution, reading the poller's landed GeoJSON FeatureCollection files.
# Silver explodes the per-alert features, derives a state abbreviation from the
# first UGC zone code (e.g. "DCZ001" -> "DC") so alerts can join to dim_state,
# and applies DLT expectations for data quality.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_BUCKET = spark.conf.get("hazard_watch.source_bucket")
BRONZE_SCHEMA = spark.conf.get("hazard_watch.bronze_schema")
SILVER_SCHEMA = spark.conf.get("hazard_watch.silver_schema")

# COMMAND ----------
@dp.table(name=f"{BRONZE_SCHEMA}.alerts_bronze", comment="Raw NWS active-alerts GeoJSON FeatureCollection payloads, one row per poll.")
def alerts_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("multiLine", "true")
        .load(f"{SOURCE_BUCKET}/alerts/raw/")
        .withColumn("_ingest_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_timestamp", F.current_timestamp())
    )

# COMMAND ----------
ALERT_EXPECTATIONS = {
    "valid_alert_id": "id IS NOT NULL",
    "valid_event": "event IS NOT NULL",
    "valid_severity": "severity IS NOT NULL",
    "valid_effective": "effective IS NOT NULL",
}

@dp.table(name=f"{SILVER_SCHEMA}.alerts_silver", comment="One row per active alert, quality-filtered, with a derived state_abbr for dim_state joins.")
@dp.expect_all_or_drop(ALERT_EXPECTATIONS)
def alerts_silver():
    features = (
        spark.readStream.table(f"{BRONZE_SCHEMA}.alerts_bronze")
        .select(F.explode("features").alias("feature"), "_ingest_timestamp")
    )
    return features.select(
        F.col("feature.properties.id").alias("id"),
        F.col("feature.properties.event").alias("event"),
        F.col("feature.properties.severity").alias("severity"),
        F.col("feature.properties.certainty").alias("certainty"),
        F.col("feature.properties.urgency").alias("urgency"),
        F.col("feature.properties.headline").alias("headline"),
        F.col("feature.properties.areaDesc").alias("area_desc"),
        F.upper(F.substring(F.element_at(F.col("feature.properties.geocode.UGC"), 1), 1, 2)).alias("state_abbr"),
        F.col("feature.properties.effective").cast("timestamp").alias("effective"),
        F.col("feature.properties.expires").cast("timestamp").alias("expires"),
        F.col("feature.properties.senderName").alias("sender_name"),
        F.col("_ingest_timestamp"),
    )
