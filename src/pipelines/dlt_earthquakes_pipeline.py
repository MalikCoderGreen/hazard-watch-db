# Databricks notebook source
# Lakeflow Declarative Pipeline (DLT): USGS earthquakes bronze -> silver.
# Bronze is genuine incremental streaming via Auto Loader (cloudFiles) with
# schema evolution, reading the poller's landed GeoJSON FeatureCollection files.
# Silver explodes the per-quake features and applies DLT expectations for data quality.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_BUCKET = spark.conf.get("hazard_watch.source_bucket")
BRONZE_SCHEMA = spark.conf.get("hazard_watch.bronze_schema")
SILVER_SCHEMA = spark.conf.get("hazard_watch.silver_schema")

# COMMAND ----------
@dp.table(name=f"{BRONZE_SCHEMA}.earthquakes_bronze", comment="Raw USGS GeoJSON FeatureCollection payloads, one row per poll.")
def earthquakes_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("multiLine", "true")
        .load(f"{SOURCE_BUCKET}/earthquakes/raw/")
        .withColumn("_ingest_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_timestamp", F.current_timestamp())
    )

# COMMAND ----------
EARTHQUAKE_EXPECTATIONS = {
    "valid_event_id": "id IS NOT NULL",
    "valid_magnitude": "mag IS NOT NULL AND mag BETWEEN -2 AND 10",
    "valid_event_time": "event_time IS NOT NULL",
    "valid_coordinates": "longitude BETWEEN -180 AND 180 AND latitude BETWEEN -90 AND 90",
}

@dp.table(name=f"{SILVER_SCHEMA}.earthquakes_silver", comment="One row per earthquake, quality-filtered, with derived event_time and location.")
@dp.expect_all_or_drop(EARTHQUAKE_EXPECTATIONS)
def earthquakes_silver():
    features = (
        spark.readStream.table(f"{BRONZE_SCHEMA}.earthquakes_bronze")
        .select(F.explode("features").alias("feature"), "_ingest_timestamp")
    )
    return features.select(
        F.col("feature.id").alias("id"),
        F.col("feature.properties.mag").alias("mag"),
        F.col("feature.properties.place").alias("place"),
        F.col("feature.properties.type").alias("event_type"),
        F.col("feature.properties.status").alias("status"),
        F.col("feature.properties.tsunami").cast("boolean").alias("tsunami_flag"),
        (F.col("feature.properties.time") / 1000).cast("timestamp").alias("event_time"),
        F.col("feature.geometry.coordinates")[0].alias("longitude"),
        F.col("feature.geometry.coordinates")[1].alias("latitude"),
        F.col("feature.geometry.coordinates")[2].alias("depth_km"),
        F.col("_ingest_timestamp"),
    )
