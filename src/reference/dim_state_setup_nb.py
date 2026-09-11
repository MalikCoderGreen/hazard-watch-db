# Databricks notebook source
# Loads the static state/territory FIPS reference dimension into silver.dim_state
# and gold.dim_state. This is the shared join key that lets the three otherwise
# unrelated sources (USGS earthquakes -> nearest-state lookup TBD, NWS alerts via
# state_abbr, NOAA Storm Events via state_fips) roll up together in gold.
# Static reference data -- run on demand, not on the daily hazard-event schedule.
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "OVERRIDE_ME")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")
# `__file__` isn't defined when a notebook-as-file task runs on a job cluster,
# so the sibling CSV's path is passed in explicitly via DAB's `${workspace.file_path}`
# substitution (the bundle's synced-files root) rather than derived from the
# notebook's own location.
dbutils.widgets.text("csv_workspace_path", "OVERRIDE_ME")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
csv_workspace_path = dbutils.widgets.get("csv_workspace_path")

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

# COMMAND ----------
dim_state_df = (
    spark.read.format("csv")
    .option("header", "true")
    .load(f"file:{csv_workspace_path}")
    .withColumn("state_fips", F.col("state_fips").cast("int"))
)

# COMMAND ----------
dim_state_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{silver_schema}.dim_state"
)
dim_state_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{gold_schema}.dim_state"
)
