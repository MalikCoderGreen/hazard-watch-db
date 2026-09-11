# Databricks notebook source
# Lakeflow Declarative Pipeline (DLT): gold cross-source daily hazard-activity
# rollup by state, as a Materialized View instead of a plain overwritten table.
# All three inputs are batch (non-streaming) reads, so `@dp.table` here
# produces a materialized view -- the pipeline engine tracks the query and
# incrementally maintains the result on refresh, rather than every run doing a
# full recompute-and-overwrite by hand the way the old plain-notebook version did.
#
# Joins USGS earthquakes, NWS alerts, and NOAA Storm Events (three otherwise-
# unrelated sources) through gold.dim_state, giving a single dimensional model
# instead of three siloed fact tables.
#
# Earthquakes don't carry a state directly -- USGS `place` strings are free text
# like "12km SSW of Ridgecrest, CA" or "South Sandwich Islands region" (no state).
# We extract a trailing ", XX" 2-letter code where present and validate it
# against dim_state; quakes without a matching US state code (mostly
# international/oceanic events) are excluded from this state-level rollup by
# design. Alerts and storm events get the same "validate against dim_state,
# don't trust the raw source code" treatment below, for the same reason: NWS
# UGC codes and NOAA's own STATE_FIPS scheme both include marine-zone/pseudo
# codes that look plausible but aren't real states.
#
# dim_state itself stays a plain-notebook load (src/reference/dim_state_setup_nb.py)
# -- it's a static external seed, not a derived aggregate, so there's nothing
# for a materialized view to incrementally maintain there. This pipeline reads
# it as an ordinary existing UC table, not a table it manages.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER_SCHEMA = spark.conf.get("hazard_watch.silver_schema")
GOLD_SCHEMA = spark.conf.get("hazard_watch.gold_schema")

# COMMAND ----------
@dp.table(
    name=f"{GOLD_SCHEMA}.agg_hazard_activity_by_state_daily",
    comment="Materialized view: daily hazard activity (earthquakes, alerts, storm events) by state.",
)
def agg_hazard_activity_by_state_daily():
    dim_state = spark.read.table(f"{GOLD_SCHEMA}.dim_state")
    valid_state_abbrs = dim_state.select("state_abbr").distinct()

    earthquakes = spark.read.table(f"{SILVER_SCHEMA}.earthquakes_silver")
    earthquakes = earthquakes.withColumn("state_abbr", F.upper(F.regexp_extract("place", r",\s*([A-Za-z]{2})$", 1)))
    earthquakes_daily = (
        earthquakes.filter(F.col("state_abbr") != "")
        .join(valid_state_abbrs, on="state_abbr", how="inner")
        .groupBy("state_abbr", F.to_date("event_time").alias("activity_date"))
        .agg(
            F.count("*").alias("earthquake_count"),
            F.round(F.avg("mag"), 2).alias("avg_magnitude"),
            F.max("mag").alias("max_magnitude"),
        )
    )

    alerts = spark.read.table(f"{SILVER_SCHEMA}.alerts_silver")
    alerts_daily = (
        alerts.filter(F.col("state_abbr").isNotNull())
        .join(valid_state_abbrs, on="state_abbr", how="inner")
        .groupBy("state_abbr", F.to_date("effective").alias("activity_date"))
        .agg(
            F.count("*").alias("alert_count"),
            F.sum(F.when(F.col("severity").isin("Extreme", "Severe"), 1).otherwise(0)).alias("severe_alert_count"),
        )
    )

    storm_events = spark.read.table(f"{SILVER_SCHEMA}.storm_events_st")
    # Join by state NAME, not STATE_FIPS: NOAA's own STATE_FIPS numbering for storm
    # events diverges from the real Census FIPS codes in dim_state for territories
    # (e.g. Puerto Rico is 99 in NOAA's scheme, 72 in dim_state) and repurposes
    # 81-99 for marine zones (Gulf of Mexico, the Great Lakes, ...) that have no
    # state at all.
    storm_events_with_state = storm_events.join(
        dim_state.select(F.upper("state_name").alias("state_name_upper"), "state_abbr"),
        on=storm_events["state_name"] == F.col("state_name_upper"),
        how="inner",
    )
    storm_events_daily = storm_events_with_state.groupBy(
        "state_abbr", F.to_date("begin_time").alias("activity_date")
    ).agg(
        F.count("*").alias("storm_event_count"),
        F.sum("damage_total_usd").alias("storm_damage_total_usd"),
        F.sum("deaths_direct").alias("storm_deaths_direct"),
        F.sum("severity_score").alias("storm_severity_score_sum"),
    )

    combined = (
        earthquakes_daily.select("state_abbr", "activity_date")
        .unionByName(alerts_daily.select("state_abbr", "activity_date"))
        .unionByName(storm_events_daily.select("state_abbr", "activity_date"))
        .distinct()
    )

    gold_df = (
        combined.join(earthquakes_daily, on=["state_abbr", "activity_date"], how="left")
        .join(alerts_daily, on=["state_abbr", "activity_date"], how="left")
        .join(storm_events_daily, on=["state_abbr", "activity_date"], how="left")
        .join(dim_state.select("state_abbr", "state_name"), on="state_abbr", how="left")
    )

    count_cols = [
        "earthquake_count", "alert_count", "severe_alert_count",
        "storm_event_count", "storm_damage_total_usd", "storm_deaths_direct", "storm_severity_score_sum",
    ]
    for c in count_cols:
        gold_df = gold_df.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

    return gold_df.withColumn("_refreshed_at", F.current_timestamp())
