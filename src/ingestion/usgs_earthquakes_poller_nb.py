# Databricks notebook source
# Polls the USGS earthquake GeoJSON summary feed and lands the raw response
# in the per-env S3 landing zone for Auto Loader to pick up in the bronze DLT pipeline.
# Run on a short schedule (e.g. every 15 min) via resources/jobs/ingestion_job.yml
# to approximate a real-time feed without needing a long-running streaming source.
import json
import urllib.request
from datetime import datetime, timezone

dbutils.widgets.text("source_bucket", "s3://OVERRIDE_ME")
dbutils.widgets.text("feed", "all_hour")  # one of: all_hour, all_day, significant_week, etc.

source_bucket = dbutils.widgets.get("source_bucket")
feed = dbutils.widgets.get("feed")

FEED_URL = f"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{feed}.geojson"

# COMMAND ----------
req = urllib.request.Request(FEED_URL, headers={"User-Agent": "hazard-watch/1.0 (green.malik5@gmail.com)"})
with urllib.request.urlopen(req, timeout=30) as resp:
    payload = json.loads(resp.read())

ingest_ts = datetime.now(timezone.utc)
landing_path = (
    f"{source_bucket}/earthquakes/raw/"
    f"year={ingest_ts:%Y}/month={ingest_ts:%m}/day={ingest_ts:%d}/"
    f"usgs_{feed}_{ingest_ts:%Y%m%dT%H%M%SZ}.json"
)

# COMMAND ----------
dbutils.fs.put(landing_path, json.dumps(payload), overwrite=True)
print(f"Landed {len(payload.get('features', []))} features -> {landing_path}")
