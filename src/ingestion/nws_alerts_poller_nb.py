# Databricks notebook source
# Polls the NWS/NOAA active alerts feed and lands the raw response in the
# per-env S3 landing zone for Auto Loader to pick up in the bronze DLT pipeline.
# api.weather.gov requires a descriptive User-Agent identifying the app + contact
# per NWS API usage policy; requests without one get throttled/blocked.
import json
import urllib.request
from datetime import datetime, timezone

dbutils.widgets.text("source_bucket", "s3://OVERRIDE_ME")
dbutils.widgets.text("area", "")  # optional 2-letter state/marine area code; blank = all active US alerts

source_bucket = dbutils.widgets.get("source_bucket")
area = dbutils.widgets.get("area")

ALERTS_URL = "https://api.weather.gov/alerts/active" + (f"?area={area}" if area else "")

# COMMAND ----------
req = urllib.request.Request(
    ALERTS_URL,
    headers={
        "User-Agent": "hazard-watch/1.0 (green.malik5@gmail.com)",
        "Accept": "application/geo+json",
    },
)
with urllib.request.urlopen(req, timeout=30) as resp:
    payload = json.loads(resp.read())

ingest_ts = datetime.now(timezone.utc)
landing_path = (
    f"{source_bucket}/alerts/raw/"
    f"year={ingest_ts:%Y}/month={ingest_ts:%m}/day={ingest_ts:%d}/"
    f"nws_alerts_{ingest_ts:%Y%m%dT%H%M%SZ}.json"
)

# COMMAND ----------
dbutils.fs.put(landing_path, json.dumps(payload), overwrite=True)
print(f"Landed {len(payload.get('features', []))} alerts -> {landing_path}")
