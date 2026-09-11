# Databricks notebook source
# NOAA NCEI Storm Events Database: batch ingestion into Bronze.
# Plain notebook (not DLT) + DQX quarantine split, matching the dc-bikeshare house
# style. Unlike a typical bronze layer, this does a MERGE on EVENT_ID rather than an
# overwrite/append, because NCEI republishes corrected versions of a year's file
# months after the initial release -- an overwrite would lose that correction history
# across separate yearly loads, and a plain append would duplicate EVENT_IDs.
#
# CSV extracts are staged manually (NCEI's bulk file names carry an unstable
# "_c{creation-date}" suffix that changes whenever a year is republished, so this
# is intentionally not auto-scraped) at:
#   /Volumes/{catalog}/{bronze_schema}/{volume_name}/storm_events/StormEvents_details-ftp_v1.0_d{YYYY}_c*.csv.gz

# COMMAND ----------
import subprocess
import sys
dqx_whl_volume_path = dbutils.widgets.get("dqx_whl_volume_path")
subprocess.check_call([sys.executable, "-m", "pip", "install", dqx_whl_volume_path, "--quiet"])

# COMMAND ----------
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import VolumeFileChecksStorageConfig
from databricks.sdk import WorkspaceClient
from delta.tables import DeltaTable

# COMMAND ----------
dbutils.widgets.text("catalog", "OVERRIDE_ME")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("quarantine_schema", "quarantine")
dbutils.widgets.text("source_bucket", "s3://OVERRIDE_ME")
dbutils.widgets.text("volume_name", "raw_landing_zone")
dbutils.widgets.text("dqx_whl_volume_path", "OVERRIDE_ME")
dbutils.widgets.text("bundle_target", "dev")

# COMMAND ----------
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
quarantine_schema = dbutils.widgets.get("quarantine_schema")
source_bucket = dbutils.widgets.get("source_bucket")
volume_name = dbutils.widgets.get("volume_name")
bundle_target = dbutils.widgets.get("bundle_target")

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{quarantine_schema}")

# COMMAND ----------
spark.sql(f"""
    CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{bronze_schema}.{volume_name}
    LOCATION '{source_bucket}/raw_landing_zone'
""")

# COMMAND ----------
raw_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/storm_events/StormEvents_details-ftp_v1.0_d*_c*.csv.gz")
)

# COMMAND ----------
ws = WorkspaceClient()
dq_engine = DQEngine(ws)

# COMMAND ----------
df_quality_checks = dq_engine.load_checks(
    config=VolumeFileChecksStorageConfig(location=f"/Volumes/{catalog}/config_files/dqx_files/dqx_checks_storm_events.yaml")
)

# COMMAND ----------
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(raw_df, df_quality_checks)

# COMMAND ----------
if bundle_target == "dev":
    display(valid_df)

# COMMAND ----------
if bundle_target == "dev":
    display(quarantined_df)

# COMMAND ----------
bronze_table = f"{catalog}.{bronze_schema}.storm_events_bt"

if spark.catalog.tableExists(bronze_table):
    target = DeltaTable.forName(spark, bronze_table)
    (
        target.alias("t")
        .merge(valid_df.alias("s"), "t.EVENT_ID = s.EVENT_ID")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    valid_df.write.format("delta").saveAsTable(bronze_table)

# COMMAND ----------
quarantined_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(f"{catalog}.{quarantine_schema}.storm_events_quarantined")
