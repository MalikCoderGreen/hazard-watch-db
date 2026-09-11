# Hazard Watch

Hazard Watch is a public-sector hazards data platform built on Databricks. It
combines real-time USGS earthquake detections and NWS/NOAA active weather
alerts with historical severity context from NOAA's Storm Events Database,
giving a single, governed view of hazard activity across every U.S. state and
territory.

The platform demonstrates a production-style Lakehouse pattern on Databricks:
medallion architecture, Auto Loader ingestion with schema evolution, Lakeflow
Declarative Pipelines (including a materialized view), automated data quality
enforcement, Unity Catalog governance, and cost-aware operations — all built
within Databricks Free Trial Edition and a fixed compute budget.

---

## Data Sources

| Source | Type | Notes |
| --- | --- | --- |
| [USGS Earthquake GeoJSON Feed](https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/) | Near real-time (polled every 15 min) | Public, no authentication required |
| [NWS/NOAA Active Alerts API](https://www.weather.gov/documentation/services-web-api) | Near real-time (polled every 15 min) | Public; requires only a descriptive `User-Agent` header |
| [NOAA NCEI Storm Events Database](https://www.ncdc.noaa.gov/stormevents/) | Historical batch | Public; annual CSV extracts, periodically corrected/republished by NOAA |

All three sources are public, requiring no credentials or paid access —
consistent with using only public or synthetic data.

---

## Architecture

```
                      USGS quake feed          NWS alerts feed         NCEI Storm Events CSV
                            │                        │                          │
                     (poller notebook,         (poller notebook,          (staged annually
                      every 15 min)              every 15 min)             into landing zone)
                            │                        │                          │
                            ▼                        ▼                          ▼
                 S3 landing zone              S3 landing zone           UC Volume landing zone
                            │                        │                          │
   ══════ Lakeflow Declarative Pipeline ══════════════════════════════ │       Notebook + DQX
                            ▼                        ▼                          ▼
                  bronze.earthquakes_bronze   bronze.alerts_bronze      bronze.storm_events_bt
                    (Auto Loader, schema         (Auto Loader, schema     (batch load, DQX quality
                     evolution)                    evolution)              checks, incremental
                            │                        │                      merge — NOAA republishes
                            ▼                        ▼                      corrected annual files)
                  silver.earthquakes_silver   silver.alerts_silver             │
                    (declarative quality        (declarative quality           ▼
                     expectations)               expectations)          silver.storm_events_st
                            │                        │                  (severity scoring, damage
                            │                        │                   normalization)
                            └────────────┬───────────┴──────────────┬───────────┘
                                         ▼                          │
                                 gold.dim_state ◄────────────────────┘
                             (state/territory reference dimension)
                                         │
                     ══════ Lakeflow Declarative Pipeline ══════
                                         ▼
                    gold.agg_hazard_activity_by_state_daily
                              (Materialized View)
                       Earthquake activity, alert severity, and storm
                          event impact — rolled up by state and day
```

Data flows through the standard **bronze → silver → gold** medallion
structure, with quarantined records held in their own schema for governed
review rather than being silently dropped.

---

## Design Decisions

**Hybrid ingestion strategy.** The real-time sources (USGS, NWS) run as
Lakeflow Declarative Pipelines end to end: Auto Loader lands raw data
incrementally with automatic schema evolution, and declarative quality
expectations gate what reaches the silver layer. The historical Storm Events
dataset is ingested as scheduled notebooks instead, since it arrives as
irregular annual batch files rather than a continuous stream — this uses
Databricks Labs DQX to validate records and route failures to quarantine
rather than a declarative pipeline's expectation model, which is better
suited to streaming validation.

**The gold layer is a materialized view**, not a static table. Because the
aggregation is a pure read-and-transform over the silver tables, defining it
as a Lakeflow Declarative Pipeline lets Databricks manage refresh and
incremental maintenance automatically, rather than relying on a scheduled
notebook to recompute and overwrite the table on every run.

**Quality gates hold rather than discard.** Every ingestion path uses DQX
data-quality checks; records that fail validation are written to a dedicated
`quarantine` schema (kept separate from `bronze` for cleaner, independently
governed access) rather than being dropped, preserving a full audit trail of
what failed and why.

**Cross-source joins use validated dimensions, not raw source codes.**
Earthquakes, alerts, and storm events each report location differently (free
text, NWS zone codes, and NOAA's own non-Census FIPS numbering,
respectively), and none of those raw codes are reliable keys on their own —
NOAA's scheme, for example, assigns different codes to territories than the
Census Bureau does, and both NWS and NOAA repurpose parts of their numbering
for non-state marine zones. The gold layer resolves all three against a
validated state/territory reference dimension before aggregating, so the
rollup reflects real states and territories only.

**Governance is enforced through Unity Catalog, not convention.** Every
catalog, schema, and table carries Unity Catalog tags for classification and
data lineage (data sensitivity, source system, medallion layer), queryable
through the standard `information_schema` views. All jobs and pipelines run
under dedicated service principals rather than personal credentials, with
least-privilege grants scoped to exactly what each workload needs.

**Cost is monitored, not assumed.** Two Lakeview dashboards give continuous
visibility into platform health: DBU spend against the project's budget cap
(broken down by job/pipeline, using Databricks' system billing tables), and
data quality trends via the quarantine rate. Both are deployed as
first-class Databricks Asset Bundle resources alongside the pipelines they
monitor.

---

## Project Structure

```
hazard-watch/
├── databricks.yml                     # Databricks Asset Bundle config (dev/staging/prod targets)
├── dqx_checks_storm_events.yaml       # DQX data-quality rules for Storm Events ingestion
├── resources/
│   ├── pipelines/
│   │   ├── dlt_earthquakes_pipeline.yml   # USGS bronze -> silver
│   │   ├── dlt_alerts_pipeline.yml        # NWS bronze -> silver
│   │   └── dlt_gold_pipeline.yml          # Gold rollup (materialized view)
│   ├── jobs/
│   │   ├── ingestion_job.yml          # Scheduled pollers + pipeline triggers
│   │   ├── storm_events_etl.yml       # Storm Events batch load (DQX + incremental merge)
│   │   └── gold_etl.yml               # Reference data refresh + gold pipeline
│   └── dashboards/
│       ├── hazard_watch_dbu_burn.yml           # Cost monitoring
│       └── hazard_watch_quarantine_rate.yml    # Data quality monitoring
├── src/
│   ├── ingestion/                     # USGS + NWS API pollers
│   ├── pipelines/                     # Declarative pipeline definitions (bronze/silver/gold)
│   ├── storm_events/                  # Batch ingestion notebooks (bronze/silver)
│   ├── reference/                     # Static state/territory reference dimension
│   ├── hw_transformations.py          # Shared transformation logic (unit-tested)
│   └── *.lvdash.json                  # Lakeview dashboard definitions
├── tests/                             # Unit tests
└── pyproject.toml / requirements.txt
```

---

## Deployment

The platform is deployed via Databricks Asset Bundles across three isolated
environments — development, staging, and production — each with its own
Unity Catalog catalog, S3 landing zone, and service principal:

```bash
databricks bundle validate -t <dev|staging|prod>
databricks bundle deploy -t <dev|staging|prod>

# Run the pipelines
databricks bundle run hazard_ingestion_job -t <dev|staging|prod>
databricks bundle run storm_events_etl -t <dev|staging|prod>
databricks bundle run gold_etl -t <dev|staging|prod>
```

All three environments are deployed and fully operational, running under
their respective service principals with no personal-user credentials
involved in execution.

---

## Data Quality

Data quality is enforced with [Databricks Labs DQX](https://github.com/databrickslabs/dqx)
at ingestion time. Checks validate required fields, referential domains
(event types, magnitude ranges, geographic bounds), and temporal consistency.
Records that fail validation are written to a dedicated `quarantine` schema
for review rather than discarded, and the quarantine rate is tracked on its
own Lakeview dashboard as an ongoing data-quality signal.

---

## Testing

Shared transformation logic is covered by unit tests using a local Spark
session:

```bash
pytest tests/ -v
```

---

## Current Status

All three environments (dev, staging, prod) are deployed and running
end-to-end, each independently populated from live data:

| Environment | Gold rows | States/territories | Storm events | Earthquakes | Active alerts |
| --- | --- | --- | --- | --- | --- |
| Development | 11,613 | 55 | 137,190 | 7 | 267 |
| Staging | 11,584 | 55 | 137,190 | 7 | 248 |
| Production | 11,584 | 55 | 137,190 | 10 | 454 |

Storm event counts match exactly across environments (loaded from the same
historical NOAA extract); earthquake and alert counts differ naturally, since
each environment's pollers capture live conditions at different moments.

Two Lakeview dashboards are live in every environment: DBU spend tracked
against the project's cost cap, and the Storm Events quarantine rate.

---

## Roadmap

- Automate annual staging of new NOAA Storm Events extracts as NOAA publishes them
- Extend Unity Catalog tag coverage to newly-created tables as staging/production accumulate history
- Adopt Databricks serverless budget policies for per-workload cost attribution once available in this account
