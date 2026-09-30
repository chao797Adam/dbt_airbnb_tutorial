# dbt Airbnb Case

A layered Airbnb data modeling project built with [dbt](https://www.getdbt.com/) on Databricks. It demonstrates an end-to-end pipeline from raw source data to analytics-ready mart tables, covering **incremental loads, snapshot-based history tracking, custom macros, and data tests**.

---

## 📐 Data Layer Architecture

The project follows a **source / bronze / silver / gold** architecture, mapped to dbt's `staging` / `intermediate` / `mart` directories:

| Layer | Directory | Schema | Materialization | Description |
|-------|-----------|--------|------------------|-------------|
| Source (Raw) | — | `source` | — | Raw data landed by Auto Loader from CSV volumes. All columns are strings; includes `_rescued_data`, `ingested_at`, `source_file` |
| Bronze (Staging) | `models/staging` | `bronze` | `incremental` | Type casting, column standardization. Incrementally loaded by `ingested_at`; deduplicated by `row_number()` on the primary key |
| Silver (Intermediate) | `models/intermediate` | `silver` | `incremental` + `merge` | Business-level cleaning, derived fields, and tagging |
| Gold (Serving) | `models/mart` | `gold` | `view` / `table` | Analytics-ready fact tables and one-big-table (OBT) |

```
source(airbnb_source) ──▶ staging (bronze) ──▶ intermediate (silver) ──▶ mart (gold)
```

---

## 📁 Project Structure

```
dbt_airbnb_case/
├── analyses/              # Ad-hoc analysis SQL (not materialized)
├── macros/                # Custom macros
│   ├── multiply.sql           # Multiply two numbers and round
│   ├── schema.sql             # Custom schema naming logic
│   └── tag.sql                # Bucket numeric values into tags
├── models/
│   ├── staging/            # Bronze layer
│   │   ├── sources.yml         # Source definitions
│   │   ├── properties.yml      # Tests / descriptions for staging models
│   │   ├── stg_bookings.sql
│   │   ├── stg_hosts.sql
│   │   └── stg_listings.sql
│   ├── intermediate/       # Silver layer
│   │   ├── silver_bookings.sql
│   │   ├── silver_hosts.sql
│   │   └── silver_listings.sql
│   └── mart/                # Gold layer
│       ├── gold_fact_bookings.sql
│       └── obt.sql              # One Big Table
├── snapshots/               # SCD2 historical snapshots
│   ├── dim_hosts_snapshot.sql
│   └── dim_listings_snapshot.sql
├── tests/                    # Custom data tests
├── dbt_project.yml
└── packages.yml
```

---

## 🗄️ Source

Source tables are defined in `models/staging/sources.yml`:

| Source | Catalog | Schema | Tables |
|--------|---------|--------|--------|
| `airbnb_source` | `dbt_airbnb` | `source` | `listings`, `bookings`, `hosts` |

Raw tables are written by an Auto Loader notebook into `dbt_airbnb.source.{table_name}`. Checkpoints and schema inference files live in a Unity Catalog Volume under `/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/`.

---

## 📥 Bronze Ingestion (Auto Loader Notebook)

Raw CSV files are loaded into the `source` schema via an Auto Loader notebook. The notebook is parameterized with a `table_name` widget, so the same notebook is reused for `bookings`, `listings`, and `hosts` — only the widget value changes between runs.

### Step 1 — Define and read the widget

```python
from pyspark.sql.functions import current_timestamp, col

# 1. Define the widget — appears as a parameter at the top of the notebook
dbutils.widgets.text("table_name", "", "Table Name")

# 2. Read the widget value at runtime
table_name = dbutils.widgets.get("table_name").strip()

print(f"=== Running Bronze ingestion stream for table: {table_name} ===")
```

### Step 2 — Auto Loader stream into the source schema

```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", f"/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/schema") \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .load(f"/Volumes/dbt_airbnb/source/source_data/{table_name}/")

df = df.withColumn("ingested_at", current_timestamp()) \
       .withColumn("source_file", col("_metadata.file_path"))

query = df.writeStream.format("delta") \
    .outputMode("append") \
    .trigger(once=True) \
    .option("checkpointLocation", f"/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/checkpoint") \
    .toTable(f"dbt_airbnb.source.{table_name}")

query.awaitTermination()

prog = query.lastProgress
print(f"✅ batchId      : {prog.get('batchId') if prog else None}")

total = spark.sql(f"SELECT COUNT(*) FROM dbt_airbnb.source.{table_name}").collect()[0][0]
print(f"✅ Total rows in table : {total}")

hist = spark.sql(f"DESCRIBE HISTORY dbt_airbnb.source.{table_name} LIMIT 1").collect()[0]
metrics = hist["operationMetrics"] or {}
print(f"✅ Rows written this run : {metrics.get('numOutputRows', 'N/A')}")
print(f"✅ operation            : {hist['operation']}")
```

**How it works**

- **Input:** CSV files under `/Volumes/dbt_airbnb/source/source_data/{table_name}/`
- **Output:** Delta table `dbt_airbnb.source.{table_name}`
- **Schema evolution:** `rescue` mode — unmatched or malformed fields land in `_rescued_data` instead of failing the stream
- **Audit columns:** `ingested_at` (processing timestamp) and `source_file` (origin file path) are appended to every row
- **Trigger:** `once=True` — runs a single micro-batch and exits

### Running the notebook

**Manual (for development / learning)**

Set the `table_name` widget to one of `bookings`, `listings`, `hosts`, and run the notebook once per table. Checkpoints and schema files are stored under `/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/`.

**Production (Databricks Workflow with a For each task)**

In production, the notebook is not run manually per table. It is orchestrated as a two-step Databricks Workflow:

1. **Upstream task — publish the table list.** A small notebook emits the list of tables to ingest:

```python
table_list = ["bookings", "listings", "hosts"]
dbutils.jobs.taskValues.set(key="table_list", value=table_list)
```

2. **Downstream task — For each.** The Workflow uses a **For each** task that iterates over `table_list`. Each iteration runs the ingestion notebook defined in [Step 2 — Auto Loader stream into the source schema](#step-2--auto-loader-stream-into-the-source-schema), passing the current element as the `table_name` widget.

This keeps the ingestion notebook fully table-agnostic — it only knows the single `table_name` it receives — while the Workflow handles fan-out and concurrency. Adding a new source table is just a matter of appending its name to `table_list`.

Checkpoints and schema files are stored under `/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/`.

---

## 🧱 Models

### Staging (Bronze)

Casts raw string columns into proper types, standardizes column names, and incrementally loads new rows based on `ingested_at`. Each model deduplicates by its primary key using `qualify row_number() over (partition by ... order by ingested_at desc) = 1`.

| Model | Primary Key | Description |
|-------|-------------|-------------|
| `stg_listings` | `listing_id` | Listing details (property type, city, price, etc.) |
| `stg_hosts` | `host_id` | Host info (signup date, superhost flag, response rate) |
| `stg_bookings` | `booking_id` | Booking records (nights booked, amounts, status) |

**Incremental logic:** each staging model filters on `ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})` when running in incremental mode.

### Intermediate (Silver)

Applies business logic, cleaning, and derived fields on top of staging:

| Model | Key Logic |
|-------|-----------|
| `silver_bookings` | Computes `total_amount = nights_booked * booking_amount` using the `multiply_and_round` macro; dedup by `booking_id` |
| `silver_hosts` | Cleans `host_name` (replaces spaces with underscores); tags `response_rate` as `very_good` / `good` / `fair` / `poor`; dedup by `host_id` |
| `silver_listings` | Buckets `price_per_night` into `low` / `medium` / `high` using the `tag_col` macro; dedup by `listing_id` |

All three intermediate models use `materialized='incremental'` with `unique_key` and `qualify row_number() = 1` to keep the latest version per key.

### Mart (Gold)

| Model | Materialization | Description |
|-------|------------------|-------------|
| `gold_fact_bookings` | `table` | Booking fact table, joined with listing dimensions (city, property type) |
| `obt` | `table` | One Big Table combining all fields from Bookings, Listings, and Hosts for BI/analytics |

---

## 🛠️ Custom Macros

| Macro | Purpose |
|-------|---------|
| `multiply_and_round(col1, col2, precision=2)` | Multiplies two columns and rounds to the given precision |
| `tag_col(col)` | Buckets a numeric column into tags: `<100` → `low`, `<200` → `medium`, else → `high` |
| `generate_schema_name(custom_schema_name, node)` | Overrides dbt's default schema naming so models use their configured `schema` directly, without prefixing the target schema |

---

## 📸 Snapshots

Used to track historical changes (SCD Type 2) for dimension data, stored in the `snapshots` schema:

| Snapshot | Source Model | Strategy | Tracked Columns |
|----------|--------------|----------|------------------|
| `hosts_snapshot` | `silver_hosts` | `check` | `host_name`, `is_superhost`, `response_rate`, `response_rate_tag` |
| `listings_snapshot` | `silver_listings` | `check` | `price_per_night`, `price_per_night_tag`, `room_type`, `bedrooms`, `bathrooms`, `accommodates`, `city`, `country` |

> ⚠️ Note: the project-level default snapshot strategy in `dbt_project.yml` is `timestamp` (based on `updated_at`/`ingested_at`), but both snapshot files explicitly set `strategy='check'`, which overrides the project default.

---

## ✅ Data Tests

Generic tests are declared in `models/staging/properties.yml`:

| Model | Column | Tests |
|-------|--------|-------|
| `stg_bookings` | `booking_id` | `not_null`, `unique` |
| `stg_hosts` | `host_id` | `not_null`, `unique` |
| `stg_listings` | `listing_id` | `not_null`, `unique` |

The `dbt_utils` package (declared in `packages.yml`, version `1.3.3`) is also available and can be used to add generic tests (e.g., `unique`, `not_null`, `relationships`) to each model.

---

## 🔧 Using dbt vars

Models can accept parameters through dbt's `var()` function, so the same SQL file can produce different results without editing code.

Example (as a scratch test under `analyses/`, not part of any production model):

```jinja
{% set min_nights = var('min_nights', 1) %}
select *
from {{ ref('stg_bookings') }}
where nights_booked >= {{ min_nights }}
```

- If no value is passed, `min_nights` falls back to the default `1`.
- To override it for a single run:

```bash
dbt run --select stg_bookings --vars '{"min_nights": 3}'
```

Precedence (highest to lowest):

| Source | Example |
|---|---|
| CLI `--vars` | `dbt run --vars '{"min_nights": 3}'` |
| `dbt_project.yml` | `vars: { min_nights: 2 }` |
| Model default | `var('min_nights', 1)` |

---

## 📦 Package Dependencies

| Package | Version |
|---------|---------|
| [dbt-labs/dbt_utils](https://github.com/dbt-labs/dbt-utils) | `1.3.3` |

---

## ⚠️ Notes & Deviations from the Reference Tutorial

This project was built while following [Ansh's AWS + Snowflake + dbt tutorial](https://github.com/anshumanmahapatra/aws_snowflake_dbt) (adapted here to run on Databricks). During implementation, three issues in the reference video were identified and handled differently in this project:

### 1. Bookings modeled as a dimension, causing duplicate `booking_id`

In the reference tutorial (~5h20m mark), the bookings table is modeled as `dim_bookings` — i.e., treated as a **dimension table**. However, each row represents a discrete booking transaction, which makes bookings a **fact table** by nature (transactional grain, one row per business event). Modeling it as a dimension led to duplicate `booking_id` values downstream in the reference project.

In this project, bookings are consistently modeled as a fact table (`stg_bookings` → `silver_bookings` → `gold_fact_bookings`), with `booking_id` enforced as the unique/incremental key at every layer.

### 2. Incremental/snapshot design gaps: `created_at` as the cursor, and no dedup before `MERGE`

The reference tutorial's incremental models — correctly gated with dbt's
built-in `is_incremental()` — still have two gaps that only surface once the
underlying data isn't as clean as the tutorial's own dataset:

```sql
{{ config(materialized='incremental') }}

SELECT * FROM {{ source('staging', 'hosts') }}

{% if is_incremental() %}
    WHERE CREATED_AT > (SELECT COALESCE(MAX(CREATED_AT), '1900-01-01') FROM {{ this }})
{% endif %}
```

**`CREATED_AT` as the incremental cursor.** `created_at` is a **write-once**
field — it's set when a row is first inserted and never changes. Using it
as the cursor for `WHERE`, or as the `updated_at` column in a snapshot's
`timestamp` strategy (the tutorial does both), means dbt can only ever
detect *new* rows, never *updated* ones — the value it's comparing against
simply never moves for a row that already exists. This project uses
`ingested_at` (stamped at ingestion time, refreshed on every reload) as the
incremental cursor in staging, and `strategy: check` (comparing column
values directly, not a timestamp) for snapshots — more robust when the
source doesn't provide a trustworthy `updated_at`.

**No dedup before `MERGE`.** Neither this model nor its siblings include a
`qualify row_number() = 1` (or equivalent) before the final `SELECT`.
`MERGE` requires the incoming batch to contain at most one row per key — if
the same key appears twice in one incremental window, the `MERGE` fails
outright. The tutorial's own dataset apparently never triggers this, which
only shows the dataset doesn't happen to contain a within-batch duplicate —
not that the model is safe against one. Every incremental model in this
project ends with an explicit
`qualify row_number() over (partition by <primary key> order by ingested_at desc) = 1`
for this reason.

### 3. Ambiguity in `booking_amount` vs. `total_amount`

In `silver_bookings.sql`:

```jinja
{{ multiply_and_round('nights_booked', 'booking_amount', 2) }} as total_amount
```

This calculation assumes `booking_amount` represents a **per-night rate**, and derives `total_amount = nights_booked * booking_amount`.

However, the source data definition does not make it clear whether `booking_amount` is already the **total price for the booking** (i.e., already aggregated across `nights_booked`) or a per-night rate similar to `price_per_night` in the listings table. If `booking_amount` is already a total, then multiplying it by `nights_booked` again would double-count the duration and significantly overstate `total_amount` / revenue.

The current implementation assumes `booking_amount` is a per-night rate, consistent with the naming pattern of `price_per_night`. If it is instead the already-aggregated total for the booking, `total_amount` would overstate revenue by a factor of `nights_booked`. Downstream consumers of `total_amount` should treat this assumption as unverified until the source schema is confirmed.

---

## 📚 References

- [dbt Incremental models in-depth](https://docs.getdbt.com/best-practices/materializations/4-incremental-models?version=2)
- [Reference tutorial](https://www.youtube.com/watch?v=3SZSDKEZaoA&t=19255s)
```
