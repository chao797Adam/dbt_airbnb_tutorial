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
│   │   ├── properties.yml      # Tests / descriptions for silver models
│   │   ├── silver_bookings.sql
│   │   ├── silver_hosts.sql
│   │   └── silver_listings.sql
│   └── mart/                # Gold layer
│       ├── properties.yml      # Tests / descriptions for mart models
│       ├── gold_fact_bookings.sql
│       └── obt.sql              # One Big Table
├── snapshots/               # SCD2 historical snapshots
│   ├── dim_hosts_snapshot.sql
│   └── dim_listings_snapshot.sql
├── tests/                    # Custom data tests
├── dbt_project.yml
└── packages.yml
```

## 🧪 Incremental Load Test Data

`dataset_incre_load/listing_incre2.csv` is a small sample file used to verify the incremental load behavior end-to-end. It contains two listings with a `created_at` timestamp that will be picked up by Auto Loader as new data.

To reproduce the incremental test:

1. Upload `listing_incre2.csv` into the source volume under a **new filename** (Auto Loader tracks files by path, not by content — reusing an existing filename will not be re-read).
2. Run the Auto Loader ingestion notebook with `table_name = listings`.
3. Run `dbt run --select stg_listings+` and `dbt snapshot`.
4. Verify:
   - `bronze.stg_listings` gains the two new rows with a fresh `ingested_at`.
   - `snapshots.listings_snapshot` records a new historical version for each changed `listing_id`.
   - `gold.obt` keeps `booking_id` unique.

---

## 🗄️ Source

Source tables are defined in `models/staging/sources.yml`:

| Source | Catalog | Schema | Tables |
|--------|---------|--------|--------|
| `airbnb_source` | `dbt_airbnb` | `source` | `listings`, `bookings`, `hosts` |

Raw tables are written by an Auto Loader notebook into `dbt_airbnb.source.{table_name}`. Checkpoints and schema inference files live in a Unity Catalog Volume under `/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/`.

---

## 📥 Bronze Ingestion (Auto Loader Notebook)

Raw CSV files are loaded into the `source` schema via an Auto Loader notebook. The same core logic is used for manual executions and automated workflows.

### Method 1: Manual Execution (Development)

For local development or ad-hoc loads, use a single notebook. The notebook is parameterized with a widget, allowing you to manually select the target table before running.

#### Step 1 — Define and read the widget

```python
# Define the widget — appears as a parameter at the top of the notebook
dbutils.widgets.text("table_name", "", "Table Name")

# Read the widget value at runtime
table_name = dbutils.widgets.get("table_name").strip()

print(f"=== Running Bronze ingestion stream for table: {table_name} ===")
```

#### Step 2 — Execution (Read, Write, and Verify)

```python
from pyspark.sql.functions import current_timestamp, col

# 1. Auto Loader read stream (Input from source_data)
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", f"/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/schema") \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .load(f"/Volumes/dbt_airbnb/source/source_data/{table_name}/")

# 2. Add audit columns
df = df.withColumn("ingested_at", current_timestamp()) \
       .withColumn("source_file", col("_metadata.file_path"))

# 3. Delta write stream (Output to source schema, checkpoint to bronze volume)
query = df.writeStream.format("delta") \
    .outputMode("append") \
    .trigger(once=True) \
    .option("checkpointLocation", f"/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/checkpoint") \
    .toTable(f"dbt_airbnb.source.{table_name}")

query.awaitTermination()

# 4. Verify Metrics
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

---

### Method 2: Automated Lakeflow Job (Production)

In production, the notebook is not run manually per table. It is orchestrated as a two-step **Databricks Workflow (Lakeflow Job)** using a **For each** task. This provides concurrent execution and isolated failure handling.

#### Task 1 Notebook: `get_tables` (Publish Table List)

A minimal notebook outputs the list of tables to ingest, which the downstream `For each` loop will consume.

```python
# Define the tables to process
tables = ["bookings", "listings", "hosts"]

# Publish to task values for the For each loop
dbutils.jobs.taskValues.set(key="tables", value=tables)
```

#### Task 2 Notebook: `ingest_one` (Parameterized Ingestion)

This notebook uses the same core logic as Method 1. However, the `table_name` is injected dynamically by the `For each` loop. *(Note: `trigger(availableNow=True)` is used here for modern workflow orchestration)*.

```python
from pyspark.sql.functions import current_timestamp, col

# Define the widget to receive the loop input
dbutils.widgets.text("table_name", "", "Table Name")
table_name = dbutils.widgets.get("table_name").strip()

print(f"=== Running Bronze ingestion stream for table: {table_name} ===")

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", f"/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/schema") \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .load(f"/Volumes/dbt_airbnb/source/source_data/{table_name}/")

df = df.withColumn("ingested_at", current_timestamp()) \
       .withColumn("source_file", col("_metadata.file_path"))

query = df.writeStream.format("delta") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", f"/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/checkpoint") \
    .toTable(f"dbt_airbnb.source.{table_name}")

query.awaitTermination()

prog = query.lastProgress
print(f"✅ {table_name}: batchId={prog.get('batchId') if prog else None}")
```

#### Workflow Configuration

1. **Task 1:** Add a Notebook task pointing to `get_tables`.
2. **Task 2:** Add a **For each** task.
   - **Depends on:** `Task 1`
   - **Inputs:** `{{tasks.get_tables.values.tables}}`
   - **Nested Task:** Point to the `ingest_one` notebook.
   - **Parameters:** Key = `table_name`, Value = `{{input}}`
   - **Concurrency:** Set to `3` to run all tables in parallel.

This keeps the ingestion notebook fully table-agnostic — it only knows the single `table_name` it receives — while the Workflow handles fan-out and concurrency. Adding a new source table is just a matter of appending its name to `tables` in Task 1.

Checkpoints and schema files are stored under `/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/`.

## 🧱 Models

### Staging (Bronze)

*(Note: The reference tutorial uses Snowflake, which relies on `CREATE STAGE`, `FILE FORMAT`, and `COPY INTO` to batch-load data. This project adapts the pipeline to Databricks, replacing those steps with **Auto Loader (cloudFiles)**.)*

In the reference architecture, raw data resides in AWS S3 and is loaded into Snowflake via an IAM role integration. Because the dataset for this project is relatively small, we simplify the infrastructure by using the **Databricks Unity Catalog Volumes** as the direct landing zone, bypassing the need for external cloud storage configurations.

With this Databricks implementation:
- **Auto Loader** continuously and incrementally tracks new files in the Volume.
- The `cloudFiles.schemaEvolutionMode = "rescue"` option acts as error handling. Instead of failing the load on a malformed row, Auto Loader captures mismatched or unparseable columns into a `_rescued_data` field.
- By default, Auto Loader ingests all CSV columns as `string`. Therefore, the `stg_*` dbt models must explicitly cast these string fields into their proper data types.

Upon initial ingestion by Auto Loader, **every business column in the `source` schema lands as a `string`**, alongside metadata columns (`_rescued_data`, `ingested_at`, `source_file`).

You can inspect this raw state dynamically using a quick loop:

```python
tables = ["listings", "bookings", "hosts"]

for t in tables:
    print(f"\n{'='*20} {t} Schema {'='*20}")
    display(spark.sql(f"DESCRIBE dbt_airbnb.source.{t}"))
```

*(Expected output: all data fields shown as `string`, alongside `ingested_at` and `source_file`.)*

The `stg_*` models are responsible for transforming this raw landing data into a clean Bronze layer by:
1. **Casting**: Converting all string columns into their proper types (`int`, `double`, `date`, `timestamp`).
2. **Standardizing**: Excluding the Auto Loader metadata (like `_rescued_data`) and retaining only the required business columns and audit fields.
3. **Deduplicating**: Using `qualify row_number() over (partition by <primary_key> order by ingested_at desc) = 1` to keep only the most recent version of each record.
4. **Incremental Loading**: Filtering on `ingested_at` to process only new data. Each model deduplicates by its primary key using the aforementioned `qualify` clause.

| Model | Primary Key | Description |
|-------|-------------|-------------|
| `stg_listings` | `listing_id` | Listing details (property type, city, price, etc.) |
| `stg_hosts` | `host_id` | Host info (signup date, superhost flag, response rate) |
| `stg_bookings` | `booking_id` | Booking records (nights booked, amounts, status) |

**Incremental logic:** Each staging model filters on `ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})` when running in incremental mode.

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

The join chain follows the Airbnb entity model: `host (1) → listing (many) → booking (many)`. Each hop is many-to-one, so the resulting grain stays at one row per booking.

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

Snapshots read from `silver_hosts` and `silver_listings`, both of which are deduplicated at the silver layer using `qualify row_number() over (partition by <primary key> order by ingested_at desc) = 1`. This guarantees that the snapshot source has exactly one row per key — a requirement for `strategy: check` snapshots, since duplicate keys in the source would silently produce spurious historical versions rather than raising an error.

Snapshotting directly from `source` was considered and rejected, for two
independent reasons:

1. **Schema.** Every column in `source` is a raw `string` (see
   [Data Layer Architecture](#-data-layer-architecture)) — `response_rate`
   is a string, not a number, and derived fields like `response_rate_tag`
   don't exist yet at this layer. `check_cols` would be comparing the wrong
   representation of the data, or comparing against a column that isn't
   there.
2. **Uniqueness.** `source` permits duplicate `host_id`/`listing_id` within
   a batch — Auto Loader does no deduplication (see
   [Bronze Ingestion](#-bronze-ingestion-auto-loader-notebook)). A snapshot
   source needs exactly one row per key; duplicates here don't error, they
   silently expand into spurious historical versions (see
   [Snapshot source chosen for key uniqueness](#4-snapshot-source-chosen-for-key-uniqueness)).

Both problems are already solved by the time data reaches `silver_*` — cast
to proper types, business fields derived, and deduplicated via
`qualify row_number() = 1` — which is why snapshots read from there instead.

---

## ✅ Data Tests

Generic tests are declared at every layer, all keyed on the natural primary key of each model:

| Layer | Model | Column | Tests |
|-------|-------|--------|-------|
| Staging | `stg_bookings` | `booking_id` | `not_null`, `unique` |
| Staging | `stg_hosts` | `host_id` | `not_null`, `unique` |
| Staging | `stg_listings` | `listing_id` | `not_null`, `unique` |
| Intermediate | `silver_bookings` | `booking_id` | `not_null`, `unique` |
| Intermediate | `silver_hosts` | `host_id` | `not_null`, `unique` |
| Intermediate | `silver_listings` | `listing_id` | `not_null`, `unique` |
| Mart | `gold_fact_bookings` | `booking_id` | `not_null`, `unique` |
| Mart | `obt` | `booking_id` | `not_null`, `unique` |

Key-uniqueness tests are especially important at the mart layer. `gold_fact_bookings` and `obt` are built from joins, and any join that is not strictly many-to-one will silently multiply rows — the `unique` test catches this immediately.

The `dbt_utils` package (declared in `packages.yml`, version `1.3.3`) is also available and can be used to add generic tests (e.g., `unique`, `not_null`, `relationships`) to each model.

### 🛡️ Proof of No Fan-Out (Row Count Consistency)

Data tests guarantee the logical uniqueness of primary keys. Beyond that, row count consistency across layers is the physical proof that no **Fan-out (row multiplication)** occurs during joins.

| Layer | `bookings` | `listings` | `hosts` |
|-------|------------|------------|---------|
| Source (Raw) | 5000 | 500 | 200 |
| Bronze (Staging) | 5000 | 500 | 200 |
| Silver (Intermediate) | 5000 | 500 | 200 |
| Gold (`obt`, `fct_bookings`) | 5000 | — | — |

*(Note: The row counts remain perfectly flat from the source to the Gold layer. This proves that our `qualify row_number() = 1` deduplication and layered `unique` tests successfully prevent the join explosion.)*

**Why this matters:** If we followed the reference tutorial's approach (no deduplication before the `obt` join), the `Gold` layer row count for `bookings` would exceed 5000 due to duplicate `listing_id` rows in the source matching multiple times. Thanks to our strict deduplication at the Silver layer, the `left join` strictly operates on a many-to-one basis, keeping the final table exactly at 5000 rows.

**Reproducible Verification**

You can verify the flat row counts across all layers by running the following query:

```sql
SELECT 'bookings' AS table_name, COUNT(*) AS total_rows FROM dbt_airbnb.source.bookings
UNION ALL
SELECT 'listings' AS table_name, COUNT(*) AS total_rows FROM dbt_airbnb.source.listings
UNION ALL
SELECT 'hosts' AS table_name, COUNT(*) AS total_rows FROM dbt_airbnb.source.hosts
UNION ALL
SELECT 'stg_bookings' AS table_name, COUNT(*) AS total_rows FROM dbt_airbnb.bronze.stg_bookings
UNION ALL
SELECT 'stg_listings' AS table_name, COUNT(*) AS total_rows FROM dbt_airbnb.bronze.stg_listings
UNION ALL
SELECT 'stg_hosts' AS table_name, COUNT(*) AS total_rows FROM dbt_airbnb.bronze.stg_hosts
UNION ALL 
SELECT 'silver_bookings' AS table_name, COUNT(*) AS total_rows FROM dbt_airbnb.silver.silver_bookings
UNION ALL
SELECT 'silver_listings' AS table_name, COUNT(*) AS total_rows FROM dbt_airbnb.silver.silver_listings
UNION ALL
SELECT 'silver_hosts' AS table_name, COUNT(*) AS total_rows FROM dbt_airbnb.silver.silver_hosts
UNION ALL
SELECT 'obt' AS table_name, COUNT(*) AS total_rows FROM dbt_airbnb.gold.obt
UNION ALL
SELECT 'fct_bookings' AS table_name, COUNT(*) AS total_rows FROM dbt_airbnb.gold.gold_fact_bookings;
```

If the pipeline is built correctly, the row count for `bookings` should remain exactly **5000** across `source` → `stg_bookings` → `silver_bookings` → `obt` → `gold_fact_bookings`. Any deviation indicates a Fan-out issue.

## 🔧 Using dbt vars

Models can accept parameters through dbt's `var()` function, allowing the same SQL file to produce different results without editing the code.

For example, to dynamically filter bookings at runtime, we can use a variable in a model (e.g., in `stg_bookings.sql`):

```jinja
select *
from {{ ref('stg_bookings') }}
where nights_booked >= {{ var('min_nights', 1) }}
```

- If no value is passed, `min_nights` falls back to the default `1`.
- To override it for a single run, pass the variable via the command line:

```bash
dbt run --select stg_bookings --vars '{"min_nights": 3}'
```

**Precedence (highest to lowest):**

| Source | Example |
|---|---|
| CLI `--vars` | `dbt run --vars '{"min_nights": 3}'` |
| `dbt_project.yml` | `vars: { min_nights: 2 }` |
| Model default | `var('min_nights', 1)` |

## 📦 Package Dependencies

| Package | Version |
|---------|---------|
| [dbt-labs/dbt_utils](https://github.com/dbt-labs/dbt-utils) | `1.3.3` |

---

## ⚠️ Notes & Deviations from the Reference Tutorial

This project was built while following [Ansh's AWS + Snowflake + dbt tutorial](https://github.com/anshumanmahapatra/aws_snowflake_dbt) (adapted here to run on Databricks). During implementation, four issues in the reference video were identified and handled differently in this project:

### 1. Bookings modeled as a dimension, causing duplicate `booking_id`

In the reference tutorial (~5h20m mark), the bookings table is modeled as `dim_bookings` — i.e., treated as a **dimension table**. However, each row represents a discrete booking transaction, which makes bookings a **fact table** by nature (transactional grain, one row per business event). Modeling it as a dimension led to duplicate `booking_id` values downstream in the reference project.

In this project, bookings are consistently modeled as a fact table (`stg_bookings` → `silver_bookings` → `gold_fact_bookings`), with `booking_id` enforced as the unique/incremental key at every layer.

### 2. Incremental/snapshot design gaps: `created_at` as the cursor, and no dedup before `MERGE`

The reference tutorial's incremental models use `is_incremental()` correctly, but two design gaps remain. Both only surface once the underlying data stops being as clean as the tutorial's own dataset:

```sql
{{ config(materialized='incremental') }}

SELECT * FROM {{ source('staging', 'hosts') }}

{% if is_incremental() %}
    WHERE CREATED_AT > (SELECT COALESCE(MAX(CREATED_AT), '1900-01-01') FROM {{ this }})
{% endif %}
```

**`CREATED_AT` as the incremental cursor.** `created_at` is a **write-once** field — it's set when a row is first inserted and never changes. Using it as the cursor for `WHERE`, or as the `updated_at` column in a snapshot's `timestamp` strategy (the tutorial does both), means dbt can only ever detect *new* rows, never *updated* ones — the value it's comparing against simply never moves for a row that already exists. 

**Trade-off: Using `ingested_at` due to source limitations.** The raw CSV data does not provide a reliable `updated_at` column. Therefore, this project uses `ingested_at` (stamped at ingestion time, refreshed on every reload) as the incremental cursor in staging. This is a pragmatic compromise: while it may miss late-arriving updates (if the upstream system doesn't re-drop the file), it successfully captures updated rows that `created_at` would miss. We combine this with `strategy: check` (comparing column values directly, not a timestamp) for snapshots — which is far more robust when the source lacks a trustworthy timestamp.

**No dedup before `MERGE`.** Neither this model nor its siblings include a `qualify row_number() = 1` (or equivalent) before the final `SELECT`. `MERGE` requires the incoming batch to contain at most one row per key — if the same key appears twice in one incremental window, the `MERGE` fails outright. The tutorial's own dataset apparently never triggers this, which only shows the dataset doesn't happen to contain a within-batch duplicate — not that the model is safe against one. Every incremental model in this project ends with an explicit `qualify row_number() over (partition by <primary key> order by ingested_at desc) = 1` for this reason.

**And no tests to catch it.** The reference tutorial's `obt` model and its other marts do not include a `unique` test on `booking_id`. Without dedup and without a test, duplicate rows stay invisible — the pipeline runs green, and the error only surfaces when downstream aggregations produce inflated numbers.

This project adds a `unique` + `not_null` test on the primary key of every staging, intermediate, and mart model. Running `select booking_id, count(*) from obt group by 1 having count(*) > 1` against the reference tutorial's `obt` returns rows; against this project's `obt`, it returns zero rows. The test is what makes the guarantee explicit and regression-proof.

### 3. Ambiguity in `booking_amount` vs. `total_amount`

In `silver_bookings.sql`:

```jinja
{{ multiply_and_round('nights_booked', 'booking_amount', 2) }} as total_amount
```

This calculation assumes `booking_amount` represents a **per-night rate**, and derives `total_amount = nights_booked * booking_amount`.

However, the source data definition does not make it clear whether `booking_amount` is already the **total price for the booking** (i.e., already aggregated across `nights_booked`) or a per-night rate similar to `price_per_night` in the listings table. If `booking_amount` is already a total, then multiplying it by `nights_booked` again would double-count the duration and significantly overstate `total_amount` / revenue.

The current implementation assumes `booking_amount` is a per-night rate, consistent with the naming pattern of `price_per_night`. If it is instead the already-aggregated total for the booking, `total_amount` would overstate revenue by a factor of `nights_booked`. Downstream consumers of `total_amount` should treat this assumption as unverified until the source schema is confirmed.

### 4. Snapshot source chosen for key uniqueness

The reference tutorial snapshots from `obt`, a wide join across bookings, listings, and hosts. The join chain itself is many-to-one and would not fan out on its own — but the tutorial does not dedupe the source tables before joining, so `obt` ends up with multiple rows per `booking_id`. A snapshot source must have exactly one row per `unique_key`; duplicate keys do not raise an error, they silently expand into spurious historical versions.

This project snapshots from `silver_hosts` and `silver_listings`, both of which are deduplicated at the silver layer and therefore guaranteed key-unique at the time of snapshot.

---

## 🧬 DLT Re-implementation

A parallel implementation of the same pipeline using **Databricks Lakeflow Declarative Pipelines (DLT)** lives in `dlt_airbnb_pipeline/`. It reads from the same `dbt_airbnb.source.*` tables, so the two pipelines can be compared on identical input.

| Layer | dbt | DLT |
|---|---|---|
| Bronze | `stg_*` (dedup + cast) | `bronze_*` (cast only, no dedup) |
| Silver | `silver_*` (dedup + business logic) | `silver_*` (CDC via `create_auto_cdc_flow`) |
| Gold | `gold_*`, `obt`, snapshots | `dim_*`, `gold_*`, `obt` |

**Key difference:** dbt deduplicates at both staging and silver layers — every physical table is guaranteed key-unique before it feeds the next layer. DLT deduplicates only at the silver layer via `create_auto_cdc_flow`, which merges the bronze event stream into a key-unique current state. Both produce the same result on the same source data; the difference is where the uniqueness guarantee lives.

Data quality expectations are declared with `@dlt.expect` in DLT and `dbt test` in dbt. Note that `@dlt.expect` is row-level only — it cannot express global uniqueness (`unique` in dbt). In DLT, uniqueness is enforced structurally by the CDC flow rather than verified by a test.

## 📚 References

- [dbt Incremental models in-depth](https://docs.getdbt.com/best-practices/materializations/4-incremental-models?version=2)
- [Reference tutorial](https://www.youtube.com/watch?v=3SZSDKEZaoA&t=19255s)
