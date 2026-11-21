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

## 🧪 Incremental Load & SCD2 Verification

To prove that the pipeline correctly handles incremental loads, deduplication, and SCD Type 2 history tracking, we use a sample test dataset.

### Test Data
We uploaded a new CSV file containing two rows to the source volume:
```csv
listing_id,host_id,property_type,room_type,city,country,accommodates,bedrooms,bathrooms,price_per_night,created_at
1,150,House,Entire home,Melbourne,Australia,3,1,2,84,2025-12-26 14:15:54.011160
501,91,Apartment,Private room,Sydney,Australia,1,2,3,140,2025-12-26 14:15:54.011160
```

*(Note: Auto Loader tracks files by path, so this must be uploaded under a **new filename** to be picked up as new data).*

### Verification Steps

1. **Trigger Auto Loader:** Run the Databricks Workflow Job to ingest the new CSV. The `source.listings` table will gain these 2 new rows.
2. **Run dbt models:** Execute `dbt run --select stg_listings+`. This will incrementally process the new data through Bronze, Silver, and Gold layers.
3. **Run dbt Snapshot:** Execute `dbt snapshot` to update the SCD Type 2 history.

### Expected Results

By running a dynamic loop in Databricks to inspect the layers:
```python
tables = ["bronze.stg_listings", "silver.silver_listings", "snapshots.listings_snapshot"]
for t in tables:
    display(spark.sql(f"SELECT * FROM dbt_airbnb.{t} WHERE listing_id IN (1, 501)"))
```

- **Silver Layer (`silver_listings`)**: The record for `listing_id = 1` remains strictly **1 row**, updated with the latest data (e.g., `city = Melbourne`). The `qualify row_number() = 1` logic successfully prevented duplicates.
- **Snapshot Table (`listings_snapshot`)**: The record for `listing_id = 1` now shows **2 rows** (SCD2):
  - **Old Version:** The previous state (e.g., `city = West Lisafort`) with a populated `dbt_valid_to` timestamp.
  - **New Version:** The updated state (`city = Melbourne`) with `dbt_valid_to = NULL` (current active record).

This verifies that our pipeline can seamlessly ingest new data, maintain strict primary key uniqueness, and accurately track historical changes.

---

## 🗄️ Source

Source tables are defined in `models/staging/sources.yml`:

| Source | Catalog | Schema | Tables |
|--------|---------|--------|--------|
| `airbnb_source` | `dbt_airbnb` | `source` | `listings`, `bookings`, `hosts` |

Raw tables are written by an Auto Loader notebook into `dbt_airbnb.source.{table_name}`. Checkpoints and schema inference files live in a Unity Catalog Volume under `/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/`.

---

## 📥 Bronze Ingestion (Auto Loader + Lakeflow Job)

Raw CSV files are incrementally ingested into Delta tables in the `dbt_airbnb.source` schema using Databricks Auto Loader.

The ingestion process is orchestrated through a **Lakeflow Job** consisting of an upstream Notebook task and a downstream **For each** task. Each source table is processed independently using the same parameterized ingestion notebook.

### 1. Task 1 — `datalist` (Notebook: `get_tables`)

The first notebook defines the source tables and publishes the list using Databricks Task Values.

```python
tables = ["bookings", "listings", "hosts"]

dbutils.jobs.taskValues.set(
    key="tables",
    value=tables
)
```

- `tables` is the Python variable containing the list of source tables.
- `key="tables"` defines the Task Value key used by downstream tasks.
- `value=tables` stores the actual Python list under that key.

The published list can then be retrieved by the downstream For each task.

### 2. Task 2 — `ingest` (For each)

The second task is configured as a **For each** task that iterates over the table list published by `datalist`.

**Workflow Configuration**

| Setting | Value |
|---|---|
| Task Key | `ingest` |
| Task Type | For each |
| Depends on | `datalist` |
| Inputs | `{{tasks.datalist.values.tables}}` |
| Nested Task | `ingest_iteration` |
| Nested Notebook | `ingest_one` |
| Parameter Key | `table_name` |
| Parameter Value | `{{input}}` |
| Concurrency | `3` |

The expression `{{tasks.datalist.values.tables}}` retrieves the entire list published by Task 1.

The For each task iterates over the list, and `{{input}}` represents the current table name during each iteration.

With concurrency set to `3`, up to three ingestion iterations can run in parallel, subject to available compute resources.

### 3. Auto Loader Notebook — `ingest_one`

The `ingest_one` notebook receives the current table name from the For each task and dynamically constructs the input, output, schema, and checkpoint paths.

The same notebook processes `bookings`, `listings`, and `hosts` without requiring separate ingestion code for each table.

```python
from pyspark.sql.functions import current_timestamp, col

# 1. Receive the current table name
dbutils.widgets.text("table_name", "", "Table Name")
table_name = dbutils.widgets.get("table_name").strip()

print(f"=== Running Bronze ingestion stream for table: {table_name} ===")

# 2. Read source CSV files using Auto Loader
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", f"/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/schema") \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .load(f"/Volumes/dbt_airbnb/source/source_data/{table_name}/")

# 3. Add ingestion audit columns
df = df.withColumn("ingested_at", current_timestamp()) \
       .withColumn("source_file", col("_metadata.file_path"))

# 4. Write streaming data to Delta table
query = df.writeStream.format("delta") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", f"/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/checkpoint") \
    .toTable(f"dbt_airbnb.source.{table_name}")

query.awaitTermination()

# 5. Verify ingestion progress
prog = query.lastProgress

print(f"✅ {table_name}: batchId={prog.get('batchId') if prog else None}")
```

**Key Ingestion Features**

- **Auto Loader:** Incrementally processes new CSV files from the source volume.
- **Schema Evolution:** Uses `rescue` mode to capture unexpected or incompatible fields in `_rescued_data`.
- **Audit Columns:** Adds `ingested_at` (processing timestamp) and `source_file` (origin file path) for traceability.
- **Checkpointing:** Maintains a separate checkpoint directory for each table to track ingestion progress and avoid reprocessing previously committed files.
- **Trigger:** `availableNow=True` processes currently available data and then terminates the stream.
- **Output:** Writes Delta tables to `dbt_airbnb.source.{table_name}`.

### 4. Workflow Parameter Passing

The workflow uses **Databricks Task Values, For each inputs, and Notebook Widgets** to pass table names between tasks.

**Step 1 — Publish the List**

The `datalist` task publishes:

```python
tables = ["bookings", "listings", "hosts"]

dbutils.jobs.taskValues.set(
    key="tables",
    value=tables
)
```

Here, `key="tables"` is the name used to retrieve the published list. It does not have to match the Python variable name.

**Step 2 — Retrieve the List**

The For each task reads the published list using:

```text
{{tasks.datalist.values.tables}}
```

- `datalist` identifies the upstream Task Key.
- `values` accesses the published Task Values.
- `tables` matches the key defined in `taskValues.set()`.

**Step 3 — Iterate Over the List**

For each iterates over the list, with `{{input}}` representing the current element.

| Iteration | `{{input}}` | Notebook Parameter |
|---|---|---|
| 1 | `bookings` | `table_name=bookings` |
| 2 | `listings` | `table_name=listings` |
| 3 | `hosts` | `table_name=hosts` |

**Step 4 — Receive the Parameter**

The nested task passes the current value through the `table_name` parameter.

The `ingest_one` notebook reads it using:

```python
table_name = dbutils.widgets.get("table_name").strip()
```

Conceptually, the workflow behaves like:

```python
for table_name in ["bookings", "listings", "hosts"]:
    ingest_one(table_name)
```

Unlike a sequential Python loop, Databricks For each can execute multiple iterations concurrently.

### 5. Storage and Checkpoint Structure

Each source table has its own input directory, Auto Loader schema directory, checkpoint directory, and Delta output table.

| Component | Path / Table |
|---|---|
| Source CSV | `/Volumes/dbt_airbnb/source/source_data/{table_name}/` |
| Schema Location | `/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/schema` |
| Checkpoint Location | `/Volumes/dbt_airbnb/bronze/bronzevolume/{table_name}/checkpoint` |
| Delta Output | `dbt_airbnb.source.{table_name}` |

This design keeps the ingestion notebook **table-agnostic**, while the Lakeflow Job manages task dependencies, iteration, and concurrency.

Adding another source table only requires adding its name to the list in `get_tables`, provided the corresponding source files are available and compatible with the ingestion logic.

### Architecture Comparison: AWS + Snowflake vs. Databricks

The reference tutorial implements data ingestion using **AWS S3 + Snowflake**, while this project reimplements the same ingestion requirements using **Databricks Unity Catalog Volumes + Auto Loader + Lakeflow Jobs**.

This provides a practical comparison of two approaches to ingesting source files before dbt transformations.

| Component | Reference: AWS + Snowflake | This Project: Databricks |
|---|---|---|
| Raw File Storage | AWS S3 | Unity Catalog Volumes |
| Storage Access | IAM Role + Storage Integration | Direct access through UC Volumes |
| File Configuration | Snowflake Stage + File Format | Auto Loader `cloudFiles` options |
| Schema Handling | Predefined target tables | Schema inference with `rescue` mode |
| Data Ingestion | `COPY INTO` | `readStream` + `writeStream` |
| Incremental File Tracking | Snowflake load metadata | Auto Loader checkpoints |
| Orchestration | Not explicitly implemented in the reference ingestion steps | Lakeflow Jobs + For each |
| Downstream Transformation | dbt on Snowflake | dbt on Databricks |

**Key Observations**

**1. Initial Setup Complexity**

The reference implementation requires configuring AWS IAM permissions, Snowflake storage integration, stages, file formats, and target tables before executing `COPY INTO`.

In this Databricks implementation, source CSV files are stored directly in Unity Catalog Volumes. Auto Loader handles schema inference and incremental file discovery, reducing the external integration setup required for this small project.

**2. Reusable and Parameterized Ingestion**

Instead of creating separate ingestion logic for each source table, this project uses one parameterized Auto Loader notebook.

Lakeflow Jobs dynamically retrieves the table list through Task Values and executes the notebook using a For each task, allowing up to three concurrent ingestion iterations.

**3. Separation of Responsibilities**

The Databricks implementation separates ingestion from orchestration:

- **Auto Loader:** Reads new source files and writes Delta tables.
- **Lakeflow Jobs:** Manages task dependencies, parameter passing, and concurrent execution.
- **dbt:** Performs Bronze, Silver, and Gold transformations.

**Conclusion**

For this small, multi-table project, Unity Catalog Volumes combined with Auto Loader and Lakeflow Jobs provides a convenient and reusable ingestion architecture.

This comparison reflects the specific implementations used in the reference tutorial and this project, rather than a general claim that Databricks is superior to Snowflake. Snowflake also supports automated ingestion and schema inference through additional features such as Snowpipe.

The key learning outcome is the ability to implement the same business data pipeline across different cloud data platforms while understanding their ingestion patterns, configuration requirements, and orchestration responsibilities.

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

Snapshots read from `silver_hosts` and `silver_listings`, both of which are deduplicated at the silver layer using `qualify row_number() over (partition by <primary_key> order by ingested_at desc) = 1`. This ensures that the snapshot source is key-unique when the Silver incremental models complete successfully and the uniqueness tests pass — a requirement for reliable `strategy: check` snapshots, since duplicate source keys can lead to incorrect or ambiguous historical records.

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
   [Bronze Ingestion](#-bronze-ingestion-auto-loader--lakeflow-job)). A snapshot
   source needs exactly one row per key; duplicate source keys can lead to incorrect or ambiguous snapshot history. (see
   [Snapshot source chosen for key uniqueness](#4-snapshot-source-chosen-for-key-uniqueness)).

Both problems are already solved by the time data reaches `silver_*` — cast
to proper types, business fields derived, and deduplicated via
`qualify row_number() = 1` — which is why snapshots read from there instead.

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

### 🚨 Singular Test (Warn Severity)

In addition to 16 generic `unique` and `not_null` tests, I implemented a custom singular test (`tests/source_test.sql`) to demonstrate business-level anomaly monitoring.

Most bookings are assumed to have an amount of $200 or more. The test flags bookings below this illustrative threshold as potential anomalies. Since low-value bookings may still be legitimate, I configured `severity='warn'` rather than `error`, allowing the pipeline to continue while highlighting records for further investigation.

**Terminal output:**
```text
WARN 286 source_test
Got 286 results, configured to warn if != 0
Done. PASS=0 WARN=1 ERROR=0
```

**Result:** 286 bookings were flagged for review, not classified as confirmed data errors. The $200 threshold is an illustrative assumption that would require business validation in a production environment.

### 🛡️ Proof of No Fan-Out (Row Count Consistency)

Data tests guarantee the logical uniqueness of primary keys. Beyond that, row count consistency across layers is the physical proof that no **Fan-out (row multiplication)** occurs during joins.

| Layer | `bookings` | `listings` | `hosts` |
|-------|------------|------------|---------|
| Source (Raw) | 5000 | 500 | 200 |
| Bronze (Staging) | 5000 | 500 | 200 |
| Silver (Intermediate) | 5000 | 500 | 200 |
| Gold (`obt`, `fct_bookings`) | 5000 | — | — |

*(Note: The row counts remain flat from the source to the Gold layer, which shows that no fan-out occurs. The `qualify row_number() = 1` deduplication and the layered `unique` tests are what guarantee this if the source later contains duplicates, as in the 2-row test file above.)*

**Why this matters:** In the reference video, the `FACT` table shows repeated `booking_id` rows, so its row count exceeds the number of distinct bookings (see Note 1). In this project, deduplication at the Silver layer keeps every join many-to-one, and the final table stays at exactly 5000 rows.

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

For the baseline dataset containing 5,000 unique bookings, the expected row count is exactly **5,000** across `source` → `stg_bookings` → `silver_bookings` → `obt` → `gold_fact_bookings`.

Since the Gold models join bookings with listings and hosts, maintaining the same booking row count provides evidence that these joins do not introduce fan-out. This verification assumes no additional booking records have been ingested and no records have been filtered out.


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

This project was built while following [Ansh's AWS + Snowflake + dbt tutorial](https://github.com/anshumanmahapatra/aws_snowflake_dbt) (adapted here to run on Databricks). During implementation, five issues in the reference video were identified and handled differently in this project:

### 1. Bookings modeled as a dimension, causing severe Fan-out and duplicate `booking_id`

In the reference tutorial (~5h20m mark), the bookings table is modeled as `dim_bookings` — i.e., treated as a **dimension table**. However, each row represents a discrete booking transaction, which makes bookings a **fact table** by nature (transactional grain, one row per business event). 

In the reference video (~5h19m), the `FACT` table, which should have one row per booking, contains the same `booking_id` (e.g., `ef0bd262...`) on at least 11 rows with identical values. This is a fan-out that inflates `total_amount` in any aggregation. The video does not dedupe the source tables before joining, which is the likely cause.

In this project, bookings are consistently modeled as a fact table (`stg_bookings` → `silver_bookings` → `gold_fact_bookings`), with `booking_id` strictly enforced as the unique/incremental key at every layer. We use `qualify row_number() = 1` to guarantee that one booking event equals exactly one row in the final table.

### 2. Incremental/snapshot design gaps: `created_at` as the cursor, and no dedup before `MERGE`

The reference tutorial's incremental models use `is_incremental()` correctly, but two design gaps remain. Both only surface once the underlying data stops being as clean as the tutorial's own dataset:

```sql
{{ config(materialized='incremental') }}

SELECT * FROM {{ source('staging', 'hosts') }}

{% if is_incremental() %}
    WHERE CREATED_AT > (SELECT COALESCE(MAX(CREATED_AT), '1900-01-01') FROM {{ this }})
{% endif %}
```

**`CREATED_AT` as the incremental cursor.** `created_at` is a **write-once** field — it's set when a row is first inserted and never changes. Using it as the cursor for `WHERE`, or as the `updated_at` column in a snapshot's `timestamp` strategy (the tutorial does both), means dbt cannot reliably detect updates to existing rows when their created_at values remain unchanged. It may also miss late-arriving historical records.

**Trade-off: Using `ingested_at` as an ingestion-based incremental watermark.**

The source CSV files contain `created_at` but do not provide a reliable `updated_at` field. This creates an important distinction between identifying newly arrived data and identifying the latest business version of a record.

The reference implementation uses `created_at` as its incremental watermark. While this works for strictly append-only data arriving in creation-time order, it can miss late-arriving historical records and updates to existing records whose `created_at` remains unchanged.

In this project, Auto Loader adds an `ingested_at` timestamp when source files are ingested into Bronze. Downstream dbt incremental models use this timestamp to identify newly ingested records, regardless of their original creation dates. This allows late-arriving historical records and re-ingested updates to be processed, provided they receive a newer ingestion timestamp and pass the incremental watermark.

However, **`ingested_at` represents ingestion order, not business update order**. If an older version of a record arrives after a newer version, using `ingested_at` to select the latest record may incorrectly overwrite the current business state. Additionally, changes that are never delivered to the ingestion layer cannot be detected.

**Design decision:** Given the absence of a reliable source `updated_at` or CDC sequence number, this project prioritizes ingestion completeness over guaranteed business-version ordering. It uses `ingested_at` for incremental processing and `strategy: check` for dbt snapshots to detect changes in selected business attributes.

This is a deliberate engineering trade-off rather than a complete solution to late-arriving updates. A production implementation would ideally use a reliable source modification timestamp or CDC sequence number to determine the latest business version, while retaining ingestion metadata to track newly arrived data.

**Production consideration: Source-system change tracking.**

In real-world enterprise systems, change tracking is often supported through source-maintained modification timestamps or dedicated CDC mechanisms. For example, Salesforce CRM exposes fields such as `LastModifiedDate` and `SystemModstamp`, Microsoft Dynamics 365 provides `modifiedon`, SAP systems may expose change information through Change Documents or delta-enabled extraction interfaces, and certain Xero API resources provide `UpdatedDateUTC`. The availability and reliability of these mechanisms depend on the source system, business object, and extraction method.

For this Airbnb project, the ideal source schema would include a reliable `updated_at` field for mutable entities such as `hosts` and `listings`, alongside the existing `created_at`. Auto Loader would still generate `ingested_at` to track data arrival, while `updated_at` would determine the latest business version of each record. This would allow incremental ingestion to remain independent of business-version ordering and reduce the risk of late-arriving older records overwriting newer dimension states.

Where stronger guarantees are required, a CDC sequence number or equivalent source change-tracking mechanism would provide a more reliable basis for handling out-of-order updates and deletions.

**No dedup before `MERGE`.** Neither this model nor its siblings include a `QUALIFY ROW_NUMBER() = 1` (or equivalent) before the final `SELECT`. Duplicate source keys can cause ambiguous matches during `MERGE`, potentially resulting in failures when multiple source rows match the same target row. Deduplicating the incoming batch ensures one source row per business key before the merge.

Every incremental model in this project ends with an explicit `QUALIFY ROW_NUMBER() OVER (PARTITION BY <primary_key> ORDER BY ingested_at DESC) = 1` to prevent duplicate source keys from causing merge conflicts.

**And no tests to catch it.** The reference tutorial's `obt` model and its other marts do not include a `unique` test on `booking_id`. Without dedup and without a test, duplicate rows stay invisible — the pipeline runs green, and the error only surfaces when downstream aggregations produce inflated numbers.

This project adds a `unique` + `not_null` test on the primary key of every staging, intermediate, and mart model. In the reference video, the `FACT` table shows repeated `booking_id` rows (see Note 1). In this project, `select booking_id, count(*) from obt group by 1 having count(*) > 1` returns zero rows, and the `unique` test is what makes that guarantee explicit and regression-proof.

### 3. Ambiguity in `booking_amount` vs. `total_amount`

In `silver_bookings.sql`:

```jinja
{{ multiply_and_round('nights_booked', 'booking_amount', 2) }} as total_amount
```

This calculation assumes `booking_amount` represents a **per-night rate**, and derives `total_amount = nights_booked * booking_amount`.

However, the source data definition does not make it clear whether `booking_amount` is already the **total price for the booking** (i.e., already aggregated across `nights_booked`) or a per-night rate similar to `price_per_night` in the listings table. If `booking_amount` is already a total, then multiplying it by `nights_booked` again would double-count the duration and significantly overstate `total_amount` / revenue.

The current implementation assumes `booking_amount` is a per-night rate, consistent with the naming pattern of `price_per_night`. If it is instead the already-aggregated total for the booking, `total_amount` would overstate revenue by a factor of `nights_booked`. Downstream consumers of `total_amount` should treat this assumption as unverified until the source schema is confirmed.

### 4. Snapshot source chosen for key uniqueness

The reference tutorial snapshots from `obt`, a wide join across bookings, listings, and hosts. The join chain itself is many-to-one and would not fan out on its own — but the tutorial does not dedupe the source tables before joining, so `obt` ends up with multiple rows per `booking_id`. A snapshot source must have exactly one row per `unique_key`; duplicate keys can lead to incorrect or ambiguous historical records.

This project snapshots from `silver_hosts` and `silver_listings`, both of which are deduplicated at the silver layer and therefore guaranteed key-unique at the time of snapshot.

### 5. Hardcoding column lists in Jinja is a bad practice for OBTs

The reference tutorial builds the `obt` dynamically using a Jinja `{% set configs = [...] %}` list with hardcoded column strings. In a real-world scenario with 100+ columns, this approach is extremely prone to human error (typos, missing fields, ambiguous join keys) and provides no IDE autocomplete or syntax highlighting.

This project avoids this anti-pattern by writing explicit, standard `SELECT` statements. For an `obt` that joins multiple tables with overlapping column names (like `created_at`, `ingested_at`, `source_file`), explicitly aliasing them with table prefixes is the safest and most readable approach:

```sql
select
    b.booking_id,
    b.listing_id,
    -- ...
    b.created_at as booking_created_at,
    b.ingested_at as booking_ingested_at,

    l.property_type,
    l.city,
    -- ...
    l.created_at as listing_created_at,
    
    h.host_name,
    -- ...
    h.created_at as host_created_at
from {{ ref('silver_bookings') }} b
left join {{ ref('silver_listings') }} l on b.listing_id = l.listing_id
left join {{ ref('silver_hosts') }} h on l.host_id = h.host_id
```

**How we inspect wide tables before modeling:**
To avoid guessing column names and to ensure 100% accuracy, we use a dynamic loop in Databricks to print the exact schema before writing the SQL:

```python
tables = ["listings", "bookings", "hosts"]

for t in tables:
    print(f"\n{'='*20} {t} Schema {'='*20}")
    display(spark.sql(f"DESCRIBE dbt_airbnb.source.{t}"))
```

By inspecting the schema first and explicitly writing the `SELECT` statement, we guarantee that the `obt` is readable, maintainable, and completely free of column name collisions.

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