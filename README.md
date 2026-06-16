# dbt Airbnb Case

A layered Airbnb data modeling project built with [dbt](https://www.getdbt.com/) on Databricks. It demonstrates an end-to-end pipeline from raw source data to analytics-ready mart tables, covering **incremental loads, snapshot-based history tracking, custom macros, and data tests**.

---

## 📐 Data Layer Architecture

The project follows a classic **Bronze / Silver / Gold** architecture, mapped to dbt's `staging` / `intermediate` / `mart` directories:

| Layer | Directory | Schema | Materialization | Description |
|-------|-----------|--------|------------------|-------------|
| Bronze (Raw) | `models/staging` | `bronze` | `incremental` | Type casting and field standardization on source data, incrementally loaded by `ingested_at` |
| Silver (Cleaned) | `models/intermediate` | `silver` | `incremental` + `merge` | Business-level cleaning, derived fields, and tagging |
| Gold (Serving) | `models/mart` | `gold` | `view` / `table` | Analytics-ready fact tables and one-big-table (OBT) |

```
source(airbnb_source) ─▶ staging (bronze) ─▶ intermediate (silver) ─▶ mart (gold)
```

---

## 📁 Project Structure

```
dbt_airbnb_case/
├── analyses/              # Ad-hoc analysis SQL (not materialized)
├── macros/                # Custom macros
│   ├── clean_string.sql       # String cleaning (trim + uppercase)
│   ├── multiply.sql           # Multiply two numbers and round
│   ├── schema.sql              # Custom schema naming logic
│   └── tag.sql                  # Bucket numeric values into tags
├── models/
│   ├── staging/            # Bronze layer
│   │   ├── source.yml          # Source definitions
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
│   └── source_test.sql
├── dbt_project.yml
└── packages.yml
```

---

## 🗄️ Source

Source tables are defined in `models/staging/source.yml`:

| Source | Database | Schema | Tables |
|--------|----------|--------|--------|
| `airbnb_source` | `dbt_airbnb` | `source` | `listings`, `bookings`, `hosts` |

---

## 🧱 Models

### Staging (Bronze)

Casts data types, standardizes column names, and incrementally loads new rows based on `ingested_at`.

| Model | Primary Key | Description |
|-------|-------------|-------------|
| `stg_listings` | `listing_id` | Listing details (property type, city, price, etc.) |
| `stg_hosts` | `host_id` | Host info (signup date, superhost flag, response rate) |
| `stg_bookings` | `booking_id` | Booking records (nights booked, amounts, status) |

### Intermediate (Silver)

Applies business logic, cleaning, and derived fields on top of staging:

| Model | Key Logic |
|-------|-----------|
| `silver_bookings` | Computes `total_amount` (price × nights booked) using the `multiply_and_round` macro |
| `silver_hosts` | Cleans `host_name` (replaces spaces with underscores); tags `response_rate` as `very_good` / `good` / `fair` / `poor` |
| `silver_listings` | Buckets `price_per_night` into `low` / `medium` / `high` using the `tag_col` macro |

### Mart (Gold)

| Model | Materialization | Description |
|-------|------------------|-------------|
| `gold_fact_bookings` | `view` | Booking fact table, joined with listing dimensions (city, property type) |
| `obt` | `table` | One Big Table combining all fields from Bookings, Listings, and Hosts for BI/analytics |

---

## 🛠️ Custom Macros

| Macro | Purpose |
|-------|---------|
| `clean_string(column_name)` | Trims a string column and converts it to uppercase |
| `multiply_and_round(col1, col2, precision=2)` | Multiplies two columns and rounds to the given precision |
| `tag_col(col)` | Buckets a numeric column into tags: `<100` → `low`, `<200` → `medium`, else → `high` |
| `generate_schema_name(custom_schema_name, node)` | Overrides dbt's default schema naming so models use their configured `schema` directly, without prefixing the target schema |

---

## 📸 Snapshots

Used to track historical changes (SCD Type 2) for dimension data, stored in the `snapshots` schema:

| Snapshot | Source Model | Strategy | Tracked Columns |
|----------|--------------|----------|------------------|
| `hosts_snapshot` | `stg_hosts` | `check` | `host_name`, `is_superhost`, `response_rate` |
| `listings_snapshot` | `stg_listings` | `check` | `price_per_night`, `room_type`, `bedrooms`, `bathrooms`, `accommodates`, `city`, `country` |

> ⚠️ Note: the project-level default snapshot strategy in `dbt_project.yml` is `timestamp` (based on `updated_at`/`ingested_at`), but both snapshot files explicitly set `strategy='check'`, which overrides the project default.

---

## ✅ Data Tests

`tests/source_test.sql`: a custom singular test that flags any rows in `airbnb_source.bookings` where `booking_amount < 200`. It's configured with `severity = warn`, so issues raise a warning without failing `dbt build`.

The `dbt_utils` package (declared in `packages.yml`, version `1.3.3`) is also available and can be used to add generic tests (e.g., `unique`, `not_null`, `relationships`) to each model.

---

## 🚀 Getting Started

### 1. Set Up the Environment

This project uses [uv](https://github.com/astral-sh/uv) to manage Python dependencies, running dbt on Databricks:

```bash
# Install dependencies (dbt-core, dbt-databricks)
uv sync
```

Requirements (see `pyproject.toml`):
- Python `>= 3.12`
- `dbt-core >= 1.11.6`
- `dbt-databricks >= 1.11.6`

### 2. Configure Your Profile

Add a connection profile to `~/.dbt/profiles.yml`. The profile name must match `profile: dbt_airbnb_case` in `dbt_project.yml`:

```yaml
dbt_airbnb_case:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: dbt_airbnb
      schema: dev_<your_name>
      host: <your-databricks-host>
      http_path: <your-sql-warehouse-http-path>
      token: <your-databricks-token>
      threads: 4
```

### 3. Install Package Dependencies

```bash
dbt deps
```

### 4. Run the Project

```bash
# Run all models
dbt run

# Run data tests
dbt test

# Run snapshots
dbt snapshot

# Run + test + snapshot in one go
dbt build
```

### 5. Debugging / Exploration

The `analyses/` directory contains exploratory SQL files (e.g., `explore.sql`, `ifElse.sql`, `loop.sql`, and per-model test scripts). Run `dbt compile` to view the compiled SQL for any model.

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

### 2. Use of `created_at` as snapshot timestamp — not production-grade

In the reference tutorial, the snapshot strategy uses `created_at` as the `updated_at` column for the `timestamp` strategy:

```yaml
strategy: timestamp
updated_at: created_at
```

This is problematic in a real-world context. `created_at` is a **write-once** field — it records when the record was first inserted and never changes. Using it as the snapshot trigger means dbt will **never detect any updates** to an existing row, because the timestamp never moves forward. The snapshot would only capture new inserts, completely missing the SCD2 goal of tracking changes over time.

In production pipelines, the correct approach is to use:
- `ingested_at` — the timestamp when the record was last loaded into the data platform (updated on every reload by the ingestion layer)
- `updated_at` — the timestamp from the source system recording when the record was last modified

This project uses `ingested_at` as the incremental filter in staging models, and the snapshots use `strategy: check` (comparing column values directly) rather than relying on a potentially unreliable timestamp from the source. The `check` strategy is more robust when source systems do not provide a trustworthy `updated_at` field.

### 3. Ambiguity in `booking_amount` vs. `total_amount`

In `silver_bookings.sql`:

```jinja
{{ multiply_and_round('nights_booked', 'booking_amount', 2) }} as total_amount
```

This calculation assumes `booking_amount` represents a **per-night rate**, and derives `total_amount = nights_booked * booking_amount`.

However, the source data definition does not make it clear whether `booking_amount` is already the **total price for the booking** (i.e., already aggregated across `nights_booked`) or a per-night rate similar to `price_per_night` in the listings table. If `booking_amount` is already a total, then multiplying it by `nights_booked` again would double-count the duration and significantly overstate `total_amount` / revenue.

**Open question / recommendation:** confirm the semantics of `booking_amount` with the source system or data owner before relying on `total_amount` for downstream reporting. The current implementation assumes `booking_amount` is a per-night rate (consistent with the naming pattern of `price_per_night`), but this should be validated against real source data rather than assumed.

---

## 📚 References

- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [dbt-databricks Adapter Setup](https://docs.getdbt.com/reference/warehouse-setups/databricks-setup)
- [dbt_utils Package](https://github.com/dbt-labs/dbt-utils)
