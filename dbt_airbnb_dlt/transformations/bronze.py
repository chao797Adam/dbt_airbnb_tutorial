import dlt
from pyspark.sql.functions import col, to_timestamp

# ============================================================
# bronze_bookings
# ============================================================
@dlt.table(
    name="bronze_bookings",
    comment="Raw bookings ingested from dbt_airbnb.source.bookings, with type casts applied.",
)
@dlt.expect("valid_booking_id", "booking_id IS NOT NULL")
@dlt.expect("valid_listing_id", "listing_id IS NOT NULL")
def bronze_bookings():
    return (
        spark.readStream.table("dbt_airbnb.source.bookings")
        .select(
            col("booking_id").cast("string").alias("booking_id"),
            col("listing_id").cast("bigint").alias("listing_id"),
            col("booking_date").cast("date").alias("booking_date"),
            col("nights_booked").cast("int").alias("nights_booked"),
            col("booking_amount").cast("double").alias("booking_amount"),
            col("cleaning_fee").cast("double").alias("cleaning_fee"),
            col("service_fee").cast("double").alias("service_fee"),
            col("booking_status").cast("string").alias("booking_status"),
            to_timestamp("created_at").alias("created_at"),
            col("ingested_at").cast("timestamp").alias("ingested_at"),
            col("source_file").cast("string").alias("source_file"),
        )
    )


# ============================================================
# bronze_listings
# ============================================================
@dlt.table(
    name="bronze_listings",
    comment="Raw listings ingested from dbt_airbnb.source.listings, with type casts applied.",
)
@dlt.expect("valid_listing_id", "listing_id IS NOT NULL")
@dlt.expect("valid_host_id", "host_id IS NOT NULL")
def bronze_listings():
    return (
        spark.readStream.table("dbt_airbnb.source.listings")
        .select(
            col("listing_id").cast("bigint").alias("listing_id"),
            col("host_id").cast("bigint").alias("host_id"),
            col("property_type").cast("string").alias("property_type"),
            col("room_type").cast("string").alias("room_type"),
            col("city").cast("string").alias("city"),
            col("country").cast("string").alias("country"),
            col("accommodates").cast("int").alias("accommodates"),
            col("bathrooms").cast("double").alias("bathrooms"),
            col("bedrooms").cast("double").alias("bedrooms"),
            col("price_per_night").cast("double").alias("price_per_night"),
            to_timestamp("created_at").alias("created_at"),
            col("ingested_at").cast("timestamp").alias("ingested_at"),
            col("source_file").cast("string").alias("source_file"),
        )
    )


# ============================================================
# bronze_hosts
# ============================================================
@dlt.table(
    name="bronze_hosts",
    comment="Raw hosts ingested from dbt_airbnb.source.hosts, with type casts applied.",
)
@dlt.expect("valid_host_id", "host_id IS NOT NULL")
def bronze_hosts():
    return (
        spark.readStream.table("dbt_airbnb.source.hosts")
        .select(
            col("host_id").cast("bigint").alias("host_id"),
            col("host_name").cast("string").alias("host_name"),
            col("host_since").cast("date").alias("host_since"),
            col("is_superhost").cast("boolean").alias("is_superhost"),
            col("response_rate").cast("double").alias("response_rate"),
            to_timestamp("created_at").alias("created_at"),
            col("ingested_at").cast("timestamp").alias("ingested_at"),
            col("source_file").cast("string").alias("source_file"),
        )
    )