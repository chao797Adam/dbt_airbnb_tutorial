import dlt
from pyspark.sql.functions import col

# ============================================================
# dim_hosts (SCD2)
# ============================================================
dlt.create_streaming_table(
    name="dim_hosts",
    comment="Host dimension with SCD Type 2 history tracking.",
)

@dlt.view
def dim_hosts_source():
    return dlt.read_stream("silver_hosts")     # ← dlt.read_stream

dlt.create_auto_cdc_flow(
    target="dim_hosts",
    source="dim_hosts_source",
    keys=["host_id"],
    sequence_by=col("ingested_at"),
    stored_as_scd_type=2,
)


# ============================================================
# dim_listings (SCD2)
# ============================================================
dlt.create_streaming_table(
    name="dim_listings",
    comment="Listing dimension with SCD Type 2 history tracking.",
)

@dlt.view
def dim_listings_source():
    return dlt.read_stream("silver_listings")  # ← dlt.read_stream

dlt.create_auto_cdc_flow(
    target="dim_listings",
    source="dim_listings_source",
    keys=["listing_id"],
    sequence_by=col("ingested_at"),
    stored_as_scd_type=2,
)


# ============================================================
# gold_fact_bookings
# ============================================================
@dlt.table(
    name="gold_fact_bookings",
    comment="Booking fact table, joined with listing dimension (city, property_type).",
)
@dlt.expect("valid_booking_id", "booking_id IS NOT NULL")
def gold_fact_bookings():
    b = dlt.read("silver_bookings")
    l = dlt.read("silver_listings")
    return (
        b.alias("b")
        .join(l.alias("l"), "listing_id", "left")
        .select(
            col("b.booking_id"),
            col("b.listing_id"),
            col("l.city"),
            col("l.property_type"),
            col("b.booking_date"),
            col("b.total_amount"),
            col("b.booking_status"),
            col("b.ingested_at").alias("last_sync_time"),
        )
    )


# ============================================================
# obt
# ============================================================
@dlt.table(
    name="obt",
    comment="One Big Table: bookings joined with listings and hosts for BI.",
)
@dlt.expect("valid_booking_id", "booking_id IS NOT NULL")
def obt():
    b = dlt.read("silver_bookings")
    l = dlt.read("silver_listings")
    h = dlt.read("silver_hosts")
    return (
        b.alias("b")
        .join(l.alias("l"), "listing_id", "left")
        .join(h.alias("h"), "host_id", "left")
        .select(
            col("b.booking_id"),
            col("b.listing_id"),
            col("b.booking_date"),
            col("b.nights_booked"),
            col("b.booking_amount"),
            col("b.cleaning_fee"),
            col("b.service_fee"),
            col("b.total_amount"),
            col("b.booking_status"),
            col("b.created_at").alias("booking_created_at"),
            col("b.ingested_at").alias("booking_ingested_at"),
            col("b.source_file").alias("booking_source_file"),
            col("l.property_type"),
            col("l.room_type"),
            col("l.city"),
            col("l.country"),
            col("l.accommodates"),
            col("l.bathrooms"),
            col("l.bedrooms"),
            col("l.price_per_night"),
            col("l.price_per_night_tag"),
            col("l.created_at").alias("listing_created_at"),
            col("l.ingested_at").alias("listing_ingested_at"),
            col("h.host_id"),
            col("h.host_name"),
            col("h.host_since"),
            col("h.is_superhost"),
            col("h.response_rate"),
            col("h.response_rate_tag"),
            col("h.created_at").alias("host_created_at"),
        )
    )