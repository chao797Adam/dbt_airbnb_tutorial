import dlt
from pyspark.sql.functions import col, replace, when, lit, round as spark_round

# ============================================================
# silver_bookings (SCD1)
# ============================================================
dlt.create_streaming_table(
    name="silver_bookings",
    comment="Cleaned bookings with total_amount derived, deduplicated on booking_id.",
)

@dlt.view
def silver_bookings_source():
    return (
        dlt.read_stream("bronze_bookings")     # ← 改这里
        .withColumn(
            "total_amount",
            spark_round(col("nights_booked") * col("booking_amount"), 2),
        )
    )

dlt.create_auto_cdc_flow(
    target="silver_bookings",
    source="silver_bookings_source",
    keys=["booking_id"],
    sequence_by=col("ingested_at"),
    stored_as_scd_type=1,
)


# ============================================================
# silver_hosts (SCD1)
# ============================================================
dlt.create_streaming_table(
    name="silver_hosts",
    comment="Cleaned hosts with response_rate_tag, deduplicated on host_id.",
)

@dlt.view
def silver_hosts_source():
    return (
        dlt.read_stream("bronze_hosts")        # ← 改这里
        .withColumn("host_name", replace(col("host_name"), lit(" "), lit("_")))
        .withColumn(
            "response_rate_tag",
            when(col("response_rate") > 95, "very_good")
            .when(col("response_rate") > 80, "good")
            .when(col("response_rate") > 60, "fair")
            .otherwise("poor"),
        )
    )

dlt.create_auto_cdc_flow(
    target="silver_hosts",
    source="silver_hosts_source",
    keys=["host_id"],
    sequence_by=col("ingested_at"),
    stored_as_scd_type=1,
)


# ============================================================
# silver_listings (SCD1)
# ============================================================
dlt.create_streaming_table(
    name="silver_listings",
    comment="Cleaned listings with price_per_night_tag, deduplicated on listing_id.",
)

@dlt.view
def silver_listings_source():
    return (
        dlt.read_stream("bronze_listings")     # ← 改这里
        .withColumn(
            "price_per_night_tag",
            when(col("price_per_night") < 100, "low")
            .when(col("price_per_night") < 200, "medium")
            .otherwise("high"),
        )
    )

dlt.create_auto_cdc_flow(
    target="silver_listings",
    source="silver_listings_source",
    keys=["listing_id"],
    sequence_by=col("ingested_at"),
    stored_as_scd_type=1,
)