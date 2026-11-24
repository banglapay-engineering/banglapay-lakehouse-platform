
# ============================================================
# BANGLAPAY LAKEHOUSE
# PHASE 1 | BATCH BRONZE INGESTION
#
# Sources:
#   Neon PostgreSQL : 22 tables
#   MongoDB Atlas   : 6 collections
#
# Ingestion:
#   Auto Loader
#   TEXT / NDJSON
#
# Output:
#   banglapay.bronze.pg_*
#   banglapay.bronze.mongo_*
#
# Data policy:
#   Preserve complete original NDJSON envelope.
#   No business transformations.
#   No deduplication.
#   No source mutations.
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# 1. ENVIRONMENT
# ============================================================

ENVIRONMENT = (
    spark.conf.get(
        "spark.banglapay.environment"
    )
    .strip()
    .lower()
)

if ENVIRONMENT not in ("dev", "prod"):
    raise ValueError(
        f"Invalid BanglaPay environment: {ENVIRONMENT}"
    )


# ============================================================
# 2. LANDING ROOTS
# ============================================================

POSTGRES_ROOT = (
    "/Volumes/banglapay/ops/batch_landing/postgres"
)

MONGODB_ROOT = (
    "/Volumes/banglapay/ops/batch_landing/mongodb"
)


# ============================================================
# 3. POSTGRESQL SOURCE INVENTORY
# ============================================================

POSTGRES_TABLES = [

    ("accounting", "journal_entries"),
    ("accounting", "ledger_accounts"),
    ("accounting", "ledger_entries"),

    ("customer", "customer_profiles"),
    ("customer", "customers"),

    ("integration", "outbox_events"),
    ("integration", "source_runs"),

    ("merchant", "merchant_terminals"),
    ("merchant", "merchants"),

    ("network", "agent_float_accounts"),
    ("network", "agent_outlets"),
    ("network", "agents"),

    ("payment", "refunds"),
    ("payment", "reversals"),
    ("payment", "service_providers"),
    ("payment", "transaction_fees"),
    ("payment", "transaction_status_history"),
    ("payment", "transactions"),

    ("settlement", "settlement_batches"),
    ("settlement", "settlement_items"),

    ("wallet", "wallet_balances"),
    ("wallet", "wallets")

]


# ============================================================
# 4. MONGODB SOURCE INVENTORY
# ============================================================

MONGODB_COLLECTIONS = [

    "app_sessions",
    "customer_activities",
    "device_profiles",
    "fraud_cases",
    "merchant_device_events",
    "support_cases"

]


# ============================================================
# 5. GENERIC AUTO LOADER TABLE FACTORY
#
# Each source object gets:
#   - An independent streaming table
#   - An independent Auto Loader checkpoint
#   - Its own source directory
#
# The factory pattern is supported by Lakeflow pipelines.
# ============================================================

def register_batch_bronze_table(
    table_name: str,
    source_path: str,
    source_system: str,
    source_database: str,
    source_object: str,
    source_schema: str | None
):

    comment = (
        f"Raw append-only Bronze ingestion from "
        f"{source_system}: "
        f"{source_database}.{source_object}"
    )

    @dp.table(
        name=table_name,
        comment=comment
    )
    def ingest_batch_source():

        raw_stream = (
            spark.readStream

            .format("cloudFiles")

            .option(
                "cloudFiles.format",
                "text"
            )

            # Accept only completed NDJSON output files.
            .option(
                "pathGlobfilter",
                "*.ndjson"
            )

            .load(source_path)
        )

        # Preserve the complete original NDJSON line.
        # Extract only technical ingestion metadata.
        return raw_stream.select(

            F.col("value").alias("raw_record"),

            F.lit(source_system).alias(
                "_source_system"
            ),

            F.lit(source_database).alias(
                "_source_database"
            ),

            F.lit(source_schema).alias(
                "_source_schema"
            ),

            F.lit(source_object).alias(
                "_source_object"
            ),

            F.get_json_object(
                F.col("value"),
                "$._meta.batch_id"
            ).alias("_batch_id"),

            F.get_json_object(
                F.col("value"),
                "$._meta.source_primary_key"
            ).alias("_source_primary_key"),

            F.get_json_object(
                F.col("value"),
                "$._meta.snapshot_type"
            ).alias("_snapshot_type"),

            F.get_json_object(
                F.col("value"),
                "$._meta.ingested_at"
            ).alias("_source_extracted_at"),

            F.get_json_object(
                F.col("value"),
                "$._meta.sample_percent"
            ).cast("int").alias("_sample_percent"),

            F.col("_metadata.file_path").alias(
                "_source_file_path"
            ),

            F.col(
                "_metadata.file_modification_time"
            ).alias("_source_file_modification_time"),

            F.current_timestamp().alias(
                "_bronze_ingested_at"
            ),

            F.lit(ENVIRONMENT).alias(
                "_environment"
            )

        )


# ============================================================
# 6. REGISTER 22 POSTGRESQL TABLES
# ============================================================

for schema_name, table_name in POSTGRES_TABLES:

    bronze_table = (
        f"pg_{schema_name}_{table_name}"
    )

    source_path = (
        f"{POSTGRES_ROOT}/"
        f"{schema_name}/"
        f"{table_name}/"
    )

    register_batch_bronze_table(

        table_name=bronze_table,

        source_path=source_path,

        source_system="neon_postgresql",

        source_database="banglapay",

        source_object=table_name,

        source_schema=schema_name

    )


# ============================================================
# 7. REGISTER 6 MONGODB COLLECTIONS
# ============================================================

for collection_name in MONGODB_COLLECTIONS:

    bronze_table = (
        f"mongo_{collection_name}"
    )

    source_path = (
        f"{MONGODB_ROOT}/"
        f"{collection_name}/"
    )

    register_batch_bronze_table(

        table_name=bronze_table,

        source_path=source_path,

        source_system="mongodb_atlas",

        source_database="banglapay_ops",

        source_object=collection_name,

        source_schema=None

    )


# ============================================================
# 8. SOURCE REGISTRATION SUMMARY
# ============================================================

print("BanglaPay batch Bronze definitions loaded.")

print("Environment:", ENVIRONMENT.upper())

print(
    "PostgreSQL streaming tables:",
    len(POSTGRES_TABLES)
)

print(
    "MongoDB streaming tables:",
    len(MONGODB_COLLECTIONS)
)

print(
    "Total batch Bronze tables:",
    len(POSTGRES_TABLES) + len(MONGODB_COLLECTIONS)
)