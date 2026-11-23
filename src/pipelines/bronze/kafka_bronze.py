
# ============================================================
# BANGLAPAY LAKEHOUSE
# PHASE 1 | BRONZE INGESTION
# Source: Confluent Kafka
#
# Seven Kafka topics
# Fourteen partitions
#
# Architecture:
# Native Spark Structured Streaming
# Lakeflow Spark Declarative Pipelines
#
# Output:
# banglapay.bronze.kafka_events
#
# Data policy:
# Preserve original Kafka payload.
# Do not deserialize AVRO in Bronze.
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# BANGLAPAY | KAFKA RUNTIME CONFIGURATION
# Secrets are retrieved at pipeline execution time.
# Never hardcode source credentials.
# ============================================================

SCOPE_NAME = "banglapay-ingestion"

# Environment is a non-sensitive pipeline parameter.
ENVIRONMENT = spark.conf.get(
    "spark.banglapay.environment"
).strip().lower()

if ENVIRONMENT not in ("dev", "prod"):
    raise ValueError(
        f"Invalid environment: {ENVIRONMENT}"
    )

# Source connection configuration
BOOTSTRAP_SERVERS = dbutils.secrets.get(
    scope=SCOPE_NAME,
    key="CONFLUENT_BOOTSTRAP_SERVERS"
).strip()

KAFKA_API_KEY = dbutils.secrets.get(
    scope=SCOPE_NAME,
    key="CONFLUENT_API_KEY"
).strip()

KAFKA_API_SECRET = dbutils.secrets.get(
    scope=SCOPE_NAME,
    key="CONFLUENT_API_SECRET"
).strip()

# Validate required settings without exposing values.
required_config = {
    "environment": ENVIRONMENT,
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "kafka_api_key": KAFKA_API_KEY,
    "kafka_api_secret": KAFKA_API_SECRET
}

missing = [
    key
    for key, value in required_config.items()
    if not value
]

if missing:
    raise ValueError(
        f"Missing Kafka configuration: {missing}"
    )


# ------------------------------------------------------------
# 3. Kafka source topics
# ------------------------------------------------------------

KAFKA_TOPICS = [
    "mfs.agent.events.v1",
    "mfs.customer.activity.v1",
    "mfs.merchant.events.v1",
    "mfs.risk.events.v1",
    "mfs.settlement.events.v1",
    "mfs.transaction.events.v1",
    "mfs.wallet.events.v1"
]


# ------------------------------------------------------------
# 4. Build SASL/PLAIN authentication configuration
#
# Databricks uses a shaded Kafka PlainLoginModule.
# Never print this configuration.
# ------------------------------------------------------------

def jaas_escape(value: str) -> str:

    # JAAS option values are double-quoted, so quotes and
    # backslashes must be escaped. Newlines cannot be
    # represented in a JAAS config and must fail fast
    # instead of silently breaking authentication.
    if "\n" in value or "\r" in value:
        raise ValueError(
            "Kafka credential contains an illegal"
            " control character."
        )

    return (
        value
        .replace("\\", "\\\\")
        .replace('"', '\\"')
    )


JAAS_CONFIG = (
    "kafkashaded.org.apache.kafka.common.security.plain."
    "PlainLoginModule required "
    f'username="{jaas_escape(KAFKA_API_KEY)}" '
    f'password="{jaas_escape(KAFKA_API_SECRET)}";'
)


# ------------------------------------------------------------
# 5. Define one Bronze streaming table
#
# All seven topics enter the same raw event table.
# Starting offsets = earliest on first initialization.
# Checkpoints manage subsequent progress.
# ------------------------------------------------------------

@dp.table(
    name="kafka_events",
    comment=(
        "Raw immutable Kafka events from all seven BanglaPay "
        "Confluent topics. Original key and value payloads "
        "are preserved for downstream replay and AVRO decoding."
    )
)
@dp.expect(
    "valid_confluent_avro",
    "schema_id IS NOT NULL"
)
def kafka_events():

    source_df = (
        spark.readStream
        .format("kafka")

        .option(
            "kafka.bootstrap.servers",
            BOOTSTRAP_SERVERS
        )

        .option(
            "subscribe",
            ",".join(KAFKA_TOPICS)
        )

        .option(
            "startingOffsets",
            "earliest"
        )

        .option(
            "kafka.security.protocol",
            "SASL_SSL"
        )

        .option(
            "kafka.sasl.mechanism",
            "PLAIN"
        )

        .option(
            "kafka.sasl.jaas.config",
            JAAS_CONFIG
        )

        .option(
            "includeHeaders",
            "true"
        )

        .load()
    )

    # --------------------------------------------------------
    # 6. Preserve original Kafka payload and metadata
    # --------------------------------------------------------

    schema_id = (
        F.when(
            F.hex(
                F.substring(F.col("value"), 1, 1)
            ) == "00",

            F.conv(
                F.hex(
                    F.substring(F.col("value"), 2, 4)
                ),
                16,
                10
            ).cast("int")
        )
        .otherwise(F.lit(None).cast("int"))
    )

    bronze_df = source_df.select(
        F.col("topic").alias("kafka_topic"),

        F.col("partition").alias("kafka_partition"),

        F.col("offset").alias("kafka_offset"),

        F.col("timestamp").alias("kafka_timestamp"),

        F.col("timestampType").alias(
            "kafka_timestamp_type"
        ),

        F.col("key").alias("kafka_key"),

        F.col("value").alias("kafka_value"),

        F.col("headers").alias("kafka_headers"),

        F.length("value").alias("payload_size_bytes"),

        schema_id.alias("schema_id"),

        F.current_timestamp().alias("_ingested_at"),

        F.lit(ENVIRONMENT).alias("_environment")
    )

    return bronze_df