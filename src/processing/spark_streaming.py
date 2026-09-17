from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka -> Spark Structured Streaming -> Silver/Quarantine para eventos industriales de apriete."
    )
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--topic", default="torque-events-raw")
    parser.add_argument(
        "--schema", default="/opt/project/src/schemas/torque_event.avsc"
    )
    parser.add_argument("--silver-output", default="/opt/project/data/streaming/silver")
    parser.add_argument(
        "--quarantine-output", default="/opt/project/data/streaming/quarantine"
    )
    parser.add_argument(
        "--checkpoint-root", default="/opt/project/data/streaming/checkpoints"
    )
    parser.add_argument(
        "--starting-offsets",
        choices=("earliest", "latest"),
        default="earliest",
    )
    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=5,
        help="Intervalo del trigger micro-batch.",
    )
    parser.add_argument(
        "--postgres-host", default=os.getenv("POSTGRES_HOST", "postgres")
    )
    parser.add_argument(
        "--postgres-port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432"))
    )
    parser.add_argument(
        "--postgres-db", default=os.getenv("POSTGRES_DB", "tfm_tightening")
    )
    parser.add_argument(
        "--postgres-user", default=os.getenv("POSTGRES_USER", "tfm_user")
    )
    parser.add_argument(
        "--postgres-password", default=os.getenv("POSTGRES_PASSWORD", "tfm_password")
    )
    parser.add_argument(
        "--postgres-schema", default=os.getenv("POSTGRES_SCHEMA", "analytics")
    )
    return parser.parse_args()


def build_validated_stream(raw: DataFrame, avro_schema: str) -> DataFrame:
    # Formato wire de Confluent = 1 magic byte + 4 bytes de schema id + payload Avro.
    payload = F.expr("substring(value, 6, length(value) - 5)")
    decoded = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        from_avro(payload, avro_schema).alias("event"),
    ).select(
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "event.*",
    )

    error_codes = F.array_compact(
        F.array(
            F.when(
                F.col("result_id").isNull() | (F.trim(F.col("result_id")) == ""),
                F.lit("DQ_MISSING_RESULT_ID"),
            ),
            F.when(
                F.col("event_timestamp").isNull(),
                F.lit("DQ_MISSING_EVENT_TIMESTAMP"),
            ),
            F.when(
                F.col("tightening_unit_id").isNull()
                | (F.trim(F.col("tightening_unit_id")) == ""),
                F.lit("DQ_MISSING_TIGHTENING_UNIT"),
            ),
            F.when(
                F.col("process_step_id").isNull()
                | (F.trim(F.col("process_step_id")) == ""),
                F.lit("DQ_MISSING_PROCESS_STEP"),
            ),
            F.when(
                F.col("substep_id").isNull() | (F.trim(F.col("substep_id")) == ""),
                F.lit("DQ_MISSING_SUBSTEP"),
            ),
            F.when(
                F.col("min_torque").isNotNull()
                & F.col("max_torque").isNotNull()
                & (F.col("min_torque") > F.col("max_torque")),
                F.lit("DQ_INVALID_TORQUE_LIMITS"),
            ),
        )
    )

    with_dq = decoded.withColumn("dq_error_codes", error_codes).withColumn(
        "dq_is_valid", F.size(F.col("dq_error_codes")) == 0
    )

    torque_applicable = (
        F.col("target_torque").isNotNull()
        & F.col("min_torque").isNotNull()
        & F.col("max_torque").isNotNull()
        & F.col("applied_torque").isNotNull()
        & (F.col("max_torque") > F.col("min_torque"))
        & (F.col("target_torque") != F.lit(0.0))
    )

    return (
        with_dq.withColumn("torque_applicable", torque_applicable)
        .withColumn(
            "torque_spec_status",
            F.when(~F.col("dq_is_valid"), F.lit("INVALID_DQ"))
            .when(~F.col("torque_applicable"), F.lit("NOT_APPLICABLE"))
            .when(
                (F.col("applied_torque") >= F.col("min_torque"))
                & (F.col("applied_torque") <= F.col("max_torque")),
                F.lit("IN_SPEC"),
            )
            .otherwise(F.lit("OUT_OF_SPEC")),
        )
        .withColumn("stream_processing_timestamp", F.current_timestamp())
    )


def add_serving_fields(valid: DataFrame) -> DataFrame:
    """Prepara los eventos válidos para la capa de serving PostgreSQL."""
    process_identity = [
        F.coalesce(F.col("tightening_unit_id"), F.lit("<NA>")),
        F.coalesce(F.col("process_step_id"), F.lit("<NA>")),
        F.coalesce(F.col("process_step_name"), F.lit("<NA>")),
        F.coalesce(F.col("substep_id"), F.lit("<NA>")),
        F.coalesce(F.col("substep_type"), F.lit("<NA>")),
    ]
    return (
        valid.withColumn(
            "process_key", F.sha2(F.concat_ws("|", *process_identity), 256)
        )
        .withColumn("factory_id", F.col("factory_id").cast("long"))
        .withColumn("line_id", F.col("line_id").cast("long"))
        .withColumn("work_area_id", F.col("work_area_id").cast("long"))
        .withColumn("tightening_unit_id", F.col("tightening_unit_id").cast("long"))
        .withColumn("process_step_id", F.col("process_step_id").cast("long"))
        .withColumn("process_step_number", F.col("process_step_number").cast("long"))
        .withColumn("substep_id", F.col("substep_id").cast("long"))
        .withColumn("substep_number", F.col("substep_number").cast("long"))
        .withColumn("substep_type", F.col("substep_type").cast("long"))
        .withColumn("tool_id", F.col("tool_id").cast("long"))
        .withColumn("tool_number", F.col("tool_number").cast("long"))
    )


def _jdbc_options(args: argparse.Namespace) -> dict[str, str]:
    return {
        "url": f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}",
        "user": args.postgres_user,
        "password": args.postgres_password,
        "driver": "org.postgresql.Driver",
    }


def _execute_jdbc_sql(
    spark: SparkSession,
    args: argparse.Namespace,
    sql: str,
) -> None:
    """Ejecuta una sentencia SQL en PostgreSQL mediante el driver JDBC cargado por Spark."""
    java_gateway = spark.sparkContext._gateway.jvm
    driver = java_gateway.org.postgresql.Driver()

    props = java_gateway.java.util.Properties()
    props.setProperty("user", args.postgres_user)
    props.setProperty("password", args.postgres_password)

    jdbc_url = (
        f"jdbc:postgresql://{args.postgres_host}:"
        f"{args.postgres_port}/{args.postgres_db}"
    )

    connection = driver.connect(
        jdbc_url,
        props,
    )

    try:
        statement = connection.createStatement()
        try:
            statement.execute(sql)
        finally:
            statement.close()
    finally:
        connection.close()


def write_postgres_microbatch(
    batch: DataFrame,
    batch_id: int,
    *,
    spark: SparkSession,
    args: argparse.Namespace,
) -> None:
    """Publica un micro-batch válido en PostgreSQL de forma idempotente por result_id y posición Kafka."""
    if batch.rdd.isEmpty():
        return

    schema = args.postgres_schema
    target = f"{schema}.streaming_events"
    stage = f"{schema}.streaming_events_stage"
    columns = [
        "result_id",
        "event_timestamp",
        "process_key",
        "factory_id",
        "factory_name",
        "line_id",
        "line_name",
        "work_area_id",
        "work_area_name",
        "tightening_unit_id",
        "tightening_unit_name",
        "process_step_id",
        "process_step_number",
        "process_step_name",
        "substep_id",
        "substep_number",
        "substep_type",
        "tool_id",
        "tool_number",
        "tool_serial_number",
        "target_torque",
        "min_torque",
        "max_torque",
        "applied_torque",
        "source_result_status",
        "torque_applicable",
        "torque_spec_status",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "stream_processing_timestamp",
    ]

    _execute_jdbc_sql(spark, args, f"TRUNCATE TABLE {stage}")
    (
        batch.select(*columns)
        .write.format("jdbc")
        .options(**_jdbc_options(args))
        .option("dbtable", stage)
        .mode("append")
        .save()
    )

    quoted = ", ".join(columns)
    merge_sql = f"""
        INSERT INTO {target} ({quoted})
        SELECT {quoted} FROM {stage}
        ON CONFLICT DO NOTHING
    """
    _execute_jdbc_sql(spark, args, merge_sql)
    print(f"[SPARK][POSTGRES] micro-batch={batch_id} publicado en {target}")


def main() -> None:
    args = parse_args()
    schema_text = Path(args.schema).read_text(encoding="utf-8")

    spark = (
        SparkSession.builder.appName("tfm-tightening-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    validated = build_validated_stream(raw, schema_text)
    valid = validated.filter(F.col("dq_is_valid"))
    quarantine = validated.filter(~F.col("dq_is_valid"))
    serving_valid = add_serving_fields(valid)

    checkpoint_root = Path(args.checkpoint_root)

    silver_query = (
        valid.writeStream.format("parquet")
        .outputMode("append")
        .option("path", args.silver_output)
        .option("checkpointLocation", str(checkpoint_root / "silver"))
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    quarantine_query = (
        quarantine.writeStream.format("parquet")
        .outputMode("append")
        .option("path", args.quarantine_output)
        .option("checkpointLocation", str(checkpoint_root / "quarantine"))
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    postgres_query = (
        serving_valid.writeStream.foreachBatch(
            lambda batch, batch_id: write_postgres_microbatch(
                batch, batch_id, spark=spark, args=args
            )
        )
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_root / "postgres"))
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    print("[SPARK] Streaming iniciado")
    print(f"[SPARK] topic={args.topic}")
    print(f"[SPARK] silver={args.silver_output}")
    print(f"[SPARK] quarantine={args.quarantine_output}")
    print(f"[SPARK] serving=PostgreSQL {args.postgres_schema}.streaming_events")
    print("[SPARK] Detener con Ctrl+C")

    try:
        spark.streams.awaitAnyTermination()
    finally:
        silver_query.stop()
        quarantine_query.stop()
        postgres_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
