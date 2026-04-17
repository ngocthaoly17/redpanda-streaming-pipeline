from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, to_timestamp
from pyspark.sql.types import StructType, StringType
import os

BOOTSTRAP_SERVERS = "redpanda:9092"
TOPIC_NAME = "client_tickets"

CHECKPOINT_BASE = "/app/checkpoints"
OUTPUT_BASE = "/app/output"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RedpandaTicketAnalysis")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def write_batch_to_json(output_path):
    def _writer(batch_df, batch_id):
        batch_output = os.path.join(output_path, f"batch_{batch_id}")
        (
            batch_df.coalesce(1)
            .write
            .mode("overwrite")
            .json(batch_output)
        )
        print(f"[OK] Batch {batch_id} écrit dans {batch_output}")
    return _writer


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = (
        StructType()
        .add("ticket_id", StringType())
        .add("client_id", StringType())
        .add("created_at", StringType())
        .add("request", StringType())
        .add("request_type", StringType())
        .add("priority", StringType())
    )

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("created_at_ts", to_timestamp("created_at"))
    )

    enriched_df = (
        parsed_df.withColumn(
            "support_team",
            when(col("request_type") == "technical", "Technical Support")
            .when(col("request_type") == "billing", "Billing Team")
            .when(col("request_type") == "bug", "Engineering Team")
            .otherwise("Customer Care")
        )
    )

    tickets_by_type_df = enriched_df.groupBy("request_type").count()
    tickets_by_priority_df = enriched_df.groupBy("priority").count()
    tickets_by_team_df = enriched_df.groupBy("support_team").count()

    query_enriched = (
        enriched_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{OUTPUT_BASE}/enriched_tickets")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/enriched_tickets")
        .start()
    )

    query_type = (
        tickets_by_type_df.writeStream
        .outputMode("complete")
        .foreachBatch(write_batch_to_json(f"{OUTPUT_BASE}/tickets_by_type"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/tickets_by_type")
        .start()
    )

    query_priority = (
        tickets_by_priority_df.writeStream
        .outputMode("complete")
        .foreachBatch(write_batch_to_json(f"{OUTPUT_BASE}/tickets_by_priority"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/tickets_by_priority")
        .start()
    )

    query_team = (
        tickets_by_team_df.writeStream
        .outputMode("complete")
        .foreachBatch(write_batch_to_json(f"{OUTPUT_BASE}/tickets_by_team"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/tickets_by_team")
        .start()
    )

    print("Streaming démarré.")
    print("Exports :")
    print(f"- {OUTPUT_BASE}/enriched_tickets (parquet)")
    print(f"- {OUTPUT_BASE}/tickets_by_type (json via foreachBatch)")
    print(f"- {OUTPUT_BASE}/tickets_by_priority (json via foreachBatch)")
    print(f"- {OUTPUT_BASE}/tickets_by_team (json via foreachBatch)")

    spark.streams.awaitAnyTermination()

    query_enriched.stop()
    query_type.stop()
    query_priority.stop()
    query_team.stop()


if __name__ == "__main__":
    main()
