import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

APP_ROOT = os.getenv("APP_ROOT", "/app")
if APP_ROOT not in sys.path:
    sys.path.insert(0, APP_ROOT)

from services.analytics_service.config import (
    ANALYTICS_DEBUG,
    CHECKPOINT,
    HDFS_PATH,
    INPUT_TOPIC,
    KAFKA_STARTING_OFFSETS,
    KAFKA_OPTIONS,
    OUTPUT_TOPIC,
    TRIGGER,
)


def main() -> None:
    spark = SparkSession.builder.appName("analytics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(
        "[analytics] start "
        f"input_topic={INPUT_TOPIC} output_topic={OUTPUT_TOPIC} "
        f"hdfs_path={HDFS_PATH} checkpoint={CHECKPOINT} "
        f"starting_offsets={KAFKA_STARTING_OFFSETS} debug={ANALYTICS_DEBUG}"
    )

    source = (
        spark.readStream.format("kafka")
        .options(**KAFKA_OPTIONS)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("subscribe", INPUT_TOPIC)
        .load()
    )

    events = (
        source.select(F.col("value").cast("string").alias("v"))
        .select(
            F.get_json_object("v", "$.raw_json.product_id").alias("product_id"),
            F.get_json_object("v", "$.raw_json.store_id").alias("store_id"),
            F.coalesce(
                F.get_json_object("v", "$.raw_json.price.amount"),
                F.get_json_object("v", "$.raw_json.price"),
            ).cast("double").alias("price"),
            F.col("v").alias("raw_payload"),
        )
    )

    def process_batch(df, batch_id) -> None:
        total_count = df.count()
        print(f"[analytics] batch={batch_id} total_count={total_count}")
        if total_count == 0:
            print(f"[analytics] batch={batch_id} skip-empty")
            return

        valid_df = df.where(
            F.col("product_id").isNotNull()
            & F.col("store_id").isNotNull()
            & F.col("price").isNotNull()
        )
        valid_count = valid_df.count()
        invalid_count = total_count - valid_count
        print(
            f"[analytics] batch={batch_id} valid_count={valid_count} "
            f"invalid_count={invalid_count}"
        )

        if ANALYTICS_DEBUG and invalid_count > 0:
            invalid_rows = (
                df.where(
                    F.col("product_id").isNull()
                    | F.col("store_id").isNull()
                    | F.col("price").isNull()
                )
                .select("product_id", "store_id", "price", "raw_payload")
                .limit(3)
                .collect()
            )
            print(f"[analytics] batch={batch_id} invalid_samples={invalid_rows}")

        if valid_count == 0:
            print(f"[analytics] batch={batch_id} skip-no-valid-events")
            return

        valid_df.write.mode("append").json(HDFS_PATH)
        print(f"[analytics] batch={batch_id} hdfs_write_ok path={HDFS_PATH}")

        avg = valid_df.groupBy("store_id").agg(F.avg("price").alias("avg_price"))

        recs = (
            valid_df.join(avg, "store_id")
            .where(F.col("avg_price") > F.lit(0))
            .withColumn(
                "score",
                F.round((F.col("avg_price") - F.col("price")) / F.col("avg_price"), 3),
            )
            .where(F.col("score") >= F.lit(0))
        )
        recs_count = recs.count()
        print(f"[analytics] batch={batch_id} recs_count={recs_count}")

        out = recs.select(
            F.col("product_id").alias("key"),
            F.to_json(F.struct("store_id", "product_id", "score")).alias("value"),
        )

        if recs_count == 0:
            print(f"[analytics] batch={batch_id} skip-no-recs")
            return

        if ANALYTICS_DEBUG:
            rec_samples = out.limit(3).collect()
            print(f"[analytics] batch={batch_id} rec_samples={rec_samples}")

        (
            out.write.format("kafka")
            .options(**KAFKA_OPTIONS)
            .option("topic", OUTPUT_TOPIC)
            .save()
        )
        print(f"[analytics] batch={batch_id} kafka_write_ok topic={OUTPUT_TOPIC}")

    (
        events.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
