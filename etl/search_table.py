from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import argparse

spark = SparkSession.builder.getOrCreate()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform raw algo output to production datasets"
    )
    parser.add_argument("--snapshot", help="Montlhy snapshot date (YYYY-MM)")
    parser.add_argument("--source", help="Source dataset path")
    parser.add_argument("--destination", help="Destination path")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    snapshot = args.snapshot
    source = args.source
    destination = args.destination

    num_partitions = 1

    df = spark.read.parquet(source)
    (
        df
        .where(~F.col("image_id").isNull())
        .where(F.col("is_article_page") is True)
        .withColumn("page_namespace", F.lit(0))
        .withColumn("recommendation_type", F.lit('image'))
        .withColumnRenamed("wiki", "wikiid")
        .select(
            "wikiid",
            "page_id",
            "page_namespace",
            "recommendation_type"
        )
        .coalesce(num_partitions)
        .write.partitionBy("snapshot")
        .mode("overwrite")  # Requires dynamic partitioning enabled
        .parquet(destination)
    )
    spark.stop()
