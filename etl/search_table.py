from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import argparse

spark = SparkSession.builder.getOrCreate()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform raw algo output to production datasets"
    )
    parser.add_argument("--snapshot", help="Monthly snapshot date (YYYY-MM-DD)")
    parser.add_argument("--source", help="Source dataset path")
    parser.add_argument("--destination", help="Destination path")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    snapshot = args.snapshot.split("-")
    source = args.source
    destination = args.destination
    year = snapshot[0]
    month = snapshot[1]
    day = snapshot[2]

    num_partitions = 1

    df = spark.read.parquet(source)
    (
        df
        .where(~F.col("image_id").isNull())
        .filter(F.col("is_article_page") == True)
        .withColumn("page_namespace", F.lit(0))
        .withColumn("recommendation_type", F.lit('image'))
        .withColumn("year", F.lit(year))
        .withColumn("month", F.lit(month))
        .withColumn("day", F.lit(day))
        .withColumnRenamed("wiki", "wikiid")
        .withColumn("page_id", df.page_id.cast('int'))
        .select(
            "wikiid",
            "page_id",
            "page_namespace",
            "recommendation_type",
            "year",
            "month",
            "day"
        )
        .distinct()
        .coalesce(num_partitions)
        .write.partitionBy("year", "month", "day")
        .mode("overwrite")  # Requires dynamic partitioning enabled
        .parquet(destination)
    )
    spark.stop()
