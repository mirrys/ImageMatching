from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schema import CsvDataset

import argparse

spark = SparkSession.builder.getOrCreate()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform raw algo output to production datasets"
    )
    parser.add_argument("--snapshot", help="Montlhy snapshot date (YYYY-MM)")
    parser.add_argument("--wiki", help="Wiki name")
    parser.add_argument("--source", help="Source dataset path")
    parser.add_argument("--destination", help="Destination path")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    snapshot = args.snapshot
    source = args.source
    destination = args.destination
    wiki = args.wiki

    csv_df = (
        (
            spark.read.options(delimiter="\t", header=False, escape='"')
            .schema(CsvDataset.schema)
            .csv(source)
        )
        .withColumn("wiki_db", F.lit(wiki))
        .withColumn("snapshot", F.lit(snapshot))
    )
    csv_df.coalesce(1).write.partitionBy("wiki_db", "snapshot").mode(
        "overwrite"
    ).parquet(
        destination
    )  # Requires dynamic partitioning enabled
    spark.stop()
