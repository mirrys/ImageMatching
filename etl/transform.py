from pyspark.sql import SparkSession
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from schema import RawDataset
from instances_to_filter import InstancesToFilter

import argparse
import uuid
import datetime

spark = SparkSession.builder.getOrCreate()


class ImageRecommendation:
    confidence_rating: Column = (
        F.when(F.col("rating").cast(IntegerType()) == 1, F.lit("high"))
        .when(F.col("rating").cast(IntegerType()) == 2, F.lit("medium"))
        .when(F.col("rating").cast(IntegerType()) == 3, F.lit("low"))
    )
    source: Column = (
        F.when(
            F.col("note").like(r"image was in the Wikidata item%"), F.lit("wikidata")
        )
        .when(
            F.col("note").like(r"image was found in the following Wikis:%"),
            F.lit("wikipedia"),
        )
        .when(
            F.col("note").like(r"image was found in the Commons category%"),
            F.lit("commons"),
        )
    )

    instance_of: Column = F.when(F.col("instance_of").isNull(), F.lit(None)).otherwise(
        F.from_json("instance_of", RawDataset.instance_of_schema).getItem("id")
    )

    found_on: Column = F.when(F.col("note").isNull(), F.lit(None)).otherwise(
        F.split(
            F.regexp_replace(
                F.regexp_extract(F.col("note"), "Wikis:\s+(.*)$", 1), "\s+", ""
            ),
            ",",
        )
    )

    is_article_page: Column = (
        F.when(
            F.col("instance_of").isin(InstancesToFilter.list()),
            F.lit(False)
        )
        .otherwise(True)
    )

    def __init__(self, dataFrame: DataFrame):
        self.dataFrame = dataFrame
        if not dataFrame.schema == RawDataset.schema:
            raise AttributeError(
                f"Invalid schema. Expected '{RawDataset.schema}'. Got '{dataFrame.schema}"
            )

    def transform(self) -> DataFrame:
        with_recommendations = (
            self.dataFrame.where(~F.col("top_candidates").isNull())
            .withColumn(
                "data",
                F.explode(
                    F.from_json("top_candidates", RawDataset.top_candidates_schema)
                ),
            )
            .select("*", "data.image", "data.rating", "data.note")
            .withColumnRenamed("wiki_db", "wiki")
            .withColumnRenamed("image", "image_id")
            .withColumn("confidence_rating", self.confidence_rating)
            .withColumn("source", self.source)
            .withColumn("found_on", self.found_on)
            .select(
                "wiki",
                "page_id",
                "page_title",
                "image_id",
                "confidence_rating",
                "source",
                "instance_of",
                "found_on",
            )
        )
        without_recommendations = (
            self.dataFrame.where(F.col("top_candidates").isNull())
            .withColumnRenamed("wiki_db", "wiki")
            .withColumn("image_id", F.lit(None))
            .withColumn("confidence_rating", F.lit(None))
            .withColumn("source", F.lit(None))
            .withColumn("found_on", F.lit(None))
            .select(
                "wiki",
                "page_id",
                "page_title",
                "image_id",
                "confidence_rating",
                "source",
                "instance_of",
                "found_on",
            )
        )

        return with_recommendations.union(without_recommendations)\
            .withColumn("instance_of", self.instance_of)\
            .withColumn("is_article_page", self.is_article_page)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform raw algo output to production datasets"
    )
    parser.add_argument("--snapshot", help="Montlhy snapshot date (YYYY-MM)")
    parser.add_argument("--source", help="Source dataset path")
    parser.add_argument("--destination", help="Destination path")
    parser.add_argument(
        "--dataset-id",
        help="Production dataset identifier (optional)",
        default=str(uuid.uuid4()),
        dest="dataset_id",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    snapshot = args.snapshot
    source = args.source
    destination = args.destination
    dataset_id = args.dataset_id

    num_partitions = 1

    df = spark.read.schema(RawDataset.schema).parquet(source)
    insertion_ts = datetime.datetime.now().timestamp()
    (
        ImageRecommendation(df)
        .transform()
        .withColumn("dataset_id", F.lit(dataset_id))
        .withColumn("insertion_ts", F.lit(insertion_ts))
        .withColumn("snapshot", F.lit(snapshot))
        .sort(F.desc("page_title"))
        .coalesce(num_partitions)
        .write.partitionBy("wiki", "snapshot")
        .mode("overwrite")  # Requires dynamic partitioning enabled
        .parquet(destination)
    )
    spark.stop()

