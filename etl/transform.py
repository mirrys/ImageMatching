from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StringType, DoubleType, IntegerType
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

import argparse
import sys
import uuid
import datetime

spark = SparkSession.builder.getOrCreate()


class RawDataset:
    schema = (
        StructType()
        .add("pandas_idx", StringType(), True)
        .add("item_id", StringType(), True)
        .add("page_id", StringType(), True)
        .add("page_title", StringType(), True)
        .add("top_candidates", StringType(), True)
        .add("wiki_db", StringType(), True)
        .add("snapshot", StringType(), True)
    )
    recommendation_schema = "array<struct<image:string,note:string,rating:double>>"


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
                    F.from_json("top_candidates", RawDataset.recommendation_schema)
                ),
            )
            .select("*", "data.image", "data.rating", "data.note")
            .withColumnRenamed("wiki_db", "wiki")
            .withColumnRenamed("image", "image_id")
            .withColumn("confidence_rating", self.confidence_rating)
            .withColumn("source", self.source)
            .select(
                "wiki",
                "page_id",
                "page_title",
                "image_id",
                "confidence_rating",
                "source",
            )
        )
        without_recommendations = (
            self.dataFrame.where(F.col("top_candidates").isNull())
            .withColumnRenamed("wiki_db", "wiki")
            .withColumn("image_id", F.lit(None))
            .withColumn("confidence_rating", F.lit(None))
            .withColumn("source", F.lit(None))
            .select(
                "wiki",
                "page_id",
                "page_title",
                "image_id",
                "confidence_rating",
                "source",
            )
        )
        return with_recommendations.union(without_recommendations)

def parse_args():
    parser = argparse.ArgumentParser(description='Transform raw algo output to production datasets')
    parser.add_argument('--snapshot', help='Montlhy snapshot date (YYYY-MM)')
    parser.add_argument('--source', help='Source dataset path')
    parser.add_argument('--destination', help='Destination path')
    parser.add_argument('--dataset-id', help='Production dataset identifier (optional)', default=str(uuid.uuid4()), dest='dataset_id')    
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    snapshot = args.snapshot
    source = args.source
    destination = args.destination
    dataset_id = args.dataset_id

    df = (
        spark.read.options(delimiter="\t", header=False)
        .schema(RawDataset.schema)
        .csv(source)
    )
    insertion_ts = datetime.datetime.now().timestamp()
    (
        ImageRecommendation(df)
        .transform()
        .withColumn("dataset_id", F.lit(dataset_id))
        .withColumn("insertion_ts", F.lit(insertion_ts))
        .withColumn("snapshot", F.lit(snapshot))
        .sort(F.desc("page_title"))
        .write.options(delimiter="\t", header=False)
        .partitionBy("wiki", "snapshot")
        .mode('overwrite') # Requires dynamic partitioning enabled
        .csv(destination)
    )
