from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StringType, DoubleType, IntegerType
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

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
        F.when(F.col("rating").cast(IntegerType()) == 1, F.lit("low"))
        .when(F.col("rating").cast(IntegerType()) == 2, F.lit("medium"))
        .when(F.col("rating").cast(IntegerType()) == 3, F.lit("high"))
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


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(
            """Usage: spark-submit transform.py <snapshot> <source csv file> <destination csv file>"""
        )
        sys.exit(1)
    snapshot = sys.argv[1]
    source = sys.argv[2]
    destination = sys.argv[3]
    df = (
        spark.read.options(delimiter="\t", header=False)
        .schema(RawDataset.schema)
        .csv(source)
    )
    dataset_id = str(uuid.uuid4())
    insertion_ts = datetime.datetime.now().timestamp()
    (
        ImageRecommendation(df)
        .transform()
        .withColumn("dataset_id", F.lit(dataset_id))
        .withColumn("insertion_ts", F.lit(insertion_ts))
        .sort(F.desc("page_title"))
        .write.options(delimiter="\t", header=False)
        .partitionBy("wiki")
        .csv(destination)
    )
