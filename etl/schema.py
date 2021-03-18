from pyspark.sql.types import StructType, StringType, IntegerType


class CsvDataset:
    schema = (
        StructType()
        .add("pandas_idx", StringType(), True)
        .add("item_id", StringType(), True)
        .add("page_id", StringType(), True)
        .add("page_title", StringType(), True)
        .add("top_candidates", StringType(), True)
        .add("instance_of", StringType(), True)
    )


class RawDataset(CsvDataset):
    schema = CsvDataset.schema.add("wiki_db", StringType(), True).add(
        "snapshot", StringType(), True
    )
    recommendation_schema = "array<struct<image:string,note:string,rating:double>>"
