from pyspark.sql.types import StructType, StringType


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
    top_candidates_schema = "array<struct<image:string,note:string,rating:double>>"
    instance_of_schema = "struct<`entity-type`:string,`numeric-id`:bigint,id:string>"
