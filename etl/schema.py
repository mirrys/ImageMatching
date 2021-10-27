from pyspark.sql.types import StructType, StringType, ArrayType, MapType, LongType


class ParquetDataset:
    schema = (
        StructType()
        .add("item_id", StringType(), True)
        .add("page_id", LongType(), True)
        .add("page_title", StringType(), True)
        .add("top_candidates", ArrayType(MapType(StringType(), StringType(), True), True), True)
        .add("instance_of", StringType(), True)
        .add("wiki_db", StringType(), True)
        .add("snapshot", StringType(), True)
    )


class RawDataset(ParquetDataset):
    instance_of_schema = "struct<`entity-type`:string,`numeric-id`:bigint,id:string>"
