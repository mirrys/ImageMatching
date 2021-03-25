import pytest
from etl.transform import RawDataset
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def raw_data(spark_session):
    return spark_session.createDataFrame(
        [
            (
                "0",
                "Q1234",
                "44444",
                "Some page with suggestions",
                '[{"image": "image1.jpg", "rating": 2.0, "note": "image was found in the following Wikis: ruwiki"}]',
                None,
                "arwiki",
                "2020-12",
            ),
            (
                "1",
                "Q56789",
                "55555",
                "Some page with no suggestion",
                None,
                None,
                "arwiki",
                "2020-12",
            ),
            (
                "2",
                "Q66666",
                "523523",
                "Some page with 3 suggestions",
                "["
                '{"image": "image2.jpg", "rating": 2.0, "note": "image was found in the following Wikis: ruwiki,arwiki,enwiki"}, '
                '{"image": "image3.jpg", "rating": 1, "note": "image was in the Wikidata item"}, '
                '{"image": "image4.jpg", "rating": 3.0, "note": "image was found in the Commons category linked in '
                'the Wikidata item"} '
                "]",
                '{"entity-type":"item","numeric-id":577,"id":"Q577"}',
                "enwiki",
                "2020-12",
            ),
        ],
        RawDataset.schema,
    )


@pytest.fixture(scope="session")
def wikis(spark_session: SparkSession) -> DataFrame:
    return spark_session.createDataFrame(
        [
            [
                "image was found in the following Wikis: ruwiki, itwiki,enwiki"
            ],
            ["image was found in the following Wikis: "],
            [None],
        ],
        ["note"],
    )


def assert_shallow_equals(ddf: DataFrame, other_ddf: DataFrame) -> None:
    assert len(set(ddf.columns).difference(set(other_ddf.columns))) == 0
    assert ddf.subtract(other_ddf).rdd.isEmpty()
    assert other_ddf.subtract(ddf).rdd.isEmpty()
