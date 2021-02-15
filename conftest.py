import pytest
from etl.transform import RawDataset


@pytest.fixture(scope="session")
def raw_data(spark_session):
    return spark_session.createDataFrame(
        [
            (
                "0",
                "Q1234",
                "44444",
                "Some page",
                '[{"image": "image1.jpg", "rating": 2.0, "note": "image was found in the following Wikis: ruwiki"}]',
                "arwiki",
                "2020-12",
            )
        ],
        RawDataset.schema,
    )
