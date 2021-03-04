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
                "Some page with suggestions",
                '[{"image": "image1.jpg", "rating": 2.0, "note": "image was found in the following Wikis: ruwiki"}]',
                "arwiki",
                "2020-12",
            ),
            (
                "1",
                "Q56789",
                "55555",
                "Some page with no suggestion",
                None,
                "arwiki",
                "2020-12"
            )
        ],
        RawDataset.schema,
    )
