from etl.transform import ImageRecommendation

from pyspark.sql import functions as F


def test_etl(raw_data):
    assert raw_data.count() == 3

    ddf = ImageRecommendation(raw_data).transform()
    assert (
        len(
            set(ddf.columns).difference(
                {
                    "wiki",
                    "page_id",
                    "page_title",
                    "image_id",
                    "confidence_rating",
                    "instance_of",
                    "filter_out",
                    "source",
                }
            )
        )
        == 0
    )

    expected_num_records = 5
    assert ddf.count() == expected_num_records

    expected_confidence = {"wikipedia": "medium", "commons": "low", "wikidata": "high"}
    for source in expected_confidence:
        ddf.where(F.col("source") == source).select(
            "confidence_rating"
        ).distinct().collect()
        rows = (
            ddf.where(F.col("source") == source)
            .select("confidence_rating")
            .distinct()
            .collect()
        )
        assert len(rows) == 1
        assert rows[0]["confidence_rating"] == expected_confidence[source]

    # Unillustrated articles with no recommendation have no confidence rating
    assert (
        ddf.where(F.col("source") == "null")
        .where(F.col("confidence_rating") != "null")
        .count()
        == 0
    )

    # Instance_of json is correctly parsed
    expected_instance_of = "Q577"
    rows = (
        ddf.where(F.col("instance_of") != "null")
        .select("instance_of")
        .distinct()
        .collect()
    )
    assert len(rows) == 1
    assert rows[0]["instance_of"] == expected_instance_of

    # Pages are correctly marked for filtering
    expected_page_id = "523523"
    filter_out_rows = (
        ddf.where(F.col("filter_out"))
        .select("page_id")
        .distinct()
        .collect()
    )
    assert len(filter_out_rows) == 1
    assert filter_out_rows[0]["page_id"] == expected_page_id
