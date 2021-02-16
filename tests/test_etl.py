from etl.transform import RawDataset, ImageRecommendation


def test_etl(raw_data):
    assert raw_data.count() == 1
    ddf = ImageRecommendation(raw_data).transform()
    assert len(set(ddf.columns).difference({"wiki", "page_id", "page_title", "image_id", "confidence_rating", "source"})) == 0
    expected_num_records = 1
    assert ddf.count() == expected_num_records