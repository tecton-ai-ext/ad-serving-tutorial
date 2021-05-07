from tecton import sql_transformation
from feature_repo.shared import entities as e, data_sources
from datetime import datetime

from tecton.transformations.new_transformation import transformation
from tecton.feature_views import Input, batch_feature_view
from tecton.transformations.const import const

@transformation(mode="spark_sql")
def str_split(input_data, column_to_split, new_column_name, delimiter):
    return f"""
    SELECT
        *,
        split({column_to_split}, {delimiter}) AS {new_column_name}
    FROM {input_data}
    """

@transformation(mode="spark_sql")
def keyword_stats(input_data, keyword_column):
    return f"""
    SELECT
        auction_id,
        timestamp,
        size({keyword_column}) AS num_keywords,
        length(content_keyword) AS total_keyword_length
    FROM {input_data}
    """

@batch_feature_view(
    mode='pipeline',
    inputs={
        'ad_impressions': Input(data_sources.ad_impressions_batch)
    },
    entities=[e.auction_entity],
    ttl='1d',
    batch_schedule='1d',
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 5, 1)
    )
def auction_keywords(ad_impressions):
    split_keywords = str_split(ad_impressions, const("content_keyword"), const("keywords"), const("\' \'"))
    return keyword_stats(split_keywords, const("keywords"))
