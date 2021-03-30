from datetime import datetime
from tecton import TemporalAggregateFeaturePackage, FeatureAggregation, sql_transformation, MaterializationConfig
from feature_repo.shared import data_sources, entities
from tecton.feature_views import aggregate_feature_view, feature_view
from tecton.transformations.new_transformation import transformation
from tecton.feature_views.feature_view import Input

# TODO: remove this when we rename declarative classes
batch_feature_view = feature_view
stream_feature_view = feature_view
batch_window_aggregate_feature_view = aggregate_feature_view
stream_window_aggregate_feature_view = aggregate_feature_view

@transformation(mode='spark_sql')
def content_keyword_ctr_performance_transformer(ad_impressions):
    return f"""
        select
            content_keyword,
            clicked,
            1 as impression,
            timestamp
        from
            {ad_impressions}
        """


@stream_window_aggregate_feature_view(
    mode='pipeline',
    entities=[entities.content_keyword_entity],
    inputs={
        "ad_impressions": Input(data_sources.ad_impressions_stream)
    },
    aggregation_slide_period='1h',
    aggregations=[
        FeatureAggregation(column="impression", function="count", time_windows=["1h", "12h", "24h","72h","168h"]),
        FeatureAggregation(column="clicked", function="sum", time_windows=["1h", "12h", "24h","72h","168h"])
    ],
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 6),
    family='ad_serving',
    tags={
        'release': 'development'
    },
    owner='matt@tecton.ai'
)
def content_keyword_ctr_performance(ad_impressions):
    return content_keyword_ctr_performance_transformer(ad_impressions)

@stream_window_aggregate_feature_view(
    mode='pipeline',
    entities=[entities.content_keyword_entity],
    inputs={
        "ad_impressions": Input(data_sources.ad_impressions_stream)
    },
    aggregation_slide_period='1h',
    aggregations=[
        FeatureAggregation(column="impression", function="count", time_windows=["1h", "3h", "12h", "24h","72h","168h"]),
        FeatureAggregation(column="clicked", function="sum", time_windows=["1h", "3h", "12h", "24h","72h","168h"])
    ],
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 6),
    family='ad_serving',
    tags={
        'release': 'development'
    },
    owner='matt@tecton.ai'
)
def content_keyword_ctr_performance__v2(ad_impressions):
    return content_keyword_ctr_performance_transformer(ad_impressions)
