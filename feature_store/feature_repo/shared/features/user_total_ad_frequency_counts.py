from datetime import datetime
from tecton import TemporalAggregateFeaturePackage, FeatureAggregation, sql_transformation, MaterializationConfig
from feature_repo.shared import data_sources, entities
from tecton.feature_views import aggregate_feature_view, feature_view
from tecton.feature_views.feature_view import Input
from tecton.transformations.const import const
from tecton.transformations.new_transformation import transformation

# TODO: remove this when we rename declarative classes
batch_feature_view = feature_view
stream_feature_view = feature_view
batch_window_aggregate_feature_view = aggregate_feature_view
stream_window_aggregate_feature_view = aggregate_feature_view

@stream_window_aggregate_feature_view(
    mode="spark_sql",
    inputs={
        "ad_impressions": Input(data_sources.ad_impressions_stream)
    },
    entities=[entities.user_entity],
    aggregation_slide_period='1h',
    aggregations=[
        FeatureAggregation(column='impression', function='count', time_windows=['1h', '12h', '24h', '72h', '168h']),
    ],
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 5),
    family='ad_serving',
    tags={'release': 'production'},
    owner="matt@tecton.ai"
)
def user_total_ad_frequency_counts(ad_impressions):
    return f"""
        select
            user_uuid,
            1 as impression,
            timestamp
        from
            {ad_impressions}
        """
