from datetime import datetime
from tecton import TemporalAggregateFeaturePackage, FeatureAggregation, sql_transformation, MaterializationConfig
from feature_repo.shared import data_sources, entities
from tecton.feature_views import aggregate_feature_view, feature_view
from tecton.feature_views.feature_view import Input

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
    entities=[entities.ad_group_entity.with_join_keys('ad_group_id')],
    aggregation_slide_period='1h',
    aggregations=[
        FeatureAggregation(column='impression', function='count', time_windows=['1h', '12h', '24h', '72h', '168h']),
        FeatureAggregation(column='clicked', function='sum', time_windows=['1h', '12h', '24h', '72h', '168h'])
    ],
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 5),
    batch_schedule='1d',
    family='ad_serving',
    tags={'release': 'development'},
    owner="mike@tecton.ai"
)
def ad_group_ctr_performance(ad_impressions):
    return f"""
        select
            ad_group_id,
            clicked,
            1 as impression,
            timestamp
        from
            {ad_impressions}
        """

