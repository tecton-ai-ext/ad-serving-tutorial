from datetime import datetime
from tecton import TemporalAggregateFeaturePackage, FeatureAggregation, sql_transformation, MaterializationConfig
from feature_repo.shared import data_sources, entities
from tecton.feature_views import stream_window_aggregate_feature_view, batch_window_aggregate_feature_view
from tecton.feature_views.feature_view import Input


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
    online=False,
    offline=False,
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

@batch_window_aggregate_feature_view(
    mode="spark_sql",
    inputs={
        "ad_impressions": Input(data_sources.ad_impressions_batch)
    },
    entities=[entities.ad_group_entity.with_join_keys('ad_group_id')],
    aggregation_slide_period='1h',
    aggregations=[
        FeatureAggregation(column='impression', function='count', time_windows=['1h', '12h', '24h', '72h', '168h']),
        FeatureAggregation(column='clicked', function='sum', time_windows=['1h', '12h', '24h', '72h', '168h'])
    ],
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 1, 5),
    batch_schedule='1d',
    family='ad_serving',
    tags={'release': 'development'},
    owner="mike@tecton.ai",
    name_override="ad_group_ctr_performance:batch"
)
def ad_group_ctr_performance_batch(ad_impressions):
    return f"""
        select
            ad_group_id,
            clicked,
            1 as impression,
            timestamp
        from
            {ad_impressions}
        """
