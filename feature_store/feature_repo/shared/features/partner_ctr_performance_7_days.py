from tecton import sql_transformation, TemporalFeaturePackage, MaterializationConfig
from feature_repo.shared import entities as e, data_sources
from datetime import datetime
from tecton.feature_views import aggregate_feature_view, feature_view
from tecton.feature_views.feature_view import Input
from tecton.transformations.const import const
from tecton.transformations.new_transformation import transformation

# TODO: remove this when we rename declarative classes
batch_feature_view = feature_view
stream_feature_view = feature_view
batch_window_aggregate_feature_view = aggregate_feature_view
stream_window_aggregate_feature_view = aggregate_feature_view

@transformation(mode="spark_sql")
def partner_ctr_performance_transformer(ad_impressions, days):
    return f"""
    SELECT
        partner_id,
        avg(clicked) OVER (
            partition by partner_id
            order by timestamp
            range between interval {days} days preceding and current row
        ) as avg_clicked_{days}d,
        timestamp
    FROM
        {ad_impressions}
    """

@batch_feature_view(
    mode='pipeline',
    entities=[e.partner_entity],
    online=False,
    offline=True,
    feature_start_time=datetime(2021, 1, 1),
    ttl='1d',
    batch_schedule='1d',
    inputs={
        'ad_impressions': Input(data_sources.ad_impressions_batch)
    },
    family='ad_serving',
    tags={'release': 'development', ':production': 'true'},
    owner="ravi@tecton.ai"
)
def partner_ctr_performance__7d(ad_impressions):
    return partner_ctr_performance_transformer(ad_impressions, const(7))

@batch_feature_view(
    mode='pipeline',
    entities=[e.partner_entity],
    online=False,
    offline=True,
    feature_start_time=datetime(2021, 1, 1),
    ttl='1d',
    batch_schedule='1d',
    inputs={
        'ad_impressions': Input(data_sources.ad_impressions_batch)
    },
    family='ad_serving',
    tags={'release': 'development', ':production': 'true'},
    owner="ravi@tecton.ai"
)
def partner_ctr_performance__14d(ad_impressions):
    return partner_ctr_performance_transformer(ad_impressions, const(14))

@batch_feature_view(
    mode='pipeline',
    entities=[e.partner_entity],
    online=False,
    offline=True,
    feature_start_time=datetime(2021, 1, 1),
    ttl='1d',
    batch_schedule='1d',
    inputs={
        'ad_impressions': Input(data_sources.ad_impressions_batch)
    },
    family='ad_serving',
    tags={'release': 'development', ':production': 'true'},
    owner="ravi@tecton.ai"
)
def partner_ctr_performance__28d(ad_impressions):
    return partner_ctr_performance_transformer(ad_impressions, const(28))
