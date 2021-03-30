from tecton import TemporalFeaturePackage, pyspark_transformation, MaterializationConfig
from feature_repo.shared import data_sources, entities
from datetime import datetime

from tecton.feature_views import feature_view, aggregate_feature_view
from tecton.feature_views.feature_view import Input

# TODO: remove this when we rename declarative classes
batch_feature_view = feature_view
stream_feature_view = feature_view
batch_window_aggregate_feature_view = aggregate_feature_view
stream_window_aggregate_feature_view = aggregate_feature_view

@batch_feature_view(
    mode='pyspark',
    inputs={
        'ad_impressions': Input(data_sources.ad_impressions_batch)
    },
    entities=[entities.ad_entity],
    batch_schedule='1d',
    ttl='1d',
    online=True,
    offline=True,
    feature_start_time=datetime(2021,1,6),
    family='ad_serving',
    tags={'release': 'development'},
    owner='bot@tecton.ai',
    description='Impression Features'
)
def ad_impression_count_monthly(ad_impressions):
    import pyspark.sql.functions as F
    truncated_date_view = ad_impressions.withColumn('timestamp', F.date_trunc('day', F.col('timestamp')))
    return truncated_date_view.groupBy('ad_id', 'timestamp').agg(F.count(F.lit(1)).alias("ad_impression_count"))

