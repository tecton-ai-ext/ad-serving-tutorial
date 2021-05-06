from tecton import TemporalFeaturePackage, pyspark_transformation, MaterializationConfig
from feature_repo.shared import data_sources, entities
from datetime import datetime

from tecton.feature_views import batch_feature_view
from tecton.feature_views.feature_view import Input

@batch_feature_view(
    mode='pyspark',
    inputs={
        'ad_impressions': Input(data_sources.ad_impressions_batch, window='30d')
    },
    entities=[entities.ad_entity],
    batch_schedule='1d',
    ttl='1d',
    online=False,
    offline=False,
    feature_start_time=datetime(2021,1,6),
    family='ad_serving',
    tags={'release': 'development'},
    owner='bot@tecton.ai',
    description='Impression Features'
)
def ad_impression_count_monthly(ad_impressions):
    import pyspark.sql.functions as F
    window_spec = F.window('timestamp', '30 days', '1 day')
    df = ad_impressions.withColumn('timestamp', F.date_trunc('day', F.col('timestamp')))
    df = df.groupBy('ad_id', window_spec).agg(F.count(F.lit(1)).alias("ad_impression_count"))
    return df.select(df.window.end.cast('timestamp').alias('timestamp'), 'ad_id', 'ad_impression_count')

