from tecton import pyspark_transformation, TemporalFeaturePackage, MaterializationConfig
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

@batch_feature_view(
    mode="pyspark",
    description="[Pyspark Feature] The number of ads a user has been shown from a given ad group site over the past 7 days",
    entities=[e.user_entity, e.ad_group_entity],
    inputs={
        "ad_impressions": Input(data_sources.ad_impressions_batch, window='7 days')
    },
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 1, 1),
    ttl='1d',
    batch_schedule='1d',
    family='ad_serving',
    tags={'release': 'production'},
    owner="jaye@tecton.ai"
)
def user_ad_group_impression_count_7_days(ad_impressions):
    import pyspark.sql.functions as F
    window_spec = F.window('timestamp', '7 days', '1 day')

    user_website_views = ad_impressions.groupBy("user_uuid", "ad_group_id", window_spec).agg(
        F.count(F.col("*")).alias("user_ad_group_impressions_7_days"))
    return user_website_views.select('user_ad_group_impressions_7_days', 'user_uuid', 'ad_group_id', user_website_views.window.end.alias('timestamp').cast('timestamp'))
