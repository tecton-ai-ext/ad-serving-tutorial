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

# TODO: figure out if you can do rangebetween with dates in pyspark

"""
@transformation(mode="pyspark")
def user_ad_group_impression_count_7_days_transformer(ad_impressions):
    from pyspark.sql.window import Window
    import pyspark.sql.functions as F
    windowSpec = (
        Window
            .partitionBy(ad_impressions['user_uuid', 'ad_group_id'])
            .orderBy(ad_impressions['timestamp'])
            .rangeBetween(-sys.maxsize, sys.maxsize))

    user_website_views = ad_impressions.groupBy("user_uuid", "ad_group_id").agg(
        F.count(F.col("*")).alias("user_ad_group_impressions_7_days"))
    user_website_views = user_website_views.withColumn("timestamp",
                                                       F.to_timestamp(F.lit(context.feature_data_end_time)))
    return user_website_views


user_ad_group_impression_count_7_days = TemporalFeaturePackage(
    name="user_ad_group_impression_count_7_days",
    description="[Pyspark Feature] The number of ads a user has been shown from a given ad group site over the past 7 days",
    transformation=user_ad_group_impression_count_7_days_transformer,
    entities=[e.user_entity, e.ad_group_entity],
    materialization=MaterializationConfig(
        offline_enabled=True,
        online_enabled=True,
        feature_start_time=datetime(year=2020, month=6, day=20),
        serving_ttl="1d",
        schedule_interval="1d",
        data_lookback_period="7d"
    ),
    family='ad_serving',
    tags={'release': 'production'},
    owner="jaye@tecton.ai"
)
"""
