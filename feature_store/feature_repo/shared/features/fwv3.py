from datetime import datetime
from tecton import TemporalAggregateFeaturePackage, FeatureAggregation, sql_transformation, MaterializationConfig
from feature_repo.shared import data_sources, entities
from pyspark.sql.types import StructField, StructType, LongType, StringType, DoubleType, BooleanType, TimestampType
from tecton.transformations.new_transformation import transformation
from tecton.feature_views.feature_view import feature_view, aggregate_feature_view, Input
from tecton.transformations.const import const
@transformation(
    mode="spark_sql",
    family='ad_serving',
    tags={'release': 'staging'},
    owner="bot@tecton.ai",
    description="My transformation"
)
def impression_view(impressions, non_impressions, num):
    return f"""
        select
            user_uuid,
            {num} as impression,
            timestamp
        from
            {impressions}
        """
@transformation(mode="pyspark")
def limiter(data, value):
    return data.limit(value + 100)
@feature_view(
    mode="pipeline",
    inputs={
        "impressions": Input(data_sources.ad_impressions_stream),
        "non_impressions": Input(data_sources.ad_impressions_stream)
    },
    entities=[entities.user_entity],
    ttl="1d",
    online=True,
    offline=False,
    batch_schedule="1d",
    feature_start_time=datetime(2021, 1, 5),
    family='ad_serving',
    tags={'release': 'staging'},
    owner="bot@tecton.ai",
    description="Impression features"
)
def user_ad_impressions(non_impressions, impressions):
    view = impression_view(impressions, non_impressions, const(5))
    return limiter(view, value=const(1000))
@aggregate_feature_view(
    mode="pipeline",
    inputs={
        "impressions": Input(data_sources.ad_impressions_stream),
        "non_impressions": Input(data_sources.ad_impressions_stream)
    },
    entities=[entities.user_entity],
    aggregation_slide_period="1h",
    aggregations=[FeatureAggregation(column="impression", function="count", time_windows=["1h", "12h", "24h"])],
    online=True,
    offline=False,
    batch_schedule="1d",
    feature_start_time=datetime(2021, 1, 5),
    family='ad_serving',
    tags={'release': 'staging'},
    owner="bot@tecton.ai",
    description="My features [offline disabled]"
)
def user_ad_impression_counts(non_impressions, impressions):
    view = impression_view(impressions, non_impressions, const(5))
    return limiter(view, value=const(1000))

