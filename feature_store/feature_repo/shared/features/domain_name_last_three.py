from tecton import sql_transformation, TemporalFeaturePackage, MaterializationConfig, FeatureAggregation
from feature_repo.shared import entities as e, data_sources
from datetime import datetime
from pyspark.sql.functions import element_at, split, col


from tecton.transformations.new_transformation import transformation
from tecton.feature_views import batch_window_aggregate_feature_view, Input
from tecton.aggregation_functions import last_distinct

from tecton.transformations.const import const


@batch_window_aggregate_feature_view(
		mode="pyspark",
		entities=[e.ad_entity],
		online=False,
		offline=False,
		inputs={"ad_impressions": Input(data_sources.ad_impressions_batch)},
		feature_start_time=datetime(2021,4,1),
        		aggregation_slide_period="12h",
		aggregations=[FeatureAggregation(column="subdomain", function=last_distinct(3), time_windows=["12h", "24h"])]
)
def domain_name_last_three(ad_impressions):
    split_column = split(ad_impressions["partner_domain_name"], '\.')
    df = ad_impressions.withColumn('subdomain', element_at(split_column,-2)) \
            .withColumn('top_level_domain', element_at(split_column, -1))
    return df.select("ad_id", "timestamp", "subdomain")
