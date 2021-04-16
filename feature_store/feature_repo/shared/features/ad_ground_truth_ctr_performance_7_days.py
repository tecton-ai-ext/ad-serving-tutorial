from tecton import sql_transformation, TemporalFeaturePackage, MaterializationConfig
from feature_repo.shared import entities as e, data_sources
from datetime import datetime

from tecton.transformations.new_transformation import transformation
from tecton.feature_views import Input, batch_feature_view

@batch_feature_view(
    mode='spark_sql',
    inputs={
        'ad_impressions': Input(data_sources.ad_impressions_batch, window='7d')
    },
    entities=[e.ad_entity],
    ttl='1d',
    batch_schedule='1d',
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 5),
    family='ad_serving',
    tags={'release': 'production'},
    owner='bot@tecton.ai',
    description="[SQL Feature] The aggregate CTR of a partner website (clicks / total impressions) over the last 7 days")
def ad_ground_truth_ctr_performance_7_days(ad_impressions):
    return f"""
    SELECT
        ad_id,
        sum(clicked),
        count(1),
        window(timestamp, "7 days", "1 day").end as timestamp
    FROM
        {ad_impressions}
    GROUP BY ad_id, window(timestamp, "7 days", "1 day")
    """
