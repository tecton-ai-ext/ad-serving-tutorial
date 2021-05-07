from tecton import sql_transformation
from feature_repo.shared import entities as e, data_sources
from datetime import datetime

from tecton.transformations.new_transformation import transformation
from tecton.feature_views import Input, batch_feature_view

@batch_feature_view(
    mode='spark_sql',
    inputs={
        'ad_impressions': Input(data_sources.ad_impressions_batch)
    },
    entities=[e.auction_entity],
    ttl='1d',
    batch_schedule='1d',
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 5, 1)
    )
def num_ads_in_auction(ad_impressions):
    return f"""
    SELECT
        auction_id,
        timestamp,
        num_ads_bid,
        num_ads_bid > 1 AS multiple_ads_bid
    FROM
        {ad_impressions}
    """
