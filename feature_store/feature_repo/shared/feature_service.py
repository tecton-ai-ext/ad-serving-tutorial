from tecton import FeatureService, FeaturesConfig
from feature_repo.shared.features.ad_ground_truth_ctr_performance_7_days import ad_ground_truth_ctr_performance_7_days
from feature_repo.shared.features.user_total_ad_frequency_counts import user_total_ad_frequency_counts
from feature_repo.shared.features.ad_is_displayed_as_banner import ad_is_displayed_as_banner
from feature_repo.shared.features.user_ad_impression_counts import user_ad_impression_counts


# TODO: featureservice doesnt fully support featureviews yet
"""
ctr_prediction_service = FeatureService(
    name='ctr_prediction_service',
    description='A FeatureService used for supporting a CTR prediction model.',
    online_serving_enabled=True,
    features=[
        ad_ground_truth_ctr_performance_7_days,
        user_total_ad_frequency_counts,
        user_ad_impression_counts
    ],
    family='ad_serving',
    tags={'release': 'production'},
    owner="matt@tecton.ai",
)

online_ctr_prediction_service = FeatureService(
    name='online_ctr_prediction_service',
    description='A FeatureService used for supporting a CTR prediction model. Includes an OnlineFeaturePackage.',
    online_serving_enabled=True,
    features=[
        ad_ground_truth_ctr_performance_7_days,
        user_total_ad_frequency_counts,
        user_ad_impression_counts,
        ad_is_displayed_as_banner
    ],
    family='ad_serving',
    tags={'release': 'experimental'},
    owner="matt@tecton.ai",
)
"""
