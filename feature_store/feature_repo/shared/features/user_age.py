from tecton import sql_transformation, TemporalFeaturePackage, DataSourceConfig, MaterializationConfig
from feature_repo.shared import entities as e, data_sources
from datetime import datetime


# @sql_transformation(inputs=data_sources.user_info, has_context=True)
# def user_age_years_sql_transform(context, table_name):
#     return f"""
#         select
#             user_uuid,
#             CAST(FLOOR(datediff(to_date('{context.feature_data_end_time}'), dob) / 365.25) as INT) AS age,
#             to_timestamp('{context.feature_data_end_time}') as timestamp
#         from
#             {table_name}
#         """
#
#
# user_age = TemporalFeaturePackage(
#     name="user_age_years",
#     description="Age of a user in years",
#     transformation=user_age_years_sql_transform,
#     entities=[e.user_entity],
#
#     materialization=MaterializationConfig(
#         schedule_interval='30day',
#         online_enabled=True,
#         offline_enabled=True,
#         feature_start_time=datetime(2020, 6, 19),
#         serving_ttl='60days',
#     ),
#
#     family='ad_serving',
#     tags={'release': 'production'},
#     owner="david@tecton.ai",
# )
