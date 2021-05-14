import pandas
from tecton import on_demand_feature_view, RequestDataSource
from pyspark.sql.types import LongType, StructType, StructField
from tecton.feature_views.feature_view import Input



request_schema = StructType()
request_schema.add(StructField("ad_display_placement", LongType()))
request_data_source = RequestDataSource(request_schema=request_schema)

output_schema = StructType()
output_schema.add(StructField("ad_is_displayed_as_banner", LongType()))


@on_demand_feature_view(
    output_schema=output_schema,
    mode='pandas',
    inputs={"ad_display_placement": Input (request_data_source)},
    family='ad_serving',
    tags={'release': 'production'},
    owner="ravi@tecton.ai"
)
def ad_is_displayed_as_banner(ad_display_placement: pandas.Series):
    import pandas as pd

    series = []
    for ad_display_type in ad_display_placement:
        series.append({
            "ad_is_displayed_as_banner": 1 if ad_display_type == "Banner" else 0,
        })

    return pd.DataFrame(series)
