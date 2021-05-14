import pandas
from tecton import RequestContext, online_transformation, on_demand_feature_view, RequestDataSource
from pyspark.sql.types import LongType, StructType, StructField
from tecton.feature_views.feature_view import Input
from tecton.transformations.const import const
from tecton.transformations.new_transformation import transformation


request_schema = StructType()
request_schema.add(StructField("ad_display_placement", LongType()))
request_data_source = RequestDataSource(request_schema=request_schema)

output_schema = StructType()
output_schema.add(StructField("ad_is_displayed_as_banner", LongType()))
# TODO(fwv3): this isnt the final form


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
