from tecton import (
    VirtualDataSource,
    FileDSConfig,
    HiveDSConfig,
    KinesisDSConfig
)

from tecton_spark.function_serialization import inlined


events_config = FileDSConfig(
        uri='s3://tecton.ai.public/ad-impressions-data/ctr_events.pq',
        file_format="parquet"
)

events_vds = VirtualDataSource(
        name='sample_events_for_model',
        batch_ds_config=events_config,
        family='ad_serving',
        tags={
            'release': 'production'
        }
)
