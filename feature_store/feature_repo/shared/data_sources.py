from tecton import (
    VirtualDataSource,
    FileDSConfig,
    HiveDSConfig,
    KinesisDSConfig
)

from tecton_spark.function_serialization import inlined

@inlined
def ad_stream_translator(df):
    from pyspark.sql.types import StructType, StringType
    from pyspark.sql.functions import from_json, col, from_utc_timestamp, when

    payload_schema = (
        StructType()
        .add("clicked", StringType(), False)
        .add("auction_id", StringType(), False)
        .add("num_ads_bid", StringType(), False)
        .add("ad_id", StringType(), False)
        .add("ad_campaign_id", StringType(), False)
        .add("partner_domain_name", StringType(), False)
        .add("content_keyword", StringType(), False)
        .add("ad_content_id", StringType(), False)
        .add("ad_group_id", StringType(), False)
        .add("ad_display_placement", StringType(), False)
        .add("ad_destination_domain_id", StringType(), False)
        .add("partner_id", StringType(), False)
        .add("is_pwa", StringType(), False)
        .add("user_uuid", StringType(), False)
        .add("timestamp", StringType(), False)
        .add("datestr", StringType(), True)
    )

    return (
      df.selectExpr("cast (data as STRING) jsonData")
      .select(from_json("jsonData", payload_schema).alias("payload"))
      .select(
          col("payload.clicked").cast("long").alias("clicked"),
          col("payload.auction_id").alias("auction_id"),
          col("payload.num_ads_bid").cast("long").alias("num_ads_bid"),
          col("payload.ad_id").cast("long").alias("ad_id"),
          col("payload.ad_campaign_id").cast("long").alias("ad_campaign_id"),
          col("payload.partner_domain_name").alias("partner_domain_name"),
          col("payload.content_keyword").alias("content_keyword"),
          col("payload.ad_content_id").cast("long").alias("ad_content_id"),
          col("payload.ad_group_id").alias("ad_group_id"),
          col("payload.ad_display_placement").alias("ad_display_placement"),
          col("payload.ad_destination_domain_id").cast("long").alias("ad_destination_domain_id"),
          col("payload.partner_id").cast("long").alias("partner_id"),
          when(
            col("payload.is_pwa") == "True",
            True).when(
            col("payload.is_pwa") == "False",
            False).alias("is_pwa"),          
          col("payload.user_uuid").alias("user_uuid"),
          from_utc_timestamp("payload.timestamp", "UTC").alias("timestamp"),
          col("payload.datestr").alias("datestr")
      )
    )

ad_impressions_hive = HiveDSConfig(
    database='demo_ad_impressions',
    table='batch_events',
    timestamp_column_name='timestamp',
    date_partition_column='datestr'
)

ad_impressions_kinesis = KinesisDSConfig(
    stream_name='ad-impressions-2',
    region='us-west-2',
    raw_stream_translator=ad_stream_translator,
    timestamp_key='timestamp',
    default_watermark_delay_threshold="1minutes",
    default_initial_stream_position="trim_horizon",
    deduplication_columns=[],
    options={'roleArn': 'arn:aws:iam::472542229217:role/demo-cross-account-kinesis-ro'}
)

ad_impressions_stream = VirtualDataSource(name="ad_impressions_stream", 
    batch_ds_config=ad_impressions_hive, 
    stream_ds_config=ad_impressions_kinesis,
    family='ad_serving',
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)
ad_impressions_batch = VirtualDataSource(
    name="ad_impressions_batch", 
    batch_ds_config=ad_impressions_hive,
    family='ad_serving',
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)

events_config = FileDSConfig(
        uri='s3://ad-impressions-data/ctr_events.pq',
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

ad_users_batch = VirtualDataSource(
    name="ad_users_batch",
    batch_ds_config= FileDSConfig(
        uri='s3://jack-datasets/demos/ad-serving/user_age.parquet',
        file_format='parquet'
    ),
    family='ad_serving',
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)