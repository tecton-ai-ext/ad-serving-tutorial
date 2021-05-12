def test_user_ad_group_impression_count_7_days_transformer(spark_session):
    from feature_repo.shared.features.user_ad_group_impression_count_7_days import user_ad_group_impression_count_7_days_transformer
    assert user_ad_group_impression_count_7_days_transformer is not None

    from tecton_spark.materialization_common import MaterializationContext
    context = MaterializationContext()

    import pendulum
    context.feature_data_end_time = pendulum.now()

    l = [

        ("id1", "ad_id_1", "2020-10-28 05:02:11", True),
        ("id2", "ad_id_1", "2020-10-28 05:02:11", False),

    ]
    input_df = spark_session.createDataFrame(l, ["user_uuid", "ad_group_id", "timestamp", "clicked"])

    output = user_ad_group_impression_count_7_days_transformer.transformer(context, input_df).collect()
    print(f"Output: {output}")
    assert len(output) == 2