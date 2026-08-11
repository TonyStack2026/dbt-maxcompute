def model(dbt, session):
    dbt.config(
        materialized="incremental",
        submission_method="maxframe",
        incremental_strategy="merge",
        unique_key=["customer_id", "ds"],
        partition_by={"field": "ds", "data_type": "string"},
        on_schema_change="sync_all_columns",
        lifecycle=1,
        timeout=3600,
        maxframe_retries=2,
        tags=["complex_python", "maxframe_incremental"],
    )

    events = dbt.ref("risk_event_features_maxframe")
    if dbt.is_incremental:
        # Fixed replay window for the deterministic showcase data. A real
        # pipeline should derive this from its source watermark policy.
        events = events[events["ds"] >= "2026-08-09"]

    keys = ["customer_id", "ds"]
    totals = (
        events.groupby(keys)
        .agg(
            {
                "event_id": "count",
                "amount": "sum",
                "risk_score": "sum",
                "invalid_payload": "sum",
                "phone_country_mismatch": "sum",
            }
        )
        .reset_index()
        .rename(
            columns={
                "event_id": "event_count",
                "amount": "total_amount",
                "risk_score": "risk_score_sum",
                "invalid_payload": "invalid_payload_count",
                "phone_country_mismatch": "phone_mismatch_count",
            }
        )
    )
    maximums = (
        events.groupby(keys)[["risk_score"]]
        .max()
        .reset_index()
        .rename(columns={"risk_score": "max_risk_score"})
    )
    device_counts = (
        events.groupby(keys)[["device_id"]]
        .nunique()
        .reset_index()
        .rename(columns={"device_id": "distinct_device_count"})
    )

    result = totals.merge(maximums, on=keys, how="left")
    result = result.merge(device_counts, on=keys, how="left")
    result["average_risk_score"] = result["risk_score_sum"] / result["event_count"]
    result["high_risk_customer"] = (result["max_risk_score"] >= 70).astype("boolean")

    customers = dbt.ref("risk_customers")[["customer_id", "tier", "country_code"]]
    result = result.merge(customers, on="customer_id", how="left")
    return result[
        [
            "customer_id",
            "tier",
            "country_code",
            "event_count",
            "distinct_device_count",
            "total_amount",
            "risk_score_sum",
            "average_risk_score",
            "max_risk_score",
            "invalid_payload_count",
            "phone_mismatch_count",
            "high_risk_customer",
            "ds",
        ]
    ]
