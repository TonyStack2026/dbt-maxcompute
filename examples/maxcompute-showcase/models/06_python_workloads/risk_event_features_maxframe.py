import json
from datetime import timezone
from decimal import Decimal, InvalidOperation

import maxframe.dataframe as md
import pandas as pd
from maxframe.udf import with_python_requirements


@with_python_requirements(
    "jmespath==1.1.0",
    "python-dateutil==2.9.0.post0",
    "phonenumbers==9.0.36",
    "text-unidecode==1.3",
    prefer_binary=True,
)
def enrich_event_row(row):
    import jmespath
    import phonenumbers
    from dateutil import parser as date_parser
    from phonenumbers import PhoneNumberFormat
    from text_unidecode import unidecode

    try:
        payload = json.loads(str(row["event_json"] or ""))
        invalid_json = 0
    except (TypeError, ValueError):
        payload = {}
        invalid_json = 1

    def search(expression, default=None):
        value = jmespath.search(expression, payload)
        return default if value is None else value

    try:
        amount = float(Decimal(str(search("event.amount", 0))))
    except (InvalidOperation, TypeError, ValueError):
        amount = 0.0

    try:
        parsed_time = date_parser.isoparse(str(row["event_time"]))
        if parsed_time.tzinfo is None:
            parsed_time = parsed_time.replace(tzinfo=timezone.utc)
        event_time_utc = parsed_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        invalid_timestamp = 0
    except (TypeError, ValueError, OverflowError):
        event_time_utc = ""
        invalid_timestamp = 1

    try:
        parsed_phone = phonenumbers.parse(str(row["phone"]), str(row["default_region"]))
        if not phonenumbers.is_valid_number(parsed_phone):
            raise ValueError("invalid phone")
        phone_e164 = phonenumbers.format_number(parsed_phone, PhoneNumberFormat.E164)
        phone_region = phonenumbers.region_code_for_number(parsed_phone) or "ZZ"
        invalid_phone = 0
    except (phonenumbers.NumberParseException, TypeError, ValueError):
        phone_e164 = ""
        phone_region = "ZZ"
        invalid_phone = 1

    event_type = str(search("event.type", "unknown")).lower()
    currency = str(search("event.currency", "UNKNOWN")).upper()
    device_id = str(search("device.id", "unknown"))
    identity_slug = "_".join(unidecode(str(search("identity.name", "unknown"))).lower().split())
    is_tor = bool(search("network.is_tor", False))
    network_country = str(search("network.country", "ZZ")).upper()
    try:
        velocity = int(search("velocity.last_10m", 0))
    except (TypeError, ValueError):
        velocity = 0

    risk_score = invalid_json * 20 + invalid_timestamp * 10 + invalid_phone * 10
    risk_score += 25 if is_tor else 0
    risk_score += 18 if event_type in {"password_reset", "beneficiary_add", "payout"} else 0
    risk_score += min(35, int(amount / 500)) if amount >= 1000 else 0
    risk_score += min(20, velocity * 2) if velocity >= 5 else 0
    risk_score += 18 if currency not in {"USD", "EUR", "CNY", "GBP"} else 0
    risk_score += 12 if phone_region not in {"ZZ", str(row["default_region"])} else 0
    risk_score += 8 if network_country not in {"ZZ", str(row["default_region"])} else 0

    return pd.Series(
        {
            "event_id": str(row["event_id"]),
            "customer_id": str(row["customer_id"]),
            "event_type": event_type,
            "amount": amount,
            "currency": currency,
            "device_id": device_id,
            "identity_slug": identity_slug,
            "phone_e164": phone_e164,
            "phone_region": phone_region,
            "event_time_utc": event_time_utc,
            "base_risk_score": min(100, int(risk_score)),
            "invalid_payload": int(invalid_json or invalid_timestamp),
            "ds": str(row["ds"]),
        }
    )


def model(dbt, session):
    dbt.config(
        materialized="table",
        submission_method="maxframe",
        partition_by={"field": "ds", "data_type": "string"},
        lifecycle=1,
        timeout=3600,
        maxframe_retries=2,
        tags=["complex_python", "maxframe_dependency"],
    )

    events = dbt.ref("risk_events")
    customers = dbt.ref("risk_customers")
    devices = dbt.ref("device_reputation")

    output_dtypes = pd.Series(
        {
            "event_id": md.dtype("string"),
            "customer_id": md.dtype("string"),
            "event_type": md.dtype("string"),
            "amount": md.dtype("float64"),
            "currency": md.dtype("string"),
            "device_id": md.dtype("string"),
            "identity_slug": md.dtype("string"),
            "phone_e164": md.dtype("string"),
            "phone_region": md.dtype("string"),
            "event_time_utc": md.dtype("string"),
            "base_risk_score": md.dtype("int64"),
            "invalid_payload": md.dtype("int64"),
            "ds": md.dtype("string"),
        }
    )
    enriched = events.apply(
        enrich_event_row,
        axis=1,
        result_type="expand",
        output_type="dataframe",
        dtypes=output_dtypes,
    )

    customer_columns = customers[
        ["customer_id", "customer_name", "tier", "country_code", "credit_limit"]
    ]
    device_columns = devices[["device_id", "reputation_score", "is_tor"]]
    joined = enriched.merge(customer_columns, on="customer_id", how="left")
    joined = joined.merge(device_columns, on="device_id", how="left")

    joined["reputation_score"] = joined["reputation_score"].fillna(0).astype("int64")
    joined["device_is_tor"] = joined["is_tor"].fillna(False).astype("int64")
    joined["phone_country_mismatch"] = (joined["phone_region"] != joined["country_code"]).astype(
        "int64"
    )
    joined["amount_to_limit"] = joined["amount"] / joined["credit_limit"].astype("float64")
    joined["risk_score"] = (
        joined["base_risk_score"]
        + (100 - joined["reputation_score"]) * 0.30
        + joined["device_is_tor"] * 15
        + joined["phone_country_mismatch"] * 8
    )

    return joined[
        [
            "event_id",
            "customer_id",
            "customer_name",
            "tier",
            "country_code",
            "event_type",
            "amount",
            "currency",
            "amount_to_limit",
            "device_id",
            "reputation_score",
            "device_is_tor",
            "identity_slug",
            "phone_e164",
            "phone_region",
            "phone_country_mismatch",
            "event_time_utc",
            "invalid_payload",
            "risk_score",
            "ds",
        ]
    ]
