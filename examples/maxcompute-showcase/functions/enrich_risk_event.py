"""Normalize a nested risk event and produce a deterministic risk profile."""

import json
from datetime import timezone
from decimal import Decimal, InvalidOperation

import jmespath
import phonenumbers
from dateutil import parser as date_parser
from phonenumbers import PhoneNumberFormat
from text_unidecode import unidecode


_EVENT_TYPE = jmespath.compile("event.type")
_AMOUNT = jmespath.compile("event.amount")
_CURRENCY = jmespath.compile("event.currency")
_DEVICE_ID = jmespath.compile("device.id")
_DEVICE_LABEL = jmespath.compile("device.label")
_NETWORK_TOR = jmespath.compile("network.is_tor")
_NETWORK_COUNTRY = jmespath.compile("network.country")
_IDENTITY_NAME = jmespath.compile("identity.name")
_VELOCITY = jmespath.compile("velocity.last_10m")


def _text(value, default=""):
    if value is None:
        return default
    return str(value).strip()


def _decimal(value):
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _timestamp(value):
    parsed = date_parser.isoparse(_text(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _phone(value, default_region):
    parsed = phonenumbers.parse(_text(value), _text(default_region) or None)
    if not phonenumbers.is_valid_number(parsed):
        raise ValueError("invalid phone number")
    return (
        phonenumbers.format_number(parsed, PhoneNumberFormat.E164),
        phonenumbers.region_code_for_number(parsed) or "ZZ",
    )


def main(event_json, phone, event_time, default_region):
    reasons = []
    errors = []
    try:
        payload = json.loads(_text(event_json))
    except (TypeError, ValueError):
        payload = {}
        errors.append("invalid_json")

    event_type = _text(_EVENT_TYPE.search(payload), "unknown").lower()
    amount = _decimal(_AMOUNT.search(payload))
    currency = _text(_CURRENCY.search(payload), "UNKNOWN").upper()
    device_id = _text(_DEVICE_ID.search(payload), "unknown")
    device_label = unidecode(_text(_DEVICE_LABEL.search(payload), "unknown")).lower()
    identity_slug = "_".join(
        unidecode(_text(_IDENTITY_NAME.search(payload), "unknown")).lower().split()
    )
    network_country = _text(_NETWORK_COUNTRY.search(payload), "ZZ").upper()
    is_tor = bool(_NETWORK_TOR.search(payload))
    try:
        velocity = int(_VELOCITY.search(payload) or 0)
    except (TypeError, ValueError):
        velocity = 0
        errors.append("invalid_velocity")

    try:
        event_timestamp = _timestamp(event_time)
        event_time_utc = event_timestamp.isoformat().replace("+00:00", "Z")
        event_hour_utc = event_timestamp.hour
    except (TypeError, ValueError, OverflowError):
        event_time_utc = None
        event_hour_utc = None
        errors.append("invalid_timestamp")

    try:
        phone_e164, phone_region = _phone(phone, default_region)
    except (phonenumbers.NumberParseException, ValueError):
        phone_e164, phone_region = None, "ZZ"
        errors.append("invalid_phone")

    score = Decimal("0")
    if event_type in {"password_reset", "beneficiary_add", "payout"}:
        score += 18
        reasons.append("sensitive_event")
    if amount >= Decimal("1000"):
        score += min(35, int(amount / Decimal("500")))
        reasons.append("large_amount")
    if currency not in {"USD", "EUR", "CNY", "GBP"}:
        score += 18
        reasons.append("unusual_currency")
    if is_tor:
        score += 25
        reasons.append("tor_network")
    if velocity >= 5:
        score += min(20, velocity * 2)
        reasons.append("high_velocity")
    expected_region = _text(default_region, "ZZ").upper()
    if phone_region not in {"ZZ", expected_region}:
        score += 12
        reasons.append("phone_region_mismatch")
    if network_country not in {"ZZ", expected_region}:
        score += 8
        reasons.append("network_country_mismatch")
    if event_hour_utc is not None and event_hour_utc < 5:
        score += 5
        reasons.append("off_hours")
    if errors:
        score += min(30, len(errors) * 10)
        reasons.extend(errors)

    result = {
        "amount": float(amount),
        "currency": currency,
        "device_id": device_id,
        "device_label": device_label,
        "errors": sorted(set(errors)),
        "event_time_utc": event_time_utc,
        "event_type": event_type,
        "identity_slug": identity_slug,
        "network_country": network_country,
        "phone_e164": phone_e164,
        "phone_region": phone_region,
        "reasons": sorted(set(reasons)),
        "risk_score": min(100, int(score)),
        "schema_version": 1,
    }
    return json.dumps(result, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
