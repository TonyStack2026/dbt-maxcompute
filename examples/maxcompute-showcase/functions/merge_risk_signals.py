"""Bounded MaxCompute UDAF for merging row-level risk profiles."""

import json

import jmespath
from dateutil import parser as date_parser


_SCORE = jmespath.compile("risk_score")
_COUNTRY = jmespath.compile("phone_region")
_REASONS = jmespath.compile("reasons")
_EVENT_TIME = jmespath.compile("event_time_utc")


def _increment(counter, key, maximum_keys):
    key = str(key or "unknown")
    if key in counter or len(counter) < maximum_keys:
        counter[key] = counter.get(key, 0) + 1
    else:
        counter["__other__"] = counter.get("__other__", 0) + 1


def _merge_counter(target, source, maximum_keys):
    for key, value in source.items():
        if key in target or len(target) < maximum_keys:
            target[key] = target.get(key, 0) + int(value)
        else:
            target["__other__"] = target.get("__other__", 0) + int(value)


class MergeRiskSignals:
    def new_buffer(self):
        # count, invalid, score_sum, score_max, high_count, earliest, latest,
        # country_counts, reason_counts
        return [0, 0, 0, 0, 0, None, None, {}, {}]

    def iterate(self, buffer, risk_profile):
        buffer[0] += 1
        try:
            payload = json.loads(str(risk_profile or ""))
            score = int(_SCORE.search(payload) or 0)
        except (TypeError, ValueError):
            buffer[1] += 1
            return

        buffer[2] += score
        buffer[3] = max(buffer[3], score)
        if score >= 70:
            buffer[4] += 1

        event_time = _EVENT_TIME.search(payload)
        if event_time:
            try:
                normalized = date_parser.isoparse(str(event_time)).isoformat()
                buffer[5] = normalized if buffer[5] is None else min(buffer[5], normalized)
                buffer[6] = normalized if buffer[6] is None else max(buffer[6], normalized)
            except (TypeError, ValueError, OverflowError):
                buffer[1] += 1

        _increment(buffer[7], _COUNTRY.search(payload), 32)
        for reason in _REASONS.search(payload) or []:
            _increment(buffer[8], reason, 64)

    def merge(self, buffer, partial):
        buffer[0] += partial[0]
        buffer[1] += partial[1]
        buffer[2] += partial[2]
        buffer[3] = max(buffer[3], partial[3])
        buffer[4] += partial[4]
        if partial[5] is not None:
            buffer[5] = partial[5] if buffer[5] is None else min(buffer[5], partial[5])
        if partial[6] is not None:
            buffer[6] = partial[6] if buffer[6] is None else max(buffer[6], partial[6])
        _merge_counter(buffer[7], partial[7], 32)
        _merge_counter(buffer[8], partial[8], 64)

    def terminate(self, buffer):
        valid_count = buffer[0] - buffer[1]
        result = {
            "average_risk": round(buffer[2] / valid_count, 4) if valid_count else None,
            "country_counts": dict(sorted(buffer[7].items())),
            "earliest_event": buffer[5],
            "event_count": buffer[0],
            "high_risk_count": buffer[4],
            "invalid_count": buffer[1],
            "latest_event": buffer[6],
            "max_risk": buffer[3] if valid_count else None,
            "reason_counts": dict(sorted(buffer[8].items())),
            "schema_version": 1,
        }
        return json.dumps(result, separators=(",", ":"), sort_keys=True)
