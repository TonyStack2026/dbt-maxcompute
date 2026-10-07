"""Rendering a microbatch window so that it reads the same clock as `trunc_time()`.

The window is compared in SQL, so the interesting behaviour is what string comes out of
`MaxComputeRelation._render_event_time_filtered`. The numbers below are the ones a real project
returns: MaxCompute parses a timestamp string on the session timezone, so the string written for
a UTC boundary is that boundary's *local reading* in the session's own zone.

Pinned separately because the two paths fail differently:

* the default profile (`timezone` omitted, so pyodps submits `odps.sql.timezone=Etc/GMT`) must keep
  rendering byte-for-byte what it rendered before this existed - that path is correct today, and a
  regression there would break working pipelines;
* a session that is not UTC has to be shifted, or a day window reaches into its neighbour's
  partition and the later `insert overwrite` erases the earlier batch's rows.

When the client cannot know the session clock - no pinned timezone, or a name the host cannot
resolve - the rendering is left alone on purpose: an unverified shift is worse than the known
behaviour, and `docs/microbatch-support.md` states what that means for a microbatch model.
"""

from datetime import datetime, timezone

import pytest
from dbt.adapters.base.relation import EventTimeFilter
from odps import options

from dbt.adapters.maxcompute import session_clock
from dbt.adapters.maxcompute.relation import MaxComputeRelation

START = datetime(2025, 5, 1, tzinfo=timezone.utc)
END = datetime(2025, 5, 2, tzinfo=timezone.utc)


@pytest.fixture
def session_tz(request):
    """Set pyodps' global the same way `MaxComputeCredentials._get_odps` does, then restore it."""
    saved = options.local_timezone
    options.local_timezone = request.param
    yield
    options.local_timezone = saved


def relation(start=START, end=END, field="event_time"):
    return MaxComputeRelation.create(
        database="p",
        schema="s",
        identifier="t",
        event_time_filter=EventTimeFilter(field_name=field, start=start, end=end),
    )


def rendered(**kwargs):
    """What dbt hands to the server for a ref carrying this window: the relation plus the filter.

    Asserting the whole subquery, not the predicate alone, because the wrapping is part of what
    the warehouse parses. MaxCompute does not require a subquery alias, so this is the shape.
    """
    return relation(**kwargs).render_event_time_filtered()


def wrapped(rel, clause):
    """The subquery dbt-core builds around a filtered ref; MaxCompute needs no alias for it."""
    return f"(select * from {rel.render()} where {clause})"


def window(clause, **kwargs):
    return wrapped(relation(**kwargs), clause)


def base_window(start=START, end=END, **kwargs):
    return window(f"event_time >= '{start}' and event_time < '{end}'", **kwargs)


# --- what pyodps is going to submit -----------------------------------------------------------


@pytest.mark.parametrize(
    "local_timezone,expected",
    [
        (None, None),  # client pins nothing: the project default applies, unknowable here
        (False, session_clock.UNPINNED_SESSION_TIMEZONE),  # the adapter's own default
        ("Etc/GMT", "Etc/GMT"),
        ("Asia/Shanghai", "Asia/Shanghai"),
    ],
    ids=["unpinned", "falsey", "etc-gmt", "named"],
)
def test_session_timezone_mirrors_pyodps(local_timezone, expected):
    saved = options.local_timezone
    options.local_timezone = local_timezone
    try:
        assert session_clock.session_timezone() == expected
    finally:
        options.local_timezone = saved


@pytest.mark.parametrize(
    "name,utc",
    [
        ("Etc/GMT", True),
        ("UTC", True),
        ("GMT", True),
        ("utc", True),
        ("Asia/Shanghai", False),
        ("Etc/GMT+8", False),  # -08:00, not UTC
        (None, False),  # unknown is not "already aligned"
    ],
)
def test_is_utc_session(name, utc):
    assert session_clock.is_utc_session(name) is utc


# --- the default path is untouched ------------------------------------------------------------


@pytest.mark.parametrize("session_tz", [False, "Etc/GMT", "UTC"], indirect=True)
def test_utc_session_renders_exactly_what_dbt_core_renders(session_tz):
    assert rendered() == base_window()


@pytest.mark.parametrize("session_tz", [None], indirect=True)
def test_unknown_session_clock_is_left_alone(session_tz):
    assert rendered() == base_window()


# --- a shifted session clock ------------------------------------------------------------------


@pytest.mark.parametrize("session_tz", ["Asia/Shanghai"], indirect=True)
def test_day_window_is_written_in_the_session_clock(session_tz):
    """+08:00: the UTC boundary 2025-05-01 00:00 is 08:00 locally, and that is what gets parsed."""
    assert rendered() == window(
        "event_time >= '2025-05-01 08:00:00' and event_time < '2025-05-02 08:00:00'"
    )


@pytest.mark.parametrize("session_tz", ["Etc/GMT+8"], indirect=True)
def test_negative_offset_session(session_tz):
    """POSIX `Etc/GMT+8` is UTC-08:00, so the local reading is the previous day's 16:00."""
    assert rendered() == window(
        "event_time >= '2025-04-30 16:00:00' and event_time < '2025-05-01 16:00:00'"
    )


@pytest.mark.parametrize("session_tz", ["Asia/Kolkata"], indirect=True)
def test_half_hour_offset_session(session_tz):
    assert rendered() == window(
        "event_time >= '2025-05-01 05:30:00' and event_time < '2025-05-02 05:30:00'"
    )


@pytest.mark.parametrize("session_tz", ["America/New_York"], indirect=True)
def test_dst_aware_session(session_tz):
    """2025-05-01 is EDT (-04:00) in New York; the shift follows the date, not a fixed offset."""
    assert rendered() == window(
        "event_time >= '2025-04-30 20:00:00' and event_time < '2025-05-01 20:00:00'"
    )


@pytest.mark.parametrize("session_tz", ["Asia/Shanghai"], indirect=True)
def test_quoted_field_name_is_preserved(session_tz):
    assert "where `event time` >= '2025-05-01 08:00:00'" in rendered(field="`event time`")


@pytest.mark.parametrize("session_tz", ["Asia/Shanghai"], indirect=True)
def test_open_ended_windows(session_tz):
    assert rendered(end=None) == window("event_time >= '2025-05-01 08:00:00'")
    assert rendered(start=None) == window("event_time < '2025-05-02 08:00:00'")


@pytest.mark.parametrize("session_tz", ["Asia/Shanghai"], indirect=True)
def test_sub_second_boundaries_keep_their_fraction(session_tz):
    got = rendered(end=datetime(2025, 5, 2, 0, 0, 0, 123456, tzinfo=timezone.utc))
    assert got == window(
        "event_time >= '2025-05-01 08:00:00' and event_time < '2025-05-02 08:00:00.123456'"
    )


@pytest.mark.parametrize("session_tz", ["Not/AZone", "Etc/Whatever"], indirect=True)
def test_unresolvable_zone_name_falls_back_to_dbt_core_rendering(session_tz):
    assert rendered() == base_window()


def test_no_filter_renders_the_relation_unchanged():
    plain = MaxComputeRelation.create(database="p", schema="s", identifier="t")
    assert plain.render_event_time_filtered() == plain.render()
