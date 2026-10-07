"""The clock MaxCompute reads timestamp literals in, and how to state a UTC instant in it.

Two things have to agree for a ``microbatch`` model to be correct, and only one of them is
controlled by dbt:

* a batch window. dbt-core computes it in UTC and renders it as a timestamp string carrying an
  offset, ``event_time >= '2025-05-01 00:00:00+00:00'``. MaxCompute does not read that offset: it
  parses the wall clock on the **session** timezone (measured: ``+00:00``, ``+08:00`` and
  ``-05:00`` on the same wall clock select the same rows).
* the partition a row lands in. ``trunc_time(<field>, '<granularity>')`` cuts on **UTC**, and the
  target is auto-partitioned with exactly that expression, so the partition key does not move with
  the session clock.

The session timezone is what the profile's ``timezone`` field sets:
``MaxComputeCredentials._get_odps`` writes pyodps' ``options.local_timezone``, and pyodps turns it
into ``odps.sql.timezone`` for every statement - ``Etc/GMT`` when the value is falsey. With the
default profile the two clocks are both UTC and nothing has to be corrected. With
``timezone: Asia/Shanghai`` the window starts 8 hours early relative to its own partition, an
adjacent window overwrites the partition the previous window filled, and the run still reports
success.

This module states a UTC instant in whatever clock pyodps is about to submit, so the rendered
window and the partition key read the same day. It deliberately keeps the boundary a plain string
literal: a function call around the constant would cost partition pruning on the source side.
"""

from datetime import datetime, timezone
from typing import Optional

from dbt.adapters.events.logging import AdapterLogger
from odps import options

logger = AdapterLogger("MaxCompute")

#: What pyodps submits when ``options.local_timezone`` is falsey. The adapter takes this path when
#: the profile omits ``timezone`` - see ``odps/models/tasks/sql.py:collect_sql_settings``.
UNPINNED_SESSION_TIMEZONE = "Etc/GMT"

#: Names that all mean "the session clock is already UTC". Matching by name (rather than by
#: computed offset) keeps the default path rendering exactly the SQL it renders today.
_UTC_NAMES = frozenset(
    {
        "utc",
        "gmt",
        "gmt0",
        "gmt+0",
        "gmt-0",
        "z",
        "etc/gmt",
        "etc/gmt+0",
        "etc/gmt-0",
        "etc/utc",
        "etc/zulu",
        "posix/gmt",
        "posix/utc",
        "universal",
        "greenwich",
        "ucp",
        "+00:00",
        "-00:00",
        "0",
    }
)


def session_timezone() -> Optional[str]:
    """The session timezone pyodps will submit, or ``None`` when the client pins none.

    Mirrors pyodps' own rule so this reports the value the server actually gets, rather than a
    second guess of the profile. ``None`` means ``options.local_timezone`` is unset, in which case
    the project's own default applies and it is not knowable from here - the caller then leaves
    the window as dbt-core rendered it instead of guessing a shift.
    """
    tz = options.local_timezone
    if tz is None:
        return None
    if not tz:
        return UNPINNED_SESSION_TIMEZONE
    if isinstance(tz, str):
        return tz
    if tz is True:
        from odps.lib import tzlocal

        from odps import utils as odps_utils

        return odps_utils.get_zone_name(tzlocal.get_localzone())
    from odps import utils as odps_utils

    return odps_utils.get_zone_name(tz)


def is_utc_session(name: Optional[str]) -> bool:
    """True when the session clock cannot shift a UTC boundary, so nothing has to be corrected."""
    return name is not None and name.strip().lower() in _UTC_NAMES


def _resolve(name: str):
    """Best-effort lookup of a timezone name, ``None`` when nothing here can resolve it.

    ``zoneinfo`` reads the host's own tz database, which is the fresher source and what a Linux
    or macOS runner has. ``pytz`` - already a dependency of the stack this adapter builds on, and
    of dbt-core's own microbatch builder - carries a copy of that database with it, which is what
    lets a Windows host with no system tz data still resolve the profile's zone.
    """
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(name)
    except Exception:  # noqa: BLE001 - absent tzdata on the host is expected on some platforms
        pass
    try:
        import pytz

        return pytz.timezone(name)
    except Exception:  # noqa: BLE001 - unknown name: the caller degrades, it does not guess
        return None


def _text(moment: datetime) -> str:
    base = moment.strftime("%Y-%m-%d %H:%M:%S")
    if moment.microsecond:
        base += f".{moment.microsecond:06d}"
    return base


def as_session_wall_clock(moment: datetime, name: str) -> Optional[str]:
    """Render a UTC instant as the wall clock the session will parse it back from.

    MaxCompute reads a naive timestamp string as ``<instant> - offset(session)``, so writing the
    session's own reading of the UTC instant makes the comparison land on that instant. Returns
    ``None`` when the host cannot resolve ``name``: the caller then keeps dbt-core's rendering,
    which is what happens today, rather than shifting by an unverified amount.
    """
    if not name:
        return None
    zone = _resolve(name)
    if zone is None:
        logger.warning(
            "Cannot resolve session timezone '%s' locally; the microbatch window is compared on "
            "the session clock and may not line up with trunc_time() partitions." % name
        )
        return None
    aware = moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)
    return _text(aware.astimezone(zone))
