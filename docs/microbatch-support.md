# Microbatch models

`microbatch` is dbt-core's batched incremental strategy. This adapter implements it as
`incremental_strategy='microbatch'` on a `materialized='incremental'` model, and a batch is written
as a **partition overwrite**. Everything below was measured against a real MaxCompute project; what
is not listed is not verified.

The regressions that hold these statements in place live in
`tests/functional/adapter/incremental/`:

| File | Pins |
| --- | --- |
| `test_incremental.py::TestMicrobatchMaxCompute` | upstream's microbatch contract, on this warehouse |
| `test_microbatch_window_semantics.py` | windows, replay, empty window, late data, duplicate keys, rejected configurations |
| `test_microbatch_partition_timezone.py` | the session-timezone dependency described below |
| `test_microbatch_date_partition.py` | a `date` event column is refused, and the replacement that works |
| `test_microbatch_failure_retry.py` | what survives a batch that fails, and what `dbt retry` replays |

## How a batch is written

dbt-core computes the batch list from `begin`, `batch_size` and the run's event-time window,
re-compiles the model once per batch, and attaches a half-open filter
(`event_time >= <start> and event_time < <end>`) to every upstream ref or source that declares
`event_time`. This adapter then builds a temporary relation from that filtered query and runs

```sql
insert overwrite table <target> (select * from <tmp>)
```

On an auto-partitioned target - which is what `partition_by` with a time-typed field creates, and
what microbatch requires - MaxCompute replaces **only the partitions that appear in the batch's
rows**. Neighbouring partitions keep their rows, and a batch whose query returns nothing is a no-op.

| Situation | Result |
| --- | --- |
| Row exactly at `<start>` | written by this batch |
| Row exactly at `<end>` | left to the next batch |
| Re-running the same window | same rows; nothing appended twice |
| Window with no matching rows | target unchanged: nothing added, nothing deleted |
| Late row inside an already-written window | invisible until that window is replayed (or `lookback` covers it) |

Windows can be driven without touching any clock:

```
dbt run --event-time-start 2025-05-02 --event-time-end 2025-05-03
```

The two flags are mutually required, and they are what makes these runs reproducible.

## Requirements, and who enforces them

| Combination | Outcome | Enforced by |
| --- | --- | --- |
| no `partition_by` | rejected: "The 'microbatch' strategy requires a `partition_by` config." | this adapter |
| `event_time` declared as `partition_by.data_type: date` | rejected: "cannot batch on a `date` event column" | this adapter |
| `partition_by.granularity` != `batch_size` | rejected: "requires a `partition_by` config with the same granularity as its configured `batch_size`" | this adapter |
| no `event_time` | rejected during parsing: "Microbatch model '<name>' must provide an 'event_time' (string) config" | dbt-core |
| no `batch_size` | rejected during parsing | dbt-core |
| no `begin` | rejected during parsing: "Microbatch model '<name>' must provide a 'begin' (datetime) config" | dbt-core |
| no `unique_key` | **accepted**; the model runs | nobody |
| the same `unique_key` twice inside one window | **both rows are kept** | - |

Read the last two lines before relying on `unique_key`: a batch is a partition overwrite, so
`unique_key` neither makes the write an upsert nor deduplicates, and nothing enforces that it is
set. For key-based updates use `merge` or `delete+insert`. (The example models in this repository
used to state that a missing `unique_key` raises a compiler error; it does not, and the comment is
corrected in the same change as this page.)

## A `date` event column cannot match a batch window

`partition_by` with `data_type: date` is refused for `microbatch`, and it is worth knowing why the
refusal is the only safe shape. dbt-core compares each batch as

```sql
event_time >= '2025-05-01 00:00:00+00:00' and event_time < '2025-05-02 00:00:00+00:00'
```

MaxCompute does not convert that text against a `DATE` column, and it does not complain either - the
comparison evaluates to `NULL`. No row satisfies a `NULL` predicate, so every batch selects nothing;
an empty `insert overwrite` changes nothing; and the run reports the model as a success with the
target left empty. Measured on a real project with four rows spanning two days: `PASS=2`, zero rows
in the target, and the same `NULL` in `Etc/GMT`, `Asia/Shanghai` and `Etc/GMT+8` sessions alike. This
is therefore not the session-clock problem below: the comparison is null-valued whatever the clock.

Writing the boundary as a bare `'YYYY-MM-DD'` *does* work for a `DATE` column - measured, it selects
the day's rows - but it cannot be what this adapter renders, because the point where the boundary is
written (dbt-core's `Relation.render_event_time_filtered`) is handed a field name, not a column type,
and the date-only text is not safe for the types that work today:

Each cell is the comparison as the server evaluates it, in a `Etc/GMT` and an `Asia/Shanghai`
session alike - the third column is the shape a DATE needs, the fourth is what a rendering point that
cannot see the column type would have to emit to cover both:

| `event_time` column | `'2025-05-01 00:00:00'` | `'2025-05-01 00:00:00+00:00'` (what dbt renders) | `'2025-05-01'` | naive text OR date-only |
| --- | --- | --- | --- | --- |
| `date` | `NULL` - batch selects nothing | `NULL` - batch selects nothing | selects its day | selects its day |
| `datetime` | selects its day | **ODPS-0130071** | **ODPS-0130071** | **ODPS-0130071** |
| `timestamp` | selects its day | selects its day | `NULL` - batch selects nothing | selects its day |
| `timestamp_ntz` | selects its day | selects its day | `NULL` - batch selects nothing | selects its day |

So no type-blind rendering exists: the date-only text either silently empties a model (`timestamp`,
`timestamp_ntz`) or stops it compiling (`datetime`), and OR-ing the two texts inherits the DATETIME
failure. The combination is refused at compile time instead, in `mc_validate_microbatch_config`,
keyed on the declared `partition_by.data_type` - the one signal that is available there.

One reading is recorded without being acted on, and it is now measured as a model too: a `datetime`
event column against the offset-bearing text dbt renders fails at the server -
`ODPS-0130071 ... ODPS-0121095: Invalid argument - in function cast, string datetime's format must be
yyyy-mm-dd hh:mi:ss, input string is:2025-05-01 00:00:00+00:00` - with the node reported as an error.
So a `datetime` microbatch model does not run on this adapter's default profile; the same comparison
as a bare `datetime` column fails in an `Etc/GMT` and an `Asia/Shanghai` session alike, while the
offset-free text `>= '2025-05-01 00:00:00'` selects its day normally. That is a loud failure rather
than a silent empty target, so it is a separate problem from the one this page refuses, and it is
tracked separately rather than folded into this change.

**What to write instead.** The window is attached to the *upstream* relation - dbt-core wraps every
ref or source that declares `event_time` - so it is that column which has to stop being a DATE. Cast
it where the upstream model is built, declare `event_time` on the resulting instant column, and
partition the microbatch model on it with `data_type: timestamp`. Measured end to end
(`test_microbatch_date_partition.py`): two adjacent day windows write their own rows into their own
partitions, and replaying the first window does not erase the second.

Two shapes that look like fixes and are measured not to be:

* a `cast()` inside the microbatch model's own SELECT - the batch predicate was already applied to
  the upstream DATE column, so the target is still empty and the run still succeeds;
* `cast(event_date as datetime)`, and making the DATE column itself the partition key. This
  warehouse refuses the DATE->DATETIME cast, and partition keys must be `BIGINT` or `STRING`.

The refusal catches the shape the adapter can see - the model that declares its event field as a
`date` partition column. An *upstream* relation whose `event_time` column is a DATE is not visible
from here (only the field name reaches the rendering point) and stays silent; that residual is
pinned as a failing-behaviour test rather than left to be rediscovered.

The workaround is measured on the adapter's default session (`timezone` unset, so
`odps.sql.timezone=Etc/GMT`). Under a profile that sets `timezone`, the DATE->TIMESTAMP reading is
taken on the session clock (`2025-05-01` reads as `2025-04-30 16:00:00` UTC in an `Asia/Shanghai`
session) and lands on the previous partition day: that is the session-timezone problem below, not a
new one.

## The session timezone decides whether a window maps onto one partition

dbt renders the window as a timestamp string carrying an offset - `event_time >=
'2025-05-01 00:00:00+00:00'` - and MaxCompute evaluates that comparison on the **session** timezone
(measured: `+00:00`, `+08:00` and `-05:00` on the same wall clock select the same rows). The
partition a row lands in comes from `trunc_time(<field>, '<granularity>')`, which cuts on **UTC**.
Those two clocks have to agree, and what makes them agree is the session timezone:

* `MaxComputeCredentials._get_odps` sets pyodps' `local_timezone` to false when the profile omits
  `timezone`, and pyodps then submits `odps.sql.timezone=Etc/GMT` with every statement;
* with that default, one day window maps onto exactly one partition and two adjacent windows never
  touch the same partition;
* with `timezone: Asia/Shanghai` in the profile, the same fixtures behave differently: the window is
  read on the session clock while `trunc_time()` keeps cutting UTC days, rows before 08:00 local
  belong to the previous partition day, one "day" window covers two partitions, and the next
  window's overwrite erases rows the earlier window had written. The run reports success.

So `timezone` in the profile is not a display option as far as `microbatch` is concerned: it changes
which rows survive. Until the two clocks are reconciled in code - or the combination is refused at
compile time - **leave `timezone` unset for projects whose models use `microbatch`**.
`TestMicrobatchProfileTimezone` pins the failing combination as `xfail(strict=True)`, so it starts
reporting an error the moment the behaviour changes and can be deleted deliberately.

## Not verified here

- Python (MaxFrame) microbatch models, which apply the window to the returned DataFrame instead of
  to the compiled SQL;
- `batch_size` of `month` or `year`, and how their units interact with `trunc_time()`;
- `partitions`, `lookback`, `on_schema_change`, `grants` and model contracts on microbatch models;
- `dbt retry` for a model whose *first* batch fails;
- whether an upstream `event_time` column can be seen when the source of the rows is a `source:`
  rather than a dbt model - the DATE-typed upstream is least avoidable there, and this page's
  residual is measured on a dbt model;
- the instant-cast workaround under `batch_size` of `hour`, or under a profile that sets `timezone`
  (see the session-timezone section).
