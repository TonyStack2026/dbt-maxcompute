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
| `test_microbatch_partition_timezone.py` | the window/partition clock pairing, on a UTC session and on `timezone: Asia/Shanghai` |
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

## The batch window is compared on the session clock, the partition key on UTC

dbt renders the window as a timestamp string carrying an offset - `event_time >=
'2025-05-01 00:00:00+00:00'` - and MaxCompute evaluates that comparison on the **session** timezone
(measured: `+00:00`, `+08:00` and `-05:00` on the same wall clock select the same rows). The
partition a row lands in comes from `trunc_time(<field>, '<granularity>')`, which cuts on **UTC**.

What sets the session timezone is the profile: `MaxComputeCredentials._get_odps` writes pyodps'
`local_timezone`, and pyodps submits it as `odps.sql.timezone` with every statement - `Etc/GMT`
when the profile omits `timezone`. On that default both clocks read UTC, so nothing needs
correcting. A profile that *does* set `timezone` used to move one clock and not the other: a "day"
window reached into its neighbour's partition, and the later `insert overwrite` erased rows the
earlier window had written, while the run reported success.

`MaxComputeRelation._render_event_time_filtered` now states each boundary in the session's own
reading of the same UTC instant - `2025-05-01 00:00:00+00:00` becomes `'2025-05-01 08:00:00'` under
`timezone: Asia/Shanghai` - which the server parses back into that instant, so the window and the
partition key are cut by the same clock. Two properties of that rendering are deliberate:

* on the default profile the SQL is unchanged, character for character: the session clock is
  `Etc/GMT`, so pipelines that already agree are not routed through the new code at all;
* the boundary stays a plain string literal. The alternative spelling of the same instant,
  `from_utc_timestamp(...)`, evaluates to a `TIMESTAMP`, and MaxCompute refuses to compare a
  `TIMESTAMP` with a `DATETIME` or `DATE` operand instead of converting it (measured) - so that form
  would trade a working query for a compile error on some models. It would also replace a constant
  with a function call in the predicate, and whether the optimizer still prunes the source with a
  non-constant boundary has not been measured here.

### Which event-time types this aligns, and which it cannot

| `partition_by.data_type` | window vs partition with a non-UTC `timezone` |
| --- | --- |
| `timestamp`, `datetime` | aligned - the shift is derived from the session clock |
| `timestamp_ntz` | **not aligned** - a naive column has no clock, so shifting its boundary by the session offset moves rows between partitions |
| `date` | unusable either way - comparing a `DATE` with a timestamp string yields `NULL`, so no row falls in any window (measured) |

`dbt` hands the rendering point a field name, not a column type, so these three cannot be told
apart where the boundary is written. Until they can, a `microbatch` model partitioned on
`timestamp_ntz` or `date` should keep `timezone` unset. Refusing the combination at compile time is
the other way to close this, and it would stop jobs that behave correctly today, so that is a
decision to make deliberately rather than as part of a bug fix.

If the profile's `timezone` cannot be resolved on the host running dbt, the window is left as
dbt-core rendered it and a warning is logged: a guessed shift is worse than the known behaviour.

## Not verified here

- Python (MaxFrame) microbatch models, which apply the window to the returned DataFrame instead of
  to the compiled SQL;
- `batch_size` of `month` or `year`, and how their units interact with `trunc_time()`;
- `partitions`, `lookback`, `on_schema_change`, `grants` and model contracts on microbatch models;
- `dbt retry` for a model whose *first* batch fails.
- Hour-granularity batches whose boundary falls in a timezone's DST gap or fold. The shift is
  computed per boundary against the profile's zone, and the offline tests cover an EDT/EST zone,
  but no real run on a DST-affected project has pinned it.
- `microbatch` models whose event time is `timestamp_ntz` or `date` under a non-UTC `timezone`, and
  Python (MaxFrame) models on a non-UTC session - see the table above for why the first two are not
  aligned by this, and note that the MaxFrame path filters the returned DataFrame in Python rather
  than in SQL, so it does not go through this rendering at all.
