# Snapshot support on MaxCompute

Which snapshot combinations work on MaxCompute, and what the adapter does when one does
not.  Every "measured" statement below came out of a run of
`tests/functional/maxcompute/test_snapshot_contract.py` against a live three-tier
project (dbt-core 1.11.2, pyodps 0.13.2), where each case reads the snapshot table back
from the server and reports `(total rows, current versions, expired versions)`.  What was
not measured is listed under *Not verified* instead of being implied.

## What a snapshot needs from the warehouse

dbt keeps snapshot history in two steps: build a staging table of the changes, then run
one `merge into` against the snapshot table to expire the versions whose data changed and
insert the new current versions.  On MaxCompute that `merge into` only runs on a
**transactional** table, and the expired version of a key has to sit next to its current
version.  So:

* the snapshot table dbt creates is always created `TBLPROPERTIES("transactional"="true")`,
  unpartitioned and without a primary key - measured on the first run;
* a snapshot table that already exists is checked against those two requirements before
  anything is built, including the staging table (`TestSnapshotPreexistingPlainTarget`,
  `TestSnapshotPreexistingDeltaTarget`);
* the *source* does not have to be transactional - a snapshot only reads from it
  (`TestSnapshotPlainSource` measured a plain source table).

## Strategies

| strategy | what marks a new version | measured |
| --- | --- | --- |
| `timestamp` | `dbt_valid_from < updated_at` for the column named in `updated_at` | update of 2 keys -> `+2` versions, 2 expired, all keys still current; re-run with no change adds nothing |
| `check` | value difference over `check_cols` (or `"all"`) | a tracked column -> 1 new version; an untracked column -> nothing; `check_cols="all"` notices a column the narrow list ignores |

Two details worth knowing, both measured rather than assumed:

* **NULL and empty string are two different values for `check`.** Taking one checked
  column `'Dan' -> NULL -> '' -> 'Dan'` produced a new version at *every* step.  The md5
  that builds `dbt_scd_id` does coalesce NULL to `''`, but that hash only *names* a
  version; it is not what decides whether a row changed.
* **The version identity is `md5(unique_key, updated_at)`.** Re-inserting a key with the
  *same* `updated_at` after it was expired writes nothing: the new row's `dbt_scd_id`
  equals the expired row's, the merge's matched branch requires `dbt_valid_to is null`,
  and the not-matched branch no longer fires.  Give the revived row a newer
  `updated_at` and it becomes current again (measured: `+1` version, `+1` current).

### Hard deletes are opt-in, in dbt - not in this adapter

A row that disappears from the source is ignored unless the snapshot sets `hard_deletes`:

| `hard_deletes` | measured on MaxCompute |
| --- | --- |
| unset | nothing: the last version stays current forever (asserted so nobody "fixes" this by accident) |
| `'invalidate'` | the current version gets `dbt_valid_to` and no row is added; works for both strategies (`timestamp` `0,-2,+2`; `check` `0,-1,+1`), and a key that comes back with a newer `updated_at` becomes current again |
| `'new_record'` | a new version marked `'True'` in `dbt_is_deleted` is added (`+2` versions, 2 marked deleted).  This option needs the **dispatched macro** `get_columns_in_relation`, which had no MaxCompute implementation: every `new_record` snapshot died with `get_columns_in_relation macro not implemented for adapter maxcompute` before reaching the server.  The macro is now here, delegating to the adapter's existing pyodps column reader |

`hard_deletes` also accepts the older `invalidate_hard_deletes=True` spelling (what the
upstream adapter tests use); that alias is what the cases in
`tests/functional/adapter/test_simple_snapshot.py` rely on.

## Target table types

| snapshot table | measured result |
| --- | --- |
| created by dbt (transactional, no key) | supported: first run, re-run, update, hard delete with `invalidate`/`new_record`, revive |
| Append Delta - `tblproperties={'table.format.version': '2'}` | supported: table comes out transactional, first run and update behave as above |
| pre-existing **plain** (non-transactional) table | refused before anything is built: `Snapshot target ... is a non-transactional MaxCompute table ... recreate it with TBLPROPERTIES("transactional"="true")`.  Before this check the same run failed later, inside the merge, with `ODPS-0130071 ... merge into target table must be transactional table` - after a staging table had already been created |
| pre-existing **PK Delta** table (primary key on the unique key) | refused: `Snapshot target ... has a primary key (id) ... the record ends up with no current version`.  Before the check this combination *reported success*: the first snapshot filled the table (5 current), and after a source update the key had exactly one row left - the expired one - so the current version was silently gone |
| view / external table / metadata unreadable | **not** refused: the adapter cannot tell, so it lets the statement speak rather than guess |

## Config keys the snapshot materialization does not apply

`partition_by`, `primary_keys` / `delta`, `transactional=false` and `lifecycle` are
accepted by dbt's config system and then ignored, because the snapshot table has to be
unpartitioned, keyless, and kept for as long as the history is.  The run now warns and
still creates a table that can hold history - measured for all four: the snapshot
succeeded, the warning named the key, and the table stayed transactional, unpartitioned,
without a key (`lifecycle` reported by the server was still "unset", not the requested 30).

```
Snapshot 'snap_part' sets partition_by (a snapshot table is never partitioned: ...) ;
the MaxCompute snapshot materialization does not apply them.
```

Warn, not error: the history dbt writes is still correct, only the shape the key asked
for is not what you get, and a hard failure would break pipelines that work today.  The
two table-type cases above are errors because continuing there produces a wrong result.

## Not verified

* two-tier projects (no schema) - every measurement ran on a three-tier project;
* `unique_key` over several columns, `snapshot_table_column_names`,
  `dbt_valid_to_current`, and the column-name variants in dbt's own adapter test suite;
* snapshotting through an ephemeral model, a view, a materialized view, or a partitioned
  source where only some partitions change;
* switching an existing snapshot table between `check` and `timestamp`;
* two `dbt snapshot` runs writing the same target at the same time;
* `hard_deletes='new_record'` with `check_cols='all'`, and `dbt_is_deleted` cleanup
  behaviour over long histories.

## Reproduce

```bash
export DBT_PROFILE_PATH=/path/to/dbt_profile.yml   # type / project / schema / endpoint / auth
python -m pytest tests/functional/maxcompute/test_snapshot_contract.py -v -s
```

Each class gets its own schema and dbt drops it at teardown; the printed `SERVER[...]`
lines are the measurements.  With no profile or credentials the cases skip with a reason
rather than passing, so a run that reports nothing is visible as nothing.
