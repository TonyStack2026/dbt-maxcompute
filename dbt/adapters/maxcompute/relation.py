from dataclasses import dataclass, field
from typing import FrozenSet, Optional, TypeVar

from dbt.adapters.base.relation import BaseRelation, EventTimeFilter, InformationSchema
from dbt.adapters.contracts.relation import RelationType, Path, Policy, RelationConfig
from odps.models import Table

from dbt.adapters.maxcompute import session_clock

from dbt.adapters.maxcompute.relation_configs._materialized_view import (
    MaxComputeMaterializedViewConfig,
)

Self = TypeVar("Self", bound="MaxComputeRelation")


@dataclass
class OdpsIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class MaxComputeRelation(BaseRelation):
    quote_character: str = "`"
    # subquery alias name is not required in MaxCompute
    require_alias: bool = False

    def without_quote(self):
        return self.quote(False, False, False)

    include_policy: Policy = field(default_factory=lambda: OdpsIncludePolicy())

    renameable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,
                RelationType.Table,
            }
        )
    )

    replaceable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,
                RelationType.Table,
                RelationType.MaterializedView,
            }
        )
    )

    def _render_event_time_filtered(self, event_time_filter: EventTimeFilter) -> str:
        """State the batch window on the clock ``trunc_time()`` cuts partitions on.

        dbt-core compares ``event_time`` against a UTC boundary rendered as a timestamp string.
        MaxCompute parses that string on the session timezone, while the partitions the batch is
        overwritten into come from ``trunc_time()``, which cuts UTC days. Those two clocks line up
        on the adapter's default profile because it submits ``odps.sql.timezone=Etc/GMT``, but a
        profile that sets ``timezone`` moves only the window: one batch then reaches into the
        partition of its neighbour and the later ``insert overwrite`` erases the earlier rows.

        The fix writes the boundary as the session's own reading of the same UTC instant, which the
        server parses back into that instant. The boundary stays a plain string literal - the same
        shape dbt-core renders today, rather than a function call whose cost the optimizer would
        have to absorb - and the UTC-session path keeps rendering exactly the SQL it renders now.

        Returns dbt-core's rendering when the session clock is unknown or cannot be resolved
        locally: guessing a shift would be worse than leaving the known behaviour in place.
        """
        base = super()._render_event_time_filtered(event_time_filter)
        session = session_clock.session_timezone()
        if not base or not session or session_clock.is_utc_session(session):
            return base

        field = event_time_filter.field_name
        parts = []
        for boundary, operator in (
            (event_time_filter.start, ">="),
            (event_time_filter.end, "<"),
        ):
            if boundary is None:
                continue
            wall_clock = session_clock.as_session_wall_clock(boundary, session)
            if wall_clock is None:
                return base
            parts.append(f"{field} {operator} '{wall_clock}'")

        return " and ".join(parts) if parts else base

    @property
    def project(self):
        return self.database

    @property
    def is_transactional(self):
        return self.get("transactional", False)

    def information_schema(
        self, identifier: Optional[str] = None
    ) -> "MaxComputeInformationSchema":
        return MaxComputeInformationSchema.from_relation(self, identifier)

    @classmethod
    def from_odps_table(cls, table: Table):
        schema = table.get_schema()
        schema = schema.name if schema else "default"

        table_type = RelationType.Table
        if table.is_virtual_view:
            table_type = RelationType.View
        if table.is_materialized_view:
            table_type = RelationType.MaterializedView

        return cls.create(
            database=table.project.name,
            schema=schema,
            identifier=table.name,
            type=table_type,
        )

    @classmethod
    def materialized_view_from_relation_config(
        cls, relation_config: RelationConfig
    ) -> MaxComputeMaterializedViewConfig:
        return MaxComputeMaterializedViewConfig.from_relation_config(relation_config)


@dataclass(frozen=True, eq=False, repr=False)
class MaxComputeInformationSchema(InformationSchema):
    quote_character: str = "`"

    @classmethod
    def get_path(cls, relation: BaseRelation, information_schema_view: Optional[str]) -> Path:
        return Path(
            database="SYSTEM_CATALOG",
            schema="INFORMATION_SCHEMA",
            identifier=information_schema_view,
        )

    @classmethod
    def get_include_policy(cls, relation, information_schema_view):
        return relation.include_policy.replace(database=True, schema=True, identifier=True)

    @classmethod
    def get_quote_policy(
        cls,
        relation,
        information_schema_view: Optional[str],
    ) -> Policy:
        return relation.quote_policy.replace(database=False, schema=False, identifier=False)
