from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

import dbt_common.exceptions
from dbt.adapters.contracts.relation import RelationConfig
from dbt_common.dataclass_schema import dbtClassMixin


@dataclass
class PartitionConfig(dbtClassMixin):
    granularity: str = "day"
    copy_partitions: bool = False
    generate_column_name: Optional[str] = None

    fields: List[str] = field(default_factory=list)
    data_types: List[str] = field(default_factory=list)

    def auto_partition(self) -> bool:
        for t in self.data_types:
            if t.lower() in ["timestamp", "date", "datetime", "timestamp_ntz"]:
                return True
        return False

    def render(self, with_type: bool = True) -> str:
        default_value = len(self.data_types) == 0
        res = ""
        for i, field_name in enumerate(self.fields):
            if with_type:
                if default_value:
                    column = f"{field_name} string"
                else:
                    column = f"{field_name} {self.data_types[i]}"
            else:
                column = field_name
            res += f"{column}, "
        res = res[:-2]  # 去掉最后的逗号和空格
        return res

    @classmethod
    def parse(cls, raw_partition_by) -> Optional["PartitionConfig"]:
        if raw_partition_by is None:
            return None
        try:
            if not isinstance(raw_partition_by, dict):
                raise TypeError
            raw_partition_by = dict(raw_partition_by)
            alias_pairs = (("field", "fields"), ("data_type", "data_types"))
            for alias, canonical in alias_pairs:
                if alias in raw_partition_by:
                    if canonical in raw_partition_by:
                        raise dbt_common.exceptions.DbtValidationError(
                            f"Invalid partition_by config: use either `{alias}` or "
                            f"`{canonical}`, not both"
                        )
                    raw_partition_by[canonical] = raw_partition_by.pop(alias)

            new_dict = {}
            for key, value in raw_partition_by.items():
                if key in ["fields", "data_types"]:
                    values = value.split(",") if isinstance(value, str) else value
                    new_dict[key] = [str(item).strip() for item in values]
                else:
                    new_dict[key] = value
            res = cls.from_dict(new_dict)
            res.post_validate()
            return res
        except TypeError:
            raise dbt_common.exceptions.CompilationError(
                f"Invalid partition_by config:\n"
                f"  Got: {raw_partition_by}\n"
                f'  Expected a dictionary with "fields" and "data_types" keys'
            )

    @classmethod
    def parse_model_node(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        """
        Parse model node into a raw config for `PartitionConfig.parse`

        - Note:
            This doesn't currently collect `time_ingestion_partitioning` and `copy_partitions`
            because this was built for materialized views, which do not support those settings.
        """
        config_dict: Dict[str, Any] = relation_config.config.extra.get("partition_by")
        return config_dict

    def post_validate(self):
        if 0 < len(self.data_types) != len(self.fields):
            raise dbt_common.exceptions.DbtValidationError(
                f"Invalid partition_by config:\n"
                f"  Got: {self.fields}\n"
                f"  Got: {self.data_types}\n"
                f"  Expected the same number of fields and data types"
            )
        if self.auto_partition() and len(self.fields) > 1:
            raise dbt_common.exceptions.DbtValidationError(
                f"Invalid partition_by config:\n"
                f"  Got: {self.fields}\n"
                f"  Expected a single partition column for auto partitioning"
            )
