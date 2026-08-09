{% macro mc_validate_microbatch_config(partition_by, batch_size, event_time) %}
  {% if partition_by is none %}
    {% set missing_partition_msg -%}
    The 'microbatch' strategy requires a `partition_by` config.
    {%- endset %}
    {% do exceptions.raise_compiler_error(missing_partition_msg) %}
  {% endif %}

  {% if not event_time %}
    {% do exceptions.raise_compiler_error(
      "The 'microbatch' strategy requires an `event_time` config."
    ) %}
  {% endif %}

  {% if partition_by.granularity != batch_size %}
    {% set invalid_partition_by_granularity_msg -%}
    The 'microbatch' strategy requires a `partition_by` config with the same granularity as its configured `batch_size`.
    Got:
      `batch_size`: {{ batch_size }}
      `partition_by.granularity`: {{ partition_by.granularity }}
    {%- endset %}
    {% do exceptions.raise_compiler_error(invalid_partition_by_granularity_msg) %}
  {% endif %}
{% endmacro %}

{% macro mc_generate_microbatch_build_sql(
      tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, tblproperties
) %}
    {% set build_sql = mc_insert_overwrite_sql(
        tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, tblproperties
    ) %}

    {{ return(build_sql) }}
{% endmacro %}
