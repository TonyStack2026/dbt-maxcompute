# Delta Table 支持与建表配置

本文适用于 dbt-maxcompute **1.11.2、1.11.3b3 的 SQL 模型**。

| 物化方式 | Append Delta Table（无主键） | PK Delta Table（有主键） |
| --- | --- | --- |
| `table` | 支持，通过表属性指定 | 支持，通过事务属性和主键配置指定 |
| `incremental` | 支持，通过表属性指定 | 当前不能通过 `primary_keys` 自动创建物理主键 |

## `table`：创建 Append Delta Table

在模型中设置 `table.format.version='2'`：

```sql
{{ config(
    materialized='table',
    tblproperties={'table.format.version': '2'}
) }}

select * from {{ source('raw', 'events') }}
```

## `table`：创建 PK Delta Table

设置 `transactional=true` 和 `primary_keys`，主键支持一列或多列：

```sql
{{ config(
    materialized='table',
    transactional=true,
    primary_keys=['id']
) }}

select id, name from {{ source('raw', 'users') }}
```

主键值应非空。可用 `delta_table_bucket_num` 设置桶数，默认为 `16`。`table` 模型每次运行会重建目标表。

## `incremental`：创建 Append Delta Table

同样通过 `tblproperties` 指定表格式。以下示例使用 `merge` 按业务键更新或插入：

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    tblproperties={'table.format.version': '2'}
) }}

select id, name from {{ source('raw', 'users') }}
```

首次运行会创建 Append Delta Table；后续运行保留目标表，使用的临时表也按相同表格式创建。示例每次处理源查询的全部结果，实际业务可用 `is_incremental()` 增加增量筛选条件，并保证本批数据的业务键唯一。

纯追加场景可改用 `incremental_strategy='append'`，同时移除 `unique_key`；该策略不自动去重。Append Delta Table 是表类型，不代表只能使用 `append` 策略。

**当前限制：** SQL `incremental` 的 `unique_key` 只用于数据匹配，不是物理主键；增加 `primary_keys` 也不会自动建成 PK Delta Table。已有 PK 表在普通增量运行中保留表定义，但仍需验证具体写入策略；`--full-refresh` 会删表重建，当前无法保留其物理主键。

以上建表属性只在新建表时生效，不会自动转换已有目标表。已有数据需迁移或重建时，应先确认数据保留方案。

更多表类型说明见 [MaxCompute Delta Table 文档](https://help.aliyun.com/en/maxcompute/delta-tables)。
