# SQLMesh Utils

This repository contains things that are not included in [SQLMesh Core](https://github.com/TobikoData/sqlmesh/) for various reasons but may be useful in very specific cases.

For example, the custom materializations included here will typically do things like relax constraints / guarantees of the SQLMesh Core model kinds to work around environment-specific limitations.

## Usage

Install the `sqlmesh-utils` library into your SQLMesh Python environment:

```
$ pip install sqlmesh-utils
```

# Custom Materializations

After installing the `sqlmesh-utils` library above, you can reference the custom materializations via the [CUSTOM](https://sqlmesh.readthedocs.io/en/stable/guides/custom_materializations/) model kind like so:

```
MODEL (
    name my_db.my_model,
    kind CUSTOM (
        materialization 'custom_materialization_name',
        materialization_properties (
            config_key = 'config_value'
        )
    )
);
```

Before using any of these materializations, **you should take the time to understand the tradeoffs**. There is generally a reason they are not in upstream SQLMesh.

## Non-Idempotent Incremental By Time Range

This behaves similar to [INCREMENTAL_BY_TIME_RANGE](https://sqlmesh.readthedocs.io/en/stable/concepts/models/model_kinds/#incremental_by_time_range) model kind in SQLMesh, but with one important difference - it loads and restates data using a `MERGE` statement instead of `INSERT OVERWRITE` or `DELETE+INSERT`.

The reason you might want to use it is to prevent dirty reads during data restatement on engines that do not support atomically replacing a partition of data, such as Trino on Iceberg / Delta Lake.

Due to the use of a `MERGE` statement, this materialization type supports upserts only. That is, if records are deleted from the source data, these deletions **will not** be reflected in the target table. So the downside of using this materialization is that it's possible to end up with ghost records in your target table after a restatement. For this reason, we call it "non-idempotent".

> [!NOTE]
> Note that some engines can propagate deletes in a `MERGE` statement using syntax like `WHEN NOT MATCHED [IN SOURCE] THEN DELETE`. However, this is not part of ANSI SQL so engines like Trino and Postgres do not implement it.

### Usage

This mostly follows the same usage as [INCREMENTAL_BY_TIME_RANGE](https://sqlmesh.readthedocs.io/en/stable/concepts/models/model_kinds/#incremental_by_time_range):

```
MODEL (
    name my_db.my_model,
    kind CUSTOM (
        materialization 'non_idempotent_incremental_by_time_range',
        materialization_properties (
            time_column = event_timestamp,
            primary_key = (event_id, event_source)
        )
    )
);

SELECT event_id, event_source, event_data, event_timestamp
FROM upstream.table
```

The properties are as follows:

#### time_column

This is the column in the dataset that contains the timestamp. It follows the [same syntax](https://sqlmesh.readthedocs.io/en/latest/concepts/models/model_kinds/#time-column) as upstream `INCREMENTAL_BY_TIME_RANGE`.

#### primary_key

This is the column or combination of columns that uniquely identifies a record.

The columns listed here are used in the `ON` clause of the SQL Merge to join the source and target datasets.

Note that the `time_column` is **not** automatically injected into this list (to allow timestamps on records to be updated), so if the `time_column` does actually form part of the primary key in your dataset then it needs to be added here.