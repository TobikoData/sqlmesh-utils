import typing as t
import pytest
from sqlmesh.core.model import Model, load_sql_based_model
import sqlmesh.core.dialect as d
from sqlglot import exp, parse_one
from sqlmesh_utils.materializations.non_idempotent_incremental_by_time_range import (
    NonIdempotentIncrementalByTimeRangeMaterialization,
    MaterializationProperties,
)
from tests.materializations.conftest import to_sql_calls, MockedEngineAdapterMaker
from sqlmesh.core.engine_adapter.trino import TrinoEngineAdapter
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.date import to_timestamp, now
from sqlmesh.core.macros import RuntimeStage

ModelMaker = t.Callable[..., Model]


@pytest.fixture
def make_model() -> ModelMaker:
    def _make(properties: t.Union[str, t.List[str]], dialect: t.Optional[str] = None) -> Model:
        if isinstance(properties, list):
            properties = ",\n".join(properties)

        properties_sql = ""
        if properties:
            properties_sql = f"materialization_properties ({properties}),"

        dialect_sql = f"dialect {dialect}," if dialect else ""

        expressions = d.parse(f"""
        MODEL (
            name test.model,
            kind CUSTOM (
                materialization 'non_idempotent_incremental_by_time_range',
                {properties_sql}
                batch_size 1,
                batch_concurrency 1
            ),
            {dialect_sql}
            start '2020-01-01',
            end '2020-01-10'
        );

        SELECT name, ds FROM upstream.table WHERE ds BETWEEN @start_ts AND @end_ts;
        """)
        return load_sql_based_model(expressions=expressions)

    return _make


def test_materialization_properties(make_model: ModelMaker):
    # basic usage
    model = make_model(["time_column = ds", "primary_key = (id, ds)"])
    kind = MaterializationProperties.from_model(model)

    assert kind.time_column.column == exp.to_column("ds", quoted=True)
    assert kind.primary_key == [exp.to_column("id", quoted=True), exp.to_column("ds", quoted=True)]

    # required fields
    with pytest.raises(ConfigError, match=r"Invalid time_column"):
        model = make_model([])
        MaterializationProperties.from_model(model)

    with pytest.raises(ConfigError, match=r"primary_key` must be specified"):
        model = make_model(["time_column = ds"])
        MaterializationProperties.from_model(model)

    # primary_key cant be the same as time_column
    with pytest.raises(ConfigError, match=r"primary_key` cannot be just the time_column"):
        model = make_model(["time_column = ds", "primary_key = ds"])
        MaterializationProperties.from_model(model)


def test_insert(make_model: ModelMaker, make_mocked_engine_adapter: MockedEngineAdapterMaker):
    model: Model = make_model(["time_column = ds", "primary_key = name"], dialect="trino")
    adapter = make_mocked_engine_adapter(TrinoEngineAdapter)
    strategy = NonIdempotentIncrementalByTimeRangeMaterialization(adapter)

    start = to_timestamp("2020-01-01")
    end = to_timestamp("2020-01-03")

    strategy.insert(
        "test.snapshot_table",
        query_or_df=model.render_query(
            start=start, end=end, execution_time=now(), runtime_stage=RuntimeStage.EVALUATING
        ),
        model=model,
        is_first_insert=True,
        start=start,
        end=end,
    )

    assert to_sql_calls(adapter) == [
        parse_one(
            """
            MERGE INTO "test"."snapshot_table" AS "__merge_target__"
            USING (
            SELECT
                "name" AS "name",
                "ds" AS "ds"
            FROM "upstream"."table" AS "table"
            WHERE
                "ds" BETWEEN '2020-01-01 00:00:00' AND '2020-01-02 23:59:59.999999'
            ) AS "__MERGE_SOURCE__"
            ON (
            "__MERGE_SOURCE__"."ds" BETWEEN CAST('2020-01-01 00:00:00+00:00' AS TIMESTAMP WITH TIME ZONE) AND CAST('2020-01-02 23:59:59.999999+00:00' AS TIMESTAMP(6) WITH TIME ZONE)
            AND "__MERGE_TARGET__"."ds" BETWEEN CAST('2020-01-01 00:00:00+00:00' AS TIMESTAMP WITH TIME ZONE) AND CAST('2020-01-02 23:59:59.999999+00:00' AS TIMESTAMP(6) WITH TIME ZONE)
            )
            AND "__MERGE_TARGET__"."name" = "__MERGE_SOURCE__"."name"
            WHEN MATCHED THEN UPDATE SET "name" = "__MERGE_SOURCE__"."name", "ds" = "__MERGE_SOURCE__"."ds"
            WHEN NOT MATCHED THEN INSERT ("name", "ds") VALUES ("__MERGE_SOURCE__"."name", "__MERGE_SOURCE__"."ds")
        """,
            dialect=adapter.dialect,
        ).sql(dialect=adapter.dialect)
    ]


def test_append(make_model: ModelMaker, make_mocked_engine_adapter: MockedEngineAdapterMaker):
    model: Model = make_model(["time_column = ds", "primary_key = name"], dialect="trino")
    adapter = make_mocked_engine_adapter(TrinoEngineAdapter)
    strategy = NonIdempotentIncrementalByTimeRangeMaterialization(adapter)

    start = to_timestamp("2020-01-01")
    end = to_timestamp("2020-01-03")

    strategy.append(
        "test.snapshot_table",
        query_or_df=model.render_query(
            start=start, end=end, execution_time=now(), runtime_stage=RuntimeStage.EVALUATING
        ),
        model=model,
        start=start,
        end=end,
    )

    assert to_sql_calls(adapter) == [
        parse_one(
            """
            MERGE INTO "test"."snapshot_table" AS "__merge_target__"
            USING (
            SELECT
                "name" AS "name",
                "ds" AS "ds"
            FROM "upstream"."table" AS "table"
            WHERE
                "ds" BETWEEN '2020-01-01 00:00:00' AND '2020-01-02 23:59:59.999999'
            ) AS "__MERGE_SOURCE__"
            ON (
            "__MERGE_SOURCE__"."ds" BETWEEN CAST('2020-01-01 00:00:00+00:00' AS TIMESTAMP WITH TIME ZONE) AND CAST('2020-01-02 23:59:59.999999+00:00' AS TIMESTAMP(6) WITH TIME ZONE)
            AND "__MERGE_TARGET__"."ds" BETWEEN CAST('2020-01-01 00:00:00+00:00' AS TIMESTAMP WITH TIME ZONE) AND CAST('2020-01-02 23:59:59.999999+00:00' AS TIMESTAMP(6) WITH TIME ZONE)
            )
            AND "__MERGE_TARGET__"."name" = "__MERGE_SOURCE__"."name"
            WHEN MATCHED THEN UPDATE SET "name" = "__MERGE_SOURCE__"."name", "ds" = "__MERGE_SOURCE__"."ds"
            WHEN NOT MATCHED THEN INSERT ("name", "ds") VALUES ("__MERGE_SOURCE__"."name", "__MERGE_SOURCE__"."ds")
        """,
            dialect=adapter.dialect,
        ).sql(dialect=adapter.dialect)
    ]
