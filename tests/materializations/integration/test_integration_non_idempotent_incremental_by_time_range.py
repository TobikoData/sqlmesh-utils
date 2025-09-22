from sqlglot import exp
import sqlmesh.core.dialect as d
from sqlmesh.utils.date import to_datetime, to_ds, to_timestamp, now
from sqlmesh.core.macros import RuntimeStage
from unittest.mock import patch

from sqlmesh_utils.materializations.non_idempotent_incremental_by_time_range import (
    NonIdempotentIncrementalByTimeRangeMaterialization,
)
from tests.materializations.integration.conftest import Project


def test_basic_usage(project: Project):
    # upstream data to consume
    upstream_table_name = f"{project.test_schema}.event_data"

    upstream_data = [
        (1, "web", "cglading0@icq.com", to_datetime("2024-01-01 08:57:02")),
        (2, "web", "dwalczynski1@reuters.com", to_datetime("2024-01-02 22:47:38")),
        (3, "web", "sleggin2@va.gov", to_datetime("2024-01-03 22:28:34")),
        (1, "mobile", "atrowsdale3@sun.com", to_datetime("2024-01-04 03:55:21")),
        (1, "api", "opursey4@drupal.org", to_datetime("2024-01-05 04:39:03")),
        (6, "api", "bcutcliffe5@wisc.edu", to_datetime("2024-01-06 18:26:50")),
        (7, "mobile", "scressar6@newsvine.com", to_datetime("2024-01-07 22:06:05")),
        (8, "web", "skaradzas7@is.gd", to_datetime("2024-01-08 09:06:01")),
        (9, "web", "csnawdon8@ocn.ne.jp", to_datetime("2024-01-09 21:45:47")),
        (10, "api", "rchotty9@symantec.com", to_datetime("2024-01-10 08:07:51")),
    ]

    upstream_table_columns = {
        "event_id": exp.DataType.build("int"),
        "event_source": exp.DataType.build("varchar"),
        "data": exp.DataType.build("varchar"),
        "event_timestamp": exp.DataType.build("timestamp"),
    }

    project.engine_adapter.create_table(
        upstream_table_name, target_columns_to_types=upstream_table_columns
    )
    project.engine_adapter.insert_append(
        upstream_table_name,
        query_or_df=next(
            d.select_from_values(upstream_data, columns_to_types=upstream_table_columns)
        ),
    )

    # downstream model using custom materialization
    project.write_model(
        "test_table.sql",
        definition=f"""
        MODEL (
            name {project.test_schema}.model,
            kind CUSTOM (
                materialization 'non_idempotent_incremental_by_time_range',
                materialization_properties (
                    time_column = event_timestamp,
                    primary_key = (event_id, event_source)
                ),
                batch_size 1,
                batch_concurrency 1
            ),
            start '2024-01-01',
            end '2024-01-10'
        );

        SELECT event_id, event_source, data, event_timestamp
        FROM {upstream_table_name} WHERE event_timestamp BETWEEN @start_dt AND @end_dt;
    """,
    )

    ctx = project.context
    assert len(ctx.models) > 0
    ctx.plan(auto_apply=True, no_prompts=True)

    records = [
        tuple(r)
        for r in project.engine_adapter.fetchall(
            f"select event_id, event_source, data from {project.test_schema}.model order by event_timestamp"
        )
    ]
    assert len(records) == 10
    assert records == [(r[0], r[1], r[2]) for r in upstream_data]


def test_partial_restatement(project: Project):
    # upstream data to consume
    upstream_table_name = f"{project.test_schema}.event_data"

    original_upstream_data = [
        (1, "web", "cglading0@icq.com", to_datetime("2024-01-01 08:57:02")),
        (2, "web", "dwalczynski1@reuters.com", to_datetime("2024-01-02 22:47:38")),
        (3, "web", "sleggin2@va.gov", to_datetime("2024-01-03 22:28:34")),
        (1, "mobile", "atrowsdale3@sun.com", to_datetime("2024-01-04 03:55:21")),
        (1, "api", "opursey4@drupal.org", to_datetime("2024-01-05 04:39:03")),
        (6, "api", "bcutcliffe5@wisc.edu", to_datetime("2024-01-06 18:26:50")),
        (7, "mobile", "scressar6@newsvine.com", to_datetime("2024-01-07 22:06:05")),
        (8, "web", "skaradzas7@is.gd", to_datetime("2024-01-08 09:06:01")),
        (9, "web", "csnawdon8@ocn.ne.jp", to_datetime("2024-01-09 21:45:47")),
        (10, "api", "rchotty9@symantec.com", to_datetime("2024-01-10 08:07:51")),
    ]

    new_upstream_data = [
        # changed data
        (1, "web", "CHANGED_cglading0@icq.com", to_datetime("2024-01-01 08:57:02")),
        # new record
        (3, "api", "csnawdon8@ocn.ne.jp", to_datetime("2024-01-02 03:45:47")),
        # deleted, although this cant be propagated so will still be present in the model
        # (2,"web","dwalczynski1@reuters.com",to_datetime("2024-01-02 22:47:38")),
        # unchanged
        (3, "web", "sleggin2@va.gov", to_datetime("2024-01-03 22:28:34")),
        # changed all of these, although none of these changes will propagate because
        # the restatament intervals are restricted to 2024-01-01 00:00:00 -> 2024-01-04 00:00:00
        (1, "mobile", "__CHANGED__", to_datetime("2024-01-04 03:55:21")),
        (1, "api", "__CHANGED__", to_datetime("2024-01-05 04:39:03")),
        (6, "api", "__CHANGED__", to_datetime("2024-01-06 18:26:50")),
        (7, "mobile", "__CHANGED__", to_datetime("2024-01-07 22:06:05")),
        (8, "web", "__CHANGED__", to_datetime("2024-01-08 09:06:01")),
        (9, "web", "__CHANGED__", to_datetime("2024-01-09 21:45:47")),
        (10, "api", "__CHANGED__", to_datetime("2024-01-10 08:07:51")),
    ]

    upstream_table_columns = {
        "event_id": exp.DataType.build("int"),
        "event_source": exp.DataType.build("varchar"),
        "data": exp.DataType.build("varchar"),
        "event_timestamp": exp.DataType.build("timestamp"),
    }

    project.engine_adapter.create_table(
        upstream_table_name, target_columns_to_types=upstream_table_columns
    )
    project.engine_adapter.insert_append(
        upstream_table_name,
        query_or_df=next(
            d.select_from_values(original_upstream_data, columns_to_types=upstream_table_columns)
        ),
    )

    # downstream model using custom materialization
    project.write_model(
        "test_table.sql",
        definition=f"""
        MODEL (
            name {project.test_schema}.model,
            kind CUSTOM (
                materialization 'non_idempotent_incremental_by_time_range',
                materialization_properties (
                    time_column = event_timestamp,
                    primary_key = (event_id, event_source),
                ),
                batch_size 1,
                batch_concurrency 1
            ),
            start '2024-01-01',
            end '2024-01-10'
        );

        SELECT event_id, event_source, data, event_timestamp
        FROM {upstream_table_name} WHERE event_timestamp BETWEEN @start_dt AND @end_dt;
    """,
    )

    ctx = project.context
    assert len(ctx.models) > 0
    ctx.plan(auto_apply=True)

    # verify initial state
    assert (
        project.engine_adapter.fetchone(f"select count(*) from {project.test_schema}.model")[0]  # type: ignore
        == 10
    )
    assert (
        project.engine_adapter.fetchone(
            f"select count(*) from {project.test_schema}.model where data like '%CHANGED%'"
        )[0]  # type: ignore
        == 0
    )

    # change upstream data
    project.engine_adapter.drop_table(upstream_table_name)
    project.engine_adapter.create_table(
        upstream_table_name, target_columns_to_types=upstream_table_columns
    )
    project.engine_adapter.insert_append(
        upstream_table_name,
        query_or_df=next(
            d.select_from_values(new_upstream_data, columns_to_types=upstream_table_columns)
        ),
    )

    # restate model
    ctx.plan(
        restate_models=[f"{project.test_schema}.model"],
        start=to_datetime("2024-01-01 00:00:00"),
        end=to_datetime("2024-01-04 00:00:00"),
        auto_apply=True,
    )

    # verify new state
    records = [
        tuple(r)
        for r in project.engine_adapter.fetchall(
            f"select event_id, event_source, data, event_timestamp from {project.test_schema}.model order by event_timestamp"
        )
    ]
    assert len(records) == 11

    restated_records = [r for r in records if to_ds(r[3]) <= "2024-01-03"]
    remaining_records = [r for r in records if r not in restated_records]

    assert len(restated_records) == 4
    assert len(remaining_records) == 7

    assert restated_records[0] == (
        1,
        "web",
        "CHANGED_cglading0@icq.com",
        to_datetime("2024-01-01 08:57:02").replace(tzinfo=None),
    )
    assert restated_records[1] == (
        3,
        "api",
        "csnawdon8@ocn.ne.jp",
        to_datetime("2024-01-02 03:45:47").replace(tzinfo=None),
    )
    assert restated_records[2] == (
        2,
        "web",
        "dwalczynski1@reuters.com",
        to_datetime("2024-01-02 22:47:38").replace(tzinfo=None),
    )
    assert restated_records[3] == (
        3,
        "web",
        "sleggin2@va.gov",
        to_datetime("2024-01-03 22:28:34").replace(tzinfo=None),
    )

    assert not any(["CHANGED" in r[2] for r in remaining_records])


def test_physical_properties_integration(project: Project):
    upstream_table_name = f"{project.test_schema}.library_data"

    upstream_data = [
        (101, "fiction", "978-0-123456-78-9", to_datetime("2024-01-01 09:15:00")),
        (102, "science", "978-0-234567-89-0", to_datetime("2024-01-02 14:30:00")),
        (103, "history", "978-0-345678-90-1", to_datetime("2024-01-03 11:45:00")),
    ]

    upstream_table_columns = {
        "book_id": exp.DataType.build("int"),
        "category": exp.DataType.build("varchar"),
        "isbn": exp.DataType.build("varchar"),
        "borrowed_at": exp.DataType.build("timestamp"),
    }

    project.engine_adapter.create_table(
        upstream_table_name, target_columns_to_types=upstream_table_columns
    )
    project.engine_adapter.insert_append(
        upstream_table_name,
        query_or_df=next(
            d.select_from_values(upstream_data, columns_to_types=upstream_table_columns)
        ),
    )

    project.write_model(
        "test_library_with_properties.sql",
        definition=f"""
        MODEL (
            name {project.test_schema}.library_borrowings,
            kind CUSTOM (
                materialization 'non_idempotent_incremental_by_time_range',
                materialization_properties (
                    time_column = borrowed_at,
                    primary_key = (book_id, category)
                ),
                batch_size 1,
                batch_concurrency 1
            ),
            start '2024-01-01',
            end '2024-01-03',
            physical_properties (
                extra_props = (
                    enable_compression = true
                )
            ),
            partitioned_by [category, borrowed_at]
        );

        SELECT book_id, category, isbn, borrowed_at
        FROM {upstream_table_name} WHERE borrowed_at BETWEEN @start_dt AND @end_dt;
    """,
    )

    ctx = project.context
    assert len(ctx.models) > 0

    model = ctx.get_model(f"{project.test_schema}.library_borrowings")
    assert model is not None
    assert model.partitioned_by is not None
    assert len(model.partitioned_by) == 2
    assert model.physical_properties
    assert "extra_props" in model.physical_properties

    strategy = NonIdempotentIncrementalByTimeRangeMaterialization(project.engine_adapter)

    # tracks CTAS calls to verify table_properties are passed
    ctas_calls = []

    def mock_ctas(**kwargs):
        ctas_calls.append(kwargs)

    mock_columns = {
        "book_id": exp.DataType.build("int"),
        "category": exp.DataType.build("varchar"),
        "isbn": exp.DataType.build("varchar"),
        "borrowed_at": exp.DataType.build("timestamp"),
    }

    def mock_merge(**kwargs):
        pass

    with patch.object(project.engine_adapter, "ctas", side_effect=mock_ctas), patch.object(
        project.engine_adapter, "table_exists", return_value=False
    ), patch.object(project.engine_adapter, "columns", return_value=mock_columns), patch.object(
        project.engine_adapter, "merge", side_effect=mock_merge
    ):
        start = to_timestamp("2024-01-01")
        end = to_timestamp("2024-01-03")

        strategy.insert(
            f"{project.test_schema}.library_borrowings",
            query_or_df=model.render_query(
                start=start, end=end, execution_time=now(), runtime_stage=RuntimeStage.EVALUATING
            ),
            model=model,
            is_first_insert=True,
            start=start,
            end=end,
            render_kwargs={},
        )

    # Assert that CTAS was called with table_properties
    assert len(ctas_calls) == 1
    ctas_kwargs = ctas_calls[0]

    assert "table_properties" in ctas_kwargs
    table_props = ctas_kwargs["table_properties"]

    assert table_props is not None
    assert "extra_props" in table_props
