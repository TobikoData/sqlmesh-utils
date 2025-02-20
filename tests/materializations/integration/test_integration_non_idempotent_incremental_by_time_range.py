from sqlglot import exp
import sqlmesh.core.dialect as d
from sqlmesh.utils.date import to_datetime, to_ds

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
        upstream_table_name, columns_to_types=upstream_table_columns
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
                time_column event_timestamp,
                primary_key (event_id, event_source),
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
        upstream_table_name, columns_to_types=upstream_table_columns
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
                time_column event_timestamp,
                primary_key (event_id, event_source),
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
        upstream_table_name, columns_to_types=upstream_table_columns
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
