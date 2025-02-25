from __future__ import annotations
import typing as t
from sqlmesh import CustomMaterialization
from sqlmesh.core.model import Model
from sqlmesh.core.model.kind import TimeColumn
from sqlglot import exp
from sqlmesh.utils.date import make_inclusive
from sqlmesh.utils.errors import ConfigError, SQLMeshError
from pydantic import model_validator
from sqlmesh.utils.pydantic import list_of_fields_validator
from sqlmesh.utils.date import TimeLike
from sqlmesh.core.engine_adapter.base import MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS
from sqlmesh import CustomKind
from sqlmesh.utils import columns_to_types_all_known

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import QueryOrDF


class NonIdempotentIncrementalByTimeRangeKind(CustomKind):
    _time_column: TimeColumn
    # this is deliberately primary_key instead of unique_key to direct away from INCREMENTAL_BY_UNIQUE_KEY
    _primary_key: t.List[exp.Expression]

    @model_validator(mode="after")
    def _validate_model(self):
        self._time_column = TimeColumn.create(
            self.materialization_properties.get("time_column"), dialect=self.dialect
        )

        pk_expressions = list_of_fields_validator(
            self.materialization_properties.get("primary_key"), dict(dialect=self.dialect)
        )
        if not pk_expressions:
            raise ConfigError("`primary_key` must be specified")
        self._primary_key = pk_expressions

        time_column_present_in_primary_key = self.time_column.column in {
            col for expr in self.primary_key for col in expr.find_all(exp.Column)
        }

        if len(self.primary_key) == 1 and time_column_present_in_primary_key:
            raise ConfigError(
                "`primary_key` cannot be just the time_column. Please list the columns that when combined, uniquely identify a row"
            )

        return self

    @property
    def time_column(self) -> TimeColumn:
        return self._time_column

    @property
    def primary_key(self) -> t.List[exp.Expression]:
        return self._primary_key


class NonIdempotentIncrementalByTimeRangeMaterialization(
    CustomMaterialization[NonIdempotentIncrementalByTimeRangeKind]
):
    NAME = "non_idempotent_incremental_by_time_range"

    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        # sanity check
        if "start" not in kwargs or "end" not in kwargs:
            raise SQLMeshError("The snapshot evaluator needs to pass in start/end arguments")

        assert isinstance(model.kind, NonIdempotentIncrementalByTimeRangeKind)
        assert model.time_column

        start: TimeLike = kwargs["start"]
        end: TimeLike = kwargs["end"]

        columns_to_types = model.columns_to_types
        if not columns_to_types or not columns_to_types_all_known(columns_to_types):
            columns_to_types = self.adapter.columns(table_name)

        low, high = [
            model.convert_to_time_column(dt, columns_to_types)
            for dt in make_inclusive(start, end, self.adapter.dialect)
        ]

        def _inject_alias(node: exp.Expression, alias: str) -> exp.Expression:
            if isinstance(node, exp.Column):
                node.set("table", exp.to_identifier(alias, quoted=True))
            return node

        # note: this is a leak guard on the source side that also serves as a merge_filter
        # on the target side to help prevent a full table scan when loading intervals
        betweens = [
            exp.Between(
                this=model.time_column.column.transform(lambda n: _inject_alias(n, alias)),
                low=low,
                high=high,
            )
            for alias in [MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS]
        ]

        self.adapter.merge(
            target_table=table_name,
            source_table=query_or_df,
            columns_to_types=columns_to_types,
            unique_key=model.kind.primary_key,
            merge_filter=exp.and_(*betweens),
        )

    def append(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        **kwargs: t.Any,
    ) -> None:
        self.insert(
            table_name=table_name,
            query_or_df=query_or_df,
            model=model,
            is_first_insert=False,
            **kwargs,
        )
