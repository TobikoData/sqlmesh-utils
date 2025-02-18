from __future__ import annotations
import typing as t
from sqlmesh import CustomMaterialization
from sqlmesh.core.model import Model
from sqlmesh.core.model.kind import TimeColumn
from sqlglot import exp
from sqlmesh.utils.date import make_inclusive
from sqlmesh.utils.errors import ConfigError, SQLMeshError
import pydantic
from pydantic import field_validator, model_validator, ValidationInfo, BaseModel
from sqlmesh.utils.pydantic import list_of_fields_validator
from sqlmesh.utils.date import TimeLike, to_time_column
from sqlmesh.core.engine_adapter.base import MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS
from typing_extensions import Self


if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import QueryOrDF


class MaterializationProperties(BaseModel):
    # this config is required or we get an error like:
    # Unable to generate pydantic-core schema for <class 'sqlglot.expressions.Expression'>
    model_config = pydantic.ConfigDict(
        arbitrary_types_allowed=True,
    )

    time_column: TimeColumn
    # this is deliberately primary_key instead of unique_key to direct away from INCREMENTAL_BY_UNIQUE_KEY
    primary_key: t.List[exp.Expression]

    _time_column_validator = TimeColumn.validator()

    @field_validator("primary_key", mode="before")
    @classmethod
    def _validate_primary_key(cls, value: t.Any, info: ValidationInfo) -> t.Any:
        expressions = list_of_fields_validator(value, info.data)
        if not expressions:
            raise ConfigError("`primary_key` must be specified")

        return expressions

    @model_validator(mode="after")
    def _inject_time_column_into_primary_key(self):
        time_column_present_in_primary_key = self.time_column.column in {
            col for expr in self.primary_key for col in expr.find_all(exp.Column)
        }

        if len(self.primary_key) == 1 and time_column_present_in_primary_key:
            raise ConfigError(
                "`primary_key` cannot be just the time_column. Please list the columns that when combined, uniquely identify a row"
            )

        return self

    @classmethod
    def from_model(cls: t.Type[Self], model: Model) -> Self:
        return cls.model_validate(
            dict(
                time_column=model.custom_materialization_properties.get("time_column"),
                primary_key=model.custom_materialization_properties.get("primary_key"),
            )
        )


class NonIdempotentIncrementalByTimeRangeMaterialization(CustomMaterialization):
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

        start: TimeLike = kwargs["start"]
        end: TimeLike = kwargs["end"]
        properties = MaterializationProperties.from_model(model)

        time_column_type = model.columns_to_types_or_raise.get(properties.time_column.column.name)
        if not time_column_type:
            raise ConfigError(
                f"Time column '{properties.time_column.column.sql(dialect=model.dialect)}' not found in model '{model.name}'."
            )

        low, high = [
            to_time_column(
                dt,
                time_column_type,
                model.dialect,
                properties.time_column.format,  # todo: we have no access to the project `time_column_format` field here
            )
            for dt in make_inclusive(start, end, model.dialect)
        ]

        def _inject_alias(node: exp.Expression, alias: str) -> exp.Expression:
            if isinstance(node, exp.Column):
                return exp.column(node.this, alias)
            return node

        # note: this is a leak guard on the source side that also serves as a merge_filter
        # on the target side to help prevent a full table scan when loading intervals
        betweens = [
            exp.Between(
                this=properties.time_column.column.transform(lambda n: _inject_alias(n, alias)),
                low=low,
                high=high,
            )
            for alias in [MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS]
        ]

        self.adapter.merge(
            target_table=table_name,
            source_table=query_or_df,
            columns_to_types=model.columns_to_types,
            unique_key=properties.primary_key,
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
