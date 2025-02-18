import typing as t
import pytest
from pytest_mock import MockerFixture
from sqlmesh.core.console import configure_console, get_console, TerminalConsole
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlglot import exp, parse_one

TEngineAdapter = t.TypeVar("TEngineAdapter", bound=EngineAdapter)

MockedEngineAdapterMaker = t.Callable[..., EngineAdapter]


@pytest.fixture
def sqlmesh_console() -> TerminalConsole:
    configure_console()
    console = get_console()
    assert isinstance(console, TerminalConsole)
    return console


@pytest.fixture
def make_mocked_engine_adapter(mocker: MockerFixture) -> MockedEngineAdapterMaker:
    def _make_function(
        klass: t.Type[TEngineAdapter],
        dialect: t.Optional[str] = None,
        default_catalog: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> TEngineAdapter:
        connection_mock = mocker.NonCallableMock()
        cursor_mock = mocker.Mock()
        connection_mock.cursor.return_value = cursor_mock
        cursor_mock.connection.return_value = connection_mock
        adapter = klass(
            lambda: connection_mock,
            dialect=dialect or klass.DIALECT,
            default_catalog=default_catalog,
            **kwargs,
        )
        return adapter

    return _make_function


def to_sql_calls(adapter: EngineAdapter, identify: bool = True, **kwargs: t.Any) -> t.List[str]:
    output = []
    for call in adapter.cursor.execute.call_args_list:
        value = call[0][0]
        sql = (
            value.sql(dialect=adapter.dialect, identify=identify, **kwargs)
            if isinstance(value, exp.Expression)
            else parse_one(str(value)).sql(dialect=adapter.dialect, **kwargs)
        )
        output.append(sql)
    return output
