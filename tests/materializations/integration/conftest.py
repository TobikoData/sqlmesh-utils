import pytest
import typing as t
from pathlib import Path
from sqlmesh.core.config import Config
from sqlmesh.core.context import Context
from sqlmesh.core.console import TerminalConsole
from sqlmesh.core.config.loader import load_config_from_yaml
from sqlmesh.core.engine_adapter.base import EngineAdapter
from dataclasses import dataclass
from sqlmesh.core.constants import MODELS
from functools import cached_property
import uuid


# tag all the tests in the integration/ folder with the "integration" marker
# doing it this way means we dont have to manually do it with `pytestmark` in each file
def pytest_collection_modifyitems(items: t.List[pytest.Item]) -> None:
    integration_folder = Path(__file__).parent
    for item in items:
        if item.path.parent == integration_folder:
            item.add_marker(pytest.mark.integration)


@dataclass
class Engine:
    gateway: str
    config: Config

    @cached_property
    def adapter(self) -> EngineAdapter:
        conn = self.config.gateways[self.gateway].connection
        assert conn
        return conn.create_engine_adapter()


@dataclass
class Project:
    engine: Engine
    base_path: Path

    @property
    def models_path(self) -> Path:
        return self.base_path / MODELS

    def write_model(self, filename: str, definition: str) -> None:
        (self.models_path / filename).write_text(definition, encoding="utf8")

    @cached_property
    def context(self) -> Context:
        return Context(paths=self.base_path, config=self.engine.config, gateway=self.engine.gateway)

    @property
    def engine_adapter(self) -> EngineAdapter:
        return self.engine.adapter

    @cached_property
    def test_schema(self) -> str:
        return f"test_{str(uuid.uuid4())[0:8]}"


@pytest.fixture(
    params=[
        pytest.param("trino_delta", marks=pytest.mark.trino),
        pytest.param("postgres", marks=pytest.mark.postgres),
    ]
)
def engine(request) -> Engine:
    config_path = (
        Path(__file__).parent.parent.parent.parent
        / "_sqlmesh_upstream/tests/core/engine_adapter/integration/config.yaml"
    )
    gateway = f"inttest_{request.param}"

    loaded_config = load_config_from_yaml(config_path)
    loaded_config["gateways"] = {k: v for k, v in loaded_config["gateways"].items() if k == gateway}

    # some of the upstream configs use the database under test for the state_connection
    # this hurts parallelism because it causes conflicts so make sure we are always using a local in-memory duckdb for state
    loaded_config["gateways"][gateway]["state_connection"] = {"type": "duckdb"}

    config = Config(**loaded_config, default_gateway=gateway)

    return Engine(gateway=gateway, config=config)


@pytest.fixture
def project(sqlmesh_console: TerminalConsole, engine: Engine, tmpdir) -> t.Iterator[Project]:
    # note: we depend on sqlmesh_console here to init the console even though we dont actually use it directly
    project = Project(engine=engine, base_path=tmpdir)
    project.models_path.mkdir()

    try:
        project.engine_adapter.create_schema(schema_name=project.test_schema, warn_on_error=False)
        yield project
    finally:
        project.engine_adapter.drop_schema(schema_name=project.test_schema, cascade=True)
