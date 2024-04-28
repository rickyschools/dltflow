"""
test_initialize.py
------------------

Test the initialization of a project using the `init` function.
"""
import pathlib
from tempfile import TemporaryDirectory

import yaml
import pytest
from pydantic import ValidationError
from click.testing import CliRunner

from dltflow.cli.initialize import init
from dltflow.cli.utils import get_config
from dltflow.cli.models.project import ProjectConfig, ProjectInfo, ProjectElements, PathName


@pytest.fixture
def mock_config(monkeypatch):
    # Mock the _parse_databricks_cfg function to return a mock config
    def mock_parse_config():
        return {
            "DEFAULT": {"host": "localhost"},
            "PROFILE1": {"host": "example.com"},
        }

    monkeypatch.setattr("dltflow.cli.initialize._parse_databricks_cfg", mock_parse_config)


@pytest.fixture(autouse=True, scope="module")
def tmpdir():
    with TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture(autouse=True, scope="module")
def mock_config_file(tmpdir):
    cfg = {
        "dltflow": {
            "name": "test",
        },
        "include": {
            "code": {"name": "src"},
            "conf": {"name": "conf"},
            "workflows": {"name": "workflows"},
        },
        "targets": {
            "test": {
                "default": False,
                "mode": "dev",
                "workspace": {
                    "artifact_path": "artifacts",
                    "host": "localhost",
                    "root_path": "/tmp",
                },
            }
        },
    }

    config_file = pathlib.Path(tmpdir).joinpath("dltflow.yml")
    with open(config_file, "w") as f:
        yaml.safe_dump(cfg, f)


def test_init_creates_config_file(tmpdir, mock_config):
    """Test that the `init` function creates a config file in the specified directory."""
    runner = CliRunner()
    result = runner.invoke(init)
    config_file = pathlib.Path(tmpdir).joinpath("dltflow.yml")
    assert config_file.exists()
