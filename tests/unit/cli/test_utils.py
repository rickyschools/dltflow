"""
tests.unit.cli.test_utils
-------------------------
Unit tests for dltflow.cli.utils module.

"""

import os
import json
import tempfile

import pytest
from unittest.mock import patch

from dltflow.cli.utils import get_config, write_config
from dltflow.cli.models.project import ProjectConfig


@pytest.fixture
def cfg():
    """Return a sample project config."""
    return {
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


def test_get_config(cfg):
    """Test get_config function."""
    # mock read path

    temp_file = tempfile.NamedTemporaryFile(delete=False)
    with open(temp_file.name, "w") as f:
        f.write(json.dumps(cfg))

    with patch("dltflow.cli.utils.dltflow_config_path", temp_file.name):
        assert get_config() == ProjectConfig(**cfg)


def test_write_config(cfg):
    """Test write_config function."""
    # mock write path
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    with patch("dltflow.cli.utils.dltflow_config_path", temp_file.name):
        write_config(cfg)
        assert os.path.exists(temp_file.name)
        written_cfg = get_config()
        assert written_cfg == ProjectConfig(**cfg)
