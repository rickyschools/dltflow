"""
dltflow.utils.py
------------------
This module contains utility functions for working with dltflow configuration files.
"""

import os
from typing import Union

import yaml

from dltflow.cli.models.project import ProjectConfig

CLI_NAME = "dltflow"
dltflow_config_path = "dltflow.yml"


def get_config() -> Union[ProjectConfig, None]:
    """
    Reads the dltflow configuration file and returns a ProjectConfig object.
    If the configuration file does not exist, returns None.
    """
    if os.path.exists(dltflow_config_path):
        return ProjectConfig(**yaml.safe_load(open(dltflow_config_path)))
    return None


def write_config(config: dict):
    """Helper function to write a configuration dictionary to a file."""
    with open(dltflow_config_path, "w") as f:
        yaml.safe_dump(config, f)
