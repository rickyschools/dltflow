"""
dltflow.initialize.py
-----------------------
This module contains the `init` command for the dltflow cli. This command is used to help initialize a dltflow
project. At a minimum, it will create a `dltflow` config file in the current directory.

The code, configuration, and workflow directory names can be customized by the user. The default values are:
- code: `my_project`
- config: `conf`
- workflows: `workflows`

When these directories are created during initialization, a `.gitkeep` file is created in each directory. This is
to ensure that the directories are included in the git repository. Also, a `setup.py` and `pyproject.toml` file are
created in the root directory. These files are used to help package the project as a python package.

The `pyproject.toml` file is setup to use the `bumpver` package to help manage versioning.
"""

import os
import configparser

import click
from pydantic import ValidationError
from databricks.sdk import WorkspaceClient

from dltflow.utils import dbx_echo
from dltflow.cli.models.project import (
    ProjectConfig,
    ProjectInfo,
    ProjectElements,
    Workspace,
    Environment,
    PathName
)
from dltflow.cli.utils import get_config, write_config, CLI_NAME

CONFIG_FILE_ENV_VAR = "DATABRICKS_CONFIG_FILE"
_home = os.path.expanduser("~")


def _get_path():  # pragma: no cover
    """Get the path to the databricks config file."""
    return os.environ.get(CONFIG_FILE_ENV_VAR, os.path.join(_home, '.databrickscfg'))


def _parse_databricks_cfg():  # pragma: no cover
    """Parse the databricks config file."""
    with open(_get_path(), 'r') as f:
        data = f.read()
    config = configparser.ConfigParser()
    config.read_string(data)
    return config


def _environments_from_databricks_profile(username, project_name, shared):  # pragma: no cover
    """Get the environments from the databricks profile."""
    config = _parse_databricks_cfg()
    for section in config.sections():
        if section == 'DEFAULT':
            continue

        root_path = f"/Users/{username}/.{CLI_NAME}/{project_name}" if not shared \
            else f"/Shared/.{CLI_NAME}/{project_name}"

        yield section.lower(), Environment(
            mode=section.lower(),
            default=config[section].get('default', 'false'),
            workspace=Workspace(
                host=config[section].get('host', 'databricks'),
                root_path=config[section].get('root_path', root_path),
                artifact_path=config[section].get('artifact_path', fr"dbfs:{root_path}")
            )
        )


@click.command()
@click.option('--profile', '-p', help='Databricks profile to use', default='DEFAULT')
@click.option('--project-name', '-n', help='Name of the project', default='my_project')
@click.option('--config-path', '-c', help='Path to configuration directory', default='conf')
@click.option('--workflows-path', '-w', help='Path to workflows directory', default='workflows')
@click.option('--build-template', '-t', help='Create a templated project?', is_flag=True, default=False)
@click.option('--overwrite', '-o', help='Overwrite existing config file', is_flag=True, default=True, show_default=True)
@click.option('--shared', '-d', help='DBFS location to store the project', is_flag=True, default=True,
              show_default=True)
def init(
        profile: str,
        project_name: str = 'my_project',
        config_path: str = 'conf',
        workflows_path: str = 'workflows',
        build_template: bool = False,
        overwrite: bool = True,
        dbfs_location: str = None,
        shared: bool = True
):  # pragma: no cover
    """
    Initialize a new dltflow project.

    This cli command is used to help initialize a dltflow project. At a minimum, it will create a `dltflow` config
    file in the current directory.

    The code, configuration, and workflow directory names can be customized by the user. The default values are:
    - code: `my_project`
    - config: `conf`
    - workflows: `workflows`

    When these directories are created during initialization, a `.gitkeep` file is created in each directory. This is
    to ensure that the directories are included in the git repository. Also, a `setup.py` and `pyproject.toml` file are
    created in the root directory. These files are used to help package the project as a python package.

    The `pyproject.toml` file is setup to use the `bumpver` package to help manage versioning.

    If the user opts in to include directories, dltflow will create the following structure:


        ```text
        git-root/
            my_project/  # code goes here.
            conf/  # configuration to drive your pipelines.
            workflows/  # json or yml definitions for workflows in databricks.
            dltflow.yml  # dltflow config file.
            setup.py  # setup file for python packages.
            pyproject.toml  # pyproject file for python packages.
        ```

    Parameters
    ----------
    profile: str
        Databricks profile to use
    project_name: str

    config_path: str

    workflows_path: str

    build_template: bool

    overwrite: bool
        True

    Returns
    -------

    """
    dbx_echo(f"Initializing dltflow project configuration.")

    client = WorkspaceClient(profile=profile.upper())
    user_name = client.current_user.me().user_name

    try:
        cfg = get_config()
    except ValidationError:
        dbx_echo(
            "🚯[red]`dltflow` configuration isn't compatible with the current version / project configuration style. "
            "Please downgrade or update the configuration file. You will be prompted if you want to overwrite "
            "the existing configuration file.")
        cfg = {}
    _build = overwrite

    if cfg:
        dbx_echo("☠️🧐[yellow]`dltflow` configuration exists from a prior version. Overwriting "
                 "can be potentially dangerous.")
        cfg = cfg.dict() if isinstance(cfg, ProjectConfig) else cfg
        _build = click.confirm("Do you want to overwrite the existing `dltflow` config file?")

    if _build:
        project_name = click.prompt(
            "Project name. This will be where your pyspark code will live."
            , default=cfg.get('dltflow', {}).get('name', 'my_project')
        )
        project_info = ProjectInfo(name=project_name)
        code_path = click.prompt(
            "Code path. This is the directory where your pyspark code will live.",
            default=cfg.get('include', {}).get('code', {}).get('name', project_name)
        )
        code_launch_method = click.prompt(
            "Code launch method. This is the method to run your pipelines. Useful to denote here for DLT pipelines.",
            default='launch'
        )
        config_path = click.prompt(
            "Config path. This is the directory where configuration for pipelines will live.",
            default='conf'
        )
        workflows_path = click.prompt(
            "Workflows path. This is where json or yml definitions for workflows in databricks will live.",
            default='workflows'
        )
        shared = click.prompt(
            f"Is deployment shared or personal? We will show you where your stuff will get stored later. 😉",
            default='personal',
            show_choices=True,
            type=click.Choice(['shared', 'personal'])

        )
        add_include_dir = click.confirm(
            "Do you want to add `include` directories? (e.g. `config`, `code`, `workflows`.)",
            default=False
        )
        project_elements = ProjectElements(
            code=PathName(name=code_path, launch_method=code_launch_method),
            conf=PathName(name=config_path),
            workflows=PathName(name=workflows_path)
        )
        shared = True if shared == 'shared' else False
        targets = dict(_environments_from_databricks_profile(user_name, project_name, shared))
        project_config = ProjectConfig(
            dltflow=project_info,
            include=project_elements,
            targets=targets
        )
        write_config(project_config.dict())

        if add_include_dir:
            for dir_name in [project_name, config_path, workflows_path]:
                os.makedirs(dir_name, exist_ok=True)
                with open(os.path.join(dir_name, '.gitkeep'), 'w') as f:
                    f.write('')

            with open('setup.py', 'w') as f:
                f.write(
                    f"""from setuptools import setup, find_packages

setup(
    name='{project_name}',
    version='0.1',
    packages=find_packages(),
)
"""
                )

            with open('pyproject.toml', 'w') as f:
                f.write(
                    f"""[build-system]
requires = ["setuptools", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "{project_name}"
version = "0.0.1"
description = "A project for PySpark pipelines. Templated by `dltflow`."
src = "{project_name}"
include = ["conf", "workflows"]


[bumpver]
current_version = "0.0.1"
version_pattern = "MAJOR.MINOR.PATCH[PYTAGNUM]"

[bumpver.file_patterns]
"setup.py" = [
    'version="{{version}}"',
]
"pyproject.toml" = [
"version = '{{version}}'",
'current_version = "{{version}}"', # bumpver
]
"""
                )
        dbx_echo(f"👨‍💻📔[green]`dltflow` configuration file created successfully")
