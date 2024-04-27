"""
tests.unit.cli.test_project_models
----------------------------------
Unit tests for dltflow.cli.models.project module.
"""

from dltflow.cli.models.project import (
    ProjectInfo,
    PathName,
    ProjectElements,
    Workspace,
    Environment,
    ProjectConfig,
)


def test_project_info():
    """Test ProjectInfo class."""
    project_info = ProjectInfo(name="my_project")
    assert isinstance(project_info, ProjectInfo)
    assert project_info.name == "my_project"


def test_path_name():
    """Test PathName class."""
    path_name = PathName(name="conf")
    assert isinstance(path_name, PathName)
    assert path_name.name == "conf"

    p2 = PathName(name="workflows", extra_arg=1)
    assert isinstance(p2, PathName)


def test_project_elements():
    """Test ProjectElements class."""
    project_elements = ProjectElements()
    assert isinstance(project_elements, ProjectElements)
    assert project_elements.code.name == "src"
    assert project_elements.conf.name == "conf"
    assert project_elements.workflows.name == "workflows"


def test_workspace():
    """Test Workspace class."""
    workspace = Workspace(
        host="localhost", root_path="/root", artifact_path="/artifacts"
    )
    assert isinstance(workspace, Workspace)
    assert workspace.host == "localhost"
    assert workspace.root_path == "/root"
    assert workspace.artifact_path == "/artifacts"


def test_environment():
    """Test Environment class."""
    workspace = Workspace(
        host="localhost", root_path="/root", artifact_path="/artifacts"
    )
    environment = Environment(workspace=workspace)
    assert isinstance(environment, Environment)
    assert environment.mode == "development"
    assert environment.default == False
    assert environment.workspace == workspace


def test_project_config():
    """Test ProjectConfig class."""
    workspace = Workspace(
        host="localhost", root_path="/root", artifact_path="/artifacts"
    )
    elements = ProjectElements()
    targets = {"dev": Environment(workspace=workspace)}
    project_config = ProjectConfig(
        dltflow=ProjectInfo(name="my_project"), include=elements, targets=targets
    )
    assert isinstance(project_config, ProjectConfig)
    assert project_config.dltflow.name == "my_project"
    assert project_config.include.code.name == "src"
    assert project_config.targets["dev"].workspace == workspace
    assert project_config.targets["dev"].mode == "development"
    assert project_config.targets["dev"].default == False
