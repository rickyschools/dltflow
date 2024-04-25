"""
tests.unit.cli.test_deploy
--------------------------
This module contains the unit tests for the dltflow.cli.deploy module.

"""
import pathlib
import tempfile
from pathlib import Path

import pytest
from mock import patch

from dltflow.cli.deploy import (
    upload_py_package_to_workspace, get_class_name_from_text,
    get_module_info_from_path_name, create_notebook_and_upload,
    deploy_py_module_as_notebook, register_notebook_as_dlt_pipeline,
    handle_deployment_per_file_config_dependency
)
from dltflow.cli.models.project import (
    ProjectConfig, ProjectInfo, ProjectElements, Environment, Workspace
)


@pytest.fixture
def tmp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield pathlib.Path(tmp_dir)


@pytest.fixture
def project_config():
    """Create a project configuration for testing."""
    return ProjectConfig(
        dltflow=ProjectInfo(name='project_name'),
        include=ProjectElements(),
        targets={
            'test': Environment(
                mode='test',
                workspace=Workspace(host='host', root_path='root_path', artifact_path='artifact_path')
            )
        }
    )


def test_upload_py_package_to_workspace(tmp_dir):
    """Test the upload_py_package_to_workspace function."""
    with patch('dltflow.cli.deploy.WorkspaceClient') as mock_workspace:
        upload_py_package_to_workspace(
            run_id='run_id',
            ws_client=mock_workspace,
            dbfs_path=pathlib.Path('/dbfs_path'),
            package_path=tmp_dir.absolute(),
            profile='test',
            files=['file1.xyz', 'file2.xyz'],
        )
        # one call to make drives if they don't exist.
        assert mock_workspace.dbfs.mkdirs.call_count == 1
        # two calls to copy files
        assert mock_workspace.dbfs.copy.call_count == 2


@pytest.mark.parametrize(
    argnames='code, expected',
    argvalues=[
        (
                "\nclass MyTestClass:\n\tdef __init__(self):\n\t\tpass\n",
                ['MyTestClass']
        ),
        (
                "\nclass MyTestClass:\n\tdef __init__(self):\n\t\tpass\n\nclass MyTestClass2:\n\tdef __init__(self):\n\t\tpass\n",
                ['MyTestClass', 'MyTestClass2']
        )
    ]
)
def test_get_class_name_from_text(code, expected):
    """Test the get_class_name_from_text function."""
    assert get_class_name_from_text(code) == expected


def test_create_notebook_and_upload(tmp_dir):
    """Test the create_notebook_and_upload function."""
    with patch('dltflow.cli.deploy.WorkspaceClient') as mock_workspace:
        create_notebook_and_upload(
            ws_client=mock_workspace,
            dbfs_path=pathlib.Path('/dbfs_path'),
            notebook_name='code',
            notebook_src='import json\n\njson.dumps("{\"x\": 1}")'
        )
        # one call to make drives if they don't exist.
        assert mock_workspace.workspace.mkdirs.call_count == 1
        # one call to copy file
        assert mock_workspace.workspace.upload.call_count == 1


def test_deploy_py_module_as_notebook(

):
    """Test the deploy_py_module_as_notebook function."""
    tmp_file_one = tempfile.NamedTemporaryFile().name
    tmp_file_two = tempfile.NamedTemporaryFile().name
    with patch('dltflow.cli.deploy.WorkspaceClient') as mock_workspace:
        with patch('dltflow.cli.deploy.upload_py_package_to_workspace') as mock_py_package_upload:
            with patch('dltflow.cli.deploy.get_module_info_from_path_name') as mock_module_info:
                mock_module_info.return_value = ('MyTestClass', 'package.fake.obj_name')
                deploy_py_module_as_notebook(
                    run_id='run_id',
                    ws_client=mock_workspace,
                    project_name='project_name',
                    workflow_name='workflow_name',
                    dbfs_path=pathlib.Path('/dbfs_path'),
                    package_path=pathlib.Path('data_platform/python/dltflow/'),
                    items=[
                        {'python_file': tmp_file_one, 'config_path': 'item1'},
                        {'python_file': tmp_file_two, 'config_path': 'item2'}
                    ],
                    dependencies=['dep1', 'dep2'],
                    launch_method='launch'
                )
                # two calls total, one for each object in item.
                # each is uploaded individually to the workspace.
                # this is done to help minimize the amount of code deployed to the cluster
                # as well as to make sure that uploads/deployments are fast.
                # only the files that are needed are uploaded, just for clarity's sake.
                assert mock_module_info.call_count == 2
                assert mock_py_package_upload.call_count == 2


def test_handle_deployment_per_file_config_dependency(project_config):
    """Test the handle_deployment_per_file_config_dependency function."""
    with patch('dltflow.cli.deploy.WorkspaceClient') as mock_workspace:
        with patch('dltflow.cli.deploy.deploy_py_module_as_notebook') as mock_deploy:
            with patch('dltflow.cli.deploy.register_notebook_as_dlt_pipeline') as mock_register:
                with patch('dltflow.cli.deploy.get_config') as mock_get_config:
                    mock_get_config.return_value = project_config
                    handle_deployment_per_file_config_dependency(
                        workflow_name='workflow_name',
                        items=[
                            {'python_file': 'file1', 'config_path': 'item1'},
                            {'python_file': 'file2', 'config_path': 'item2'}
                        ],
                        dependencies=['dep1', 'dep2'],
                        deployment_config={
                            "name": "pipeline_workflow_body_goes_here",
                            "pipeline_type": "this is just for testing"
                        },
                        profile='test',
                        as_individual=False
                    )
                assert mock_workspace.call_count == 1
                assert mock_deploy.call_count == 1
                assert mock_register.call_count == 1


def test_get_module_info_from_path_name(tmp_dir):
    """Test the get_module_info_from_path_name function."""
    pro_name, pro_path, mod_path = 'project_name', Path('project_path'), 'project_path/module.py'
    module_import_path = ".".join(mod_path.split('/')[:-1])
    with patch('dltflow.cli.deploy.pathlib.Path') as mock_path:
        with patch('dltflow.cli.deploy.get_class_name_from_text') as mock_class:
            mock_class.return_value = ['MyTestClass']
            mock_path.read_text.return_value = "\nclass MyTestClass:\n\tpass"
            actual_class, actual_mod = get_module_info_from_path_name(
                project_name=pro_name,
                project_path=pro_path,
                module_path=mod_path
            )
            assert actual_class == 'MyTestClass'
            assert str(pro_path) == f'{module_import_path}'
