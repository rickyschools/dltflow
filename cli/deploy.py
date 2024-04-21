"""
dltflow.deploy.py
-------------------

The module provides a command line interface to deploy a python module as a DLT pipeline.

In the context of Databricks, a DLT pipeline is a Databricks notebook that runs a pipeline. This package provides
a unique and standalone interface to deploy a python module as a DLT pipeline.

It does so in the following steps:

1. Upload the python package to the Databricks workspace.
    a. The package is uploaded to the workspace in the form of a directory.
    b. This is done to ensure that the package is available to the Databricks cluster.
2. Create a DLT pipeline notebook.
    a. The notebook is created with the necessary imports and configurations.
3. The notebook is deployed to the Databricks workspace.
4. The notebook is registered as a DLT pipeline in the Databricks workspace.
"""

# standard imports
import uuid
import pathlib

# 3rd party imports
import click
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import Language

from dbx.utils.common import prepare_environment
from dbx.api.deployment import WorkflowDeploymentManager
from dbx.models.workflow.common.pipeline import NotebookLibrary, PipelineLibrary

# custom imports
from dltflow.utils import dbx_echo
from dltflow.cli.utils import get_config
from dltflow.cli.reader import ConfigReader
from dltflow.cli.models.project import ProjectConfig
from dltflow.cli.models.deployment import DLTFlowPipeline, DLTFlowDependency

_BASE_NOTEBOOK = """
# Databricks notebook source
{dependencies}
# COMMAND ----------
# add the deployed artifact to the python path
# so DLT can import the module.
import sys
import yaml
import pathlib
sys.path.append('{package_namespace}')
"""

_CODE_IMPORT_AND_LAUNCH_CELLS = """
# COMMAND ----------
# perform necessary imports to load configuration
# and to load the core pipeline code we want to run.
from {import_path} import {class_name}
{config_holder}
# COMMAND ----------
# load the configuration file if it exists
# and run the pipeline.
if config_path is not None: 
    config = yaml.safe_load(pathlib.Path(config_path).read_text())
    pipeline = {class_name}(spark=spark, init_conf=config)
    pipeline.{launch_method}()
"""


def upload_py_package_to_workspace(
        run_id: str,
        dbfs_path: pathlib.Path,
        package_path: pathlib.Path,
        files: list,
        profile: str,
        ws_client: WorkspaceClient = None
):
    """
    Upload a python package to the Databricks workspace. The logic being leveraged by
    this function loosely follows the `dbx` packages methodology of uploading a package, but
    with a simpler coding interface.

    Simplicity was chosen, instead of using `dbx` commands as a way to understand the underlying
    logic of the `dbx` package. At some point in the future, we may switch to using `dbx` commands
    or `dab` commands to upload the package to the Databricks workspace.


    Parameters
    ----------
    run_id
    dbfs_path
    package_path
    files
    profile
    ws_client

    Returns
    -------

    """
    if ws_client is None:
        ws_client = WorkspaceClient(profile=profile.upper())

    upload_dbfs_path = dbfs_path.joinpath('artifacts', run_id)
    if not package_path.resolve().is_dir():
        raise Exception(f"Path {package_path} is not a directory.")

    dbx_echo(f'📦📤 Python artifact upload beginning...')
    dbx_echo(f'Upload directory: [yellow]`{upload_dbfs_path}`')
    ws_client.dbfs.mkdirs(str(upload_dbfs_path))

    for file in files:
        file = pathlib.Path(file).absolute()
        ws_client.dbfs.copy(f"file://{file}/", str(upload_dbfs_path), recursive=True, )
    dbx_echo(f'📦📤 [green]Upload complete!')
    return '/dbfs' + str(upload_dbfs_path)


def get_class_name_from_text(text: str):
    defs = [line for line in text.split('\n') if 'class' in line]
    for i, line in enumerate(defs):
        defs[i] = line.split(' ')[1].split(':')[0].split('(')[0]
    return defs


def get_module_info_from_path_name(project_name, project_path, module_path: str):
    """
    A helper function to get the class name and module name from a module path.


    Parameters
    ----------
    project_name
    project_path
    module_path

    Returns
    -------

    """
    _path = pathlib.Path(module_path)
    ext = _path.suffix
    _module_parts = str(_path)[:-(len(ext))].split('/')
    module_name = '.'.join(_module_parts)
    _mp = _module_parts
    if _mp[0] == project_name:
        _mp = _mp[1:]
    full_path = project_path.joinpath(*_mp).resolve().__str__() + f'{ext}'

    # module = SourceFileLoader(module_name, full_path).load_module()
    classes = get_class_name_from_text(pathlib.Path(full_path).read_text())
    if len(classes) == 1:
        return classes[0], module_name


def create_notebook_and_upload(
        ws_client: WorkspaceClient,
        dbfs_path: pathlib.Path,
        notebook_name: str,
        notebook_src: str,
):
    """
    This function creates a DLT pipeline notebook and uploads it to the Databricks workspace.

    With regards to registering a python module as a DLT pipeline, this might be the most important
    step of that process. The logic for generating the notebook is simple, and this is the core reason
    this package is being developed instead of using something like `dbx` or `dab` commands.

    Notebooks should not be used by developers for their production grade code/pipelines. Rather, they
    should use a proper script/module that can be tested.

    To adhere to this standard, a less than 20 line notebook is generated to run the pipeline. This notebook
    can be simply considered a "driver" or "executor" for the pipeline. No core business should be
    added to the notebook.

    Parameters
    ----------
    ws_client
    dbfs_path
    notebook_name
    notebook_src


    Returns
    -------

    """
    notebook_path = f'/Workspace{str(dbfs_path)}'
    ws_client.workspace.mkdirs(notebook_path)
    ws_client.workspace.upload(
        path=notebook_path + '/' + notebook_name,
        content=notebook_src.encode(),
        overwrite=True,
        language=Language.PYTHON
    )
    full_path = f'{notebook_path}/{notebook_name}'
    dbx_echo(f'🥳🚀 [green]DLT pipeline notebook created.')
    dbx_echo('Notebook path: [yellow]{}'.format(full_path))
    return full_path


def deploy_py_module_as_notebook(
        run_id: str,
        ws_client: WorkspaceClient,
        project_name: str,
        workflow_name: str,
        dbfs_path: pathlib.Path,
        package_path: pathlib.Path,
        items: list,
        dependencies: list = None,
        launch_method: str = 'launch',
):
    """
    This function deploys a python module as a DLT pipeline to the Databricks workspace.

    Parameters
    ----------
    run_id
    ws_client: WorkspaceClient,
    project_name: str,
    workflow_name: str,
    dbfs_path: pathlib.Path,
    package_path: pathlib.Path,
    items: list,
    dependencies: list = None,
    package_namespace: str = None,
    launch_method: str = 'launch'

    Returns
    -------

    """
    dbfs = pathlib.Path('/dbfs')
    package_namespace = dbfs.joinpath(dbfs_path, 'artifacts', run_id)

    deps = ''
    if len(dependencies) > 0:
        # # MAGIC %pip install /dbfs/private-site-packages/databricks_utils-1.5.0b3-py3-none-any.whl
        # dbutils.library.restartPython()
        deps = '# install pipeline dependencies\n'
        deps += '\n'.join([f'# MAGIC %pip install {dep}' for dep in dependencies])
        deps += '\ndbutils.library.restartPython()'  # restart python after installing dependencies

    notebook_source = _BASE_NOTEBOOK.format(
        package_namespace=package_namespace,
        dependencies=deps,
    )

    for item in items:
        cfg = f'config_path = "{str(package_namespace.joinpath(item.get("config_path")))}"'
        class_name, import_path = get_module_info_from_path_name(project_name, package_path, item.get('python_file'))
        notebook_source += _CODE_IMPORT_AND_LAUNCH_CELLS.format(
            class_name=class_name,
            import_path=import_path,
            config_holder=cfg,
            launch_method=launch_method
        )

        _ = upload_py_package_to_workspace(
            run_id=run_id,
            dbfs_path=dbfs_path,
            package_path=package_path,
            files=item.values(),
            profile=ws_client.config.profile,
            ws_client=ws_client
        )

    notebook_name = f'{workflow_name}_DLT_pipeline'
    dbx_echo(f'📝🚀 Creating DLT pipeline notebook from task items/dependencies: {notebook_name}')

    return create_notebook_and_upload(
        ws_client,
        dbfs_path,
        notebook_name,
        notebook_source,
    )


def register_notebook_as_dlt_pipeline(
        ws_client: WorkspaceClient,
        notebook_path: str,
        pipeline_payload: DLTFlowPipeline,
        project_conf,
        as_individual: bool
):  # pragma: no cover
    """
    Register a notebook as a DLT pipeline in the Databricks workspace.

    Parameters
    ----------
    ws_client
    notebook_path
    pipeline_payload

    Returns
    -------

    """
    del pipeline_payload.tasks
    del pipeline_payload.libraries

    _cur_user_name = ws_client.current_user.me().user_name
    _profile_name = ws_client.config.profile.lower()

    prefix = ''
    if 'Shared' not in project_conf.workspace.root_path or as_individual:
        prefix = f'[{_profile_name}_{_cur_user_name}] '

    pipeline_payload.name = f'{prefix}{pipeline_payload.name}'

    notebook = PipelineLibrary(notebook=NotebookLibrary(path=notebook_path))

    pipeline_payload.libraries = [notebook]

    api_client = prepare_environment(ws_client.config.profile.lower())

    wf_manager = WorkflowDeploymentManager(api_client, [pipeline_payload])
    wf_manager.apply()


def handle_deployment_per_file_config_dependency(
        workflow_name,
        items,
        dependencies,
        deployment_config,
        profile,
        as_individual: bool
):
    """
    A helper function to handle the deployment of a python module as a DLT pipeline.

    Parameters
    ----------
    workflow_name
    items
    dependencies
    deployment_config
    profile

    Returns
    -------

    """
    run_id = str(uuid.uuid4())
    dbx_echo(f"Starting deployment for workflow: {workflow_name} -- Assigned artifact ID: {run_id}")
    config: ProjectConfig = get_config()
    project_path = pathlib.Path('.')
    _code_path = project_path.joinpath(config.include.code.name).resolve()
    _cfg_path = project_path.joinpath(config.include.conf.name)
    assert profile in config.targets, f"Profile {profile} not found in config file."
    _target_config = config.targets.get(profile)
    _dbfs_path = pathlib.Path(config.targets.get(profile).workspace.root_path)

    ws_client = WorkspaceClient(profile=profile.upper())

    remote_notebook_path = deploy_py_module_as_notebook(
        run_id=run_id,
        ws_client=ws_client,
        project_name=config.dltflow.name,
        workflow_name=workflow_name,
        dbfs_path=_dbfs_path,
        package_path=_code_path,
        items=items,
        dependencies=dependencies,
        launch_method=config.include.code.dict().get('launch_method', 'launch'),
    )

    register_notebook_as_dlt_pipeline(
        ws_client=ws_client,
        notebook_path=remote_notebook_path,
        pipeline_payload=deployment_config,
        project_conf=_target_config,
        as_individual=as_individual
    )


@click.command('deploy-py-dlt', help='Deploy a DLT pipeline.')
@click.option(
    '--deployment-file', default='deployment.yaml',
    required=True
)
@click.option(
    '--environment', default='DEFAULT',
    required=True
)
@click.option(
    '--as-individual', default=False, is_flag=True, show_default=True,
    help='Overrides project settings. Useful for developers as their experimenting with getting their code '
         'fully function. The impact of this flag is that any derived DLT pipelines created with have a '
         'prefix name of [{profile}_{user_name}] -- this is to not overwrite any existing pipelines with logic '
         'that is not yet fully baked..'
)
def deploy_py_dlt2(deployment_file: str, environment: str, as_individual: bool):
    """Deploy a DLT pipeline from a python module."""
    print('Deployment file:', deployment_file)
    deployment_config = ConfigReader(pathlib.Path(deployment_file).absolute()).get_config()
    env_config = deployment_config.get_environment(environment)
    for workflow in env_config.payload.workflows:
        items = workflow.tasks.get_items()
        deps = [DLTFlowDependency(**dep).get_dependency() for dep in workflow.tasks.dependencies]
        handle_deployment_per_file_config_dependency(workflow.name, items, deps, workflow, environment, as_individual)
