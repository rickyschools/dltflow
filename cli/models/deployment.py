"""
dltflow.models.deployment.py
------------------------------
This module is basically a copy and paste of the dbx.models.deployment module with some modifications to support
the dltflow deployment configuration file format. All the classes and methods are the same as in the original module,
but with names prefixed with "DLTFlow" and the appropriate return types and annotations added.

There are some additional classes defined to extend existing classes, like Pipeline.

The Pipeline is extended with a tasks attribute, which is an instance of the DLTFlowTasks class.
The DLTFlowTasks class provides an extension to the Databricks Pipeline API structure by allowing
for the definition of PySpark tasks and parameters, as well as custom dependencies needed to
build a DLT Pipeline.

These task objects are used to dynamically build a notebook, that references (via imports) the necessary
code and configuration files. This is the magic sauce to push Python code into a Databricks DLT pipeline.

This pattern allows to maximize the Software Engineering best practices, like DRY, and to keep
the codebase clean and maintainable, while also allowing engineering teams to leverage the power of Databricks
DLT for their pipeline processing needs.
"""
import inspect
import typing as t

from pydantic.v1 import BaseModel, Field, root_validator
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.task import SparkPythonTask
from dbx.models.workflow.common.pipeline import Pipeline
from dbx.models.workflow.common.libraries import PythonPyPiLibrary, mutually_exclusive, at_least_one_of

from dltflow.utils import dbx_echo


class DLTFlowDependency(FlexibleModel):
    """A dependency for a DLTFlow task."""
    whl: t.Optional[str] = None
    pypi: t.Optional[PythonPyPiLibrary] = None

    @classmethod
    @root_validator(pre=True)
    def validate(cls, values):
        """Ensure that at least one of whl or pypi is provided, but not both."""
        print(values)
        assert (values.whl or values.pypi), 'At least one of whl or pypi must be provided.'
        assert not (values.whl and values.pypi), 'whl and pypi are mutually exclusive.'
        return values

    def get_dependency(self):
        """Return the dependency as a string."""
        if self.whl:
            return self.whl
        if self.pypi:
            return self.pypi.package


class DLTFlowSparkPythonTask(SparkPythonTask):
    """A Pydantic model for a DLTFlow PySpark task."""
    python_file: str = Field(..., description='Path to the Python file')
    parameters: t.List[str] = Field(..., description='Parameters for the Python file')

    def provide_items(self):
        """Return the task items as a dictionary."""
        return {'python_file': self.python_file, 'config_path': self.parameters[-1]}


class DLTFlowTasks(BaseModel):
    """A Pydantic model for a DLTFlow tasks object."""
    items: t.List[DLTFlowSparkPythonTask] = Field(default=None, description='List of PySpark tasks')
    dependencies: t.List[DLTFlowDependency] = Field(default=None,
                                                    description='List of dependencies required for the tasks')

    def get_items(self):
        """Return the task items as a list of dictionaries."""
        return [i.provide_items() for i in self.items]

    def get_dependencies(self):
        """Return the dependencies as a list of strings."""
        return [d.get_dependency() for d in self.dependencies]


class DLTFlowPipeline(Pipeline):
    """A Pydantic model for a DLTFlow pipeline."""
    workflow_type: t.Literal['pipeline'] = Field('pipeline', description='Workflow type')
    tasks: DLTFlowTasks = Field(..., description='Tasks to be executed in the pipeline')

    def get_tasks(self):
        """Return the tasks as a list of dictionaries."""
        return self.tasks.get_items()


# load deployment classes
from dbx.models import deployment

deployment_source = inspect.getsource(deployment) \
    .replace('EnvironmentDeploymentInfo', 'DLTFlowEnvironmentDeploymentInfo') \
    .replace('DeploymentConfig', 'DLTFlowDeploymentConfig') \
    .replace(' -> Deployment', ' -> DLTFlowDeployment') \
    .replace('Optional[WorkflowList]', 'Optional[List[DLTFlowPipeline]]')

dbx_echo('⚠️🪄[red]Dynamically refactoring `dbx` deployment pydantic models to support `dltflow`.')

exec(deployment_source.strip())
