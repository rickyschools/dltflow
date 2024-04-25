"""
tests.unit.cli.test_deployment_models
-------------------------------------
Unit tests for dltflow.cli.models.deployment module.
"""

from dltflow.cli.models.deployment import (
    DLTFlowDependency, DLTFlowSparkPythonTask, DLTFlowTasks, DLTFlowPipeline
)


def test_dltflow_dependency():
    """Test DLTFlowDependency class."""
    dep = DLTFlowDependency(whl="test.whl")
    assert dep.get_dependency() == "test.whl"

    dep = DLTFlowDependency(pypi={"package": "test"})
    assert dep.get_dependency() == "test"


def test_dltflow_spark_python_task():
    """Test DLTFlowSparkPythonTask class."""
    task = DLTFlowSparkPythonTask(python_file="test.py", parameters=["--conf", "param2"])
    assert task.provide_items() == {'python_file': "test.py", 'config_path': "param2"}


def test_dltflow_tasks():
    """Test DLTFlowTasks class."""
    deps = [DLTFlowDependency(whl="test1.whl"), DLTFlowDependency(pypi={"package": "test2"})]
    tasks = DLTFlowTasks(
        items=[
            DLTFlowSparkPythonTask(python_file="test1.py", parameters=["--conf", "param1"]),
            DLTFlowSparkPythonTask(python_file="test2.py", parameters=["--conf", "param2"])
        ],
        dependencies=deps
    )
    assert tasks.get_items() == [
        {'python_file': "test1.py", 'config_path': "param1"},
        {'python_file': "test2.py", 'config_path': "param2"}
    ]
    assert tasks.get_dependencies() == ["test1.whl", "test2"]


def test_dltflow_pipeline():
    """Test DLTFlowPipeline class."""
    pipeline = DLTFlowPipeline(
        name="test",
        tasks=DLTFlowTasks(
            items=[
                DLTFlowSparkPythonTask(python_file="test1.py", parameters=["--conf", "param1"]),
                DLTFlowSparkPythonTask(python_file="test2.py", parameters=["--conf", "param2"])
            ],
            dependencies=[
                DLTFlowDependency(whl="test1.whl"),
                DLTFlowDependency(pypi={"package": "test2"})
            ]
        )
    )
    assert pipeline.get_tasks() == [
        {'python_file': "test1.py", 'config_path': "param1"},
        {'python_file': "test2.py", 'config_path': "param2"}
    ]
    assert pipeline.tasks.get_dependencies() == ["test1.whl", "test2"]
    assert pipeline.name == "test"
    assert pipeline.workflow_type == "pipeline"
    assert pipeline.tasks.items[0].python_file == "test1.py"
    assert pipeline.tasks.items[1].parameters == ["--conf", "param2"]
    assert pipeline.tasks.dependencies[0].whl == "test1.whl"
    assert pipeline.tasks.dependencies[1].pypi.package == "test2"
