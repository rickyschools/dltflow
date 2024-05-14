"""
tests.quality.test_dlt_mixin
----------------------------
This module contains the tests for the DLTMetaMixin class.

"""

import sys
import pytest

from unittest.mock import patch
from pyspark.sql import DataFrame, types as t

IntegerType = t.IntegerType

sys.path.append("../../../../")

from tests.fixtures import spark
import dlt

dlt.enable_local_execution()

from dltflow.quality import (
    DLTMetaMixin,
)  # replace "your_module" with the actual module name
from dltflow.quality.config import (
    AppendFlowConfig,
    ApplyChangesConfig,
    TableWriteOpts,
    DLTConfig,
    DLTConfigs,
)
from dltflow.quality.exceptions import DLTException


# Define a fixture for the pipeline configuration
@pytest.fixture
def pipeline_config():
    """A fixture for the pipeline configuration."""
    return {
        "dlt": [
            {
                "func_name": "orchestrate",
                "kind": "table",
                "expectations": [
                    {"name": "check_addition", "constraint": "result < 10"}
                ],
                "expectation_action": "allow",
            }
        ]
    }

@pytest.fixture
def streaming_pipeline_config():
    """A fixture for the streaming pipeline configuration."""
    return {
        "dlt": [
            {
                "func_name": "orchestrate",
                "kind": "table",
                "expectations": [
                    {"name": "check_addition", "constraint": "result < 10"}
                ],
                "expectation_action": "allow",
                "is_streaming_table": True,
                "write_opts": {
                    "name": "test",
                }
            }
        ]
    }


class MyPipeline(DLTMetaMixin):  # pragma: no cover
    """A dummy pipeline class."""

    _df = None

    def __init__(self, spark_=None, init_conf=None):
        self.conf = init_conf
        self.spark = spark_

    def orchestrate(self) -> DataFrame:  # pragma: no cover
        """A dummy orchestration method."""
        df = self._df
        return df

    def init_adapter(self, df=None):  # pragma: no cover
        if df:
            self._df = df


# dummy data
@pytest.fixture
def sample_df(spark):  # pragma: no cover
    """A fixture for a sample DataFrame."""
    return spark.createDataFrame([(1, 2, 3)], ["a", "b", "result"])


# Define a fixture for the pipeline instance
@pytest.fixture
def pipeline_instance(pipeline_config, spark, sample_df):
    """A fixture for the pipeline instance."""
    p = MyPipeline(init_conf=pipeline_config)
    p.init_adapter(df=sample_df)
    return p


def test_pipeline_init_config(pipeline_instance):
    """This test checks that the pipeline instance is initialized correctly."""
    assert callable(pipeline_instance._execution_conf[0].table_or_view_func)


# Define a test for the orchestrate method
def test_orchestrate(pipeline_instance, sample_df):
    """This test checks that the orchestrate method is working correctly."""
    with patch.object(
        pipeline_instance._execution_conf[0].dlt_config,
        "_expectation_function",
        autospec=True,
    ) as mock_expect:
        with patch.object(
            pipeline_instance._execution_conf[0], "table_or_view_func", autospec=True
        ) as mock_table:
            mock_table.return_value = sample_df
            mock_expect.return_value = lambda *args, **kwargs: sample_df
            df = pipeline_instance.orchestrate()
            assert df.count() == 1
            assert df.columns == ["a", "b", "result"]
            assert df.first()["result"] == 3


# Define a test for the apply_dlt method
def test_apply_dlt(pipeline_instance, sample_df):
    """This test checks that the apply_dlt method is working correctly."""
    with patch.object(
        pipeline_instance._execution_conf[0].dlt_config,
        "_expectation_function",
        autospec=True,
    ) as mock_expect:
        with patch.object(
            pipeline_instance._execution_conf[0], "table_or_view_func", autospec=True
        ) as mock_table:
            mock_table.return_value = sample_df
            mock_expect.return_value = lambda *args, **kwargs: sample_df
            df = pipeline_instance.orchestrate()
            assert df.count() == 1
            assert df.columns == ["a", "b", "result"]
            assert df.first()["result"] == 3


# Define a test for the DLT calls
def test_dlt_calls_non_streaming_table(pipeline_instance):
    """
    This test checks that the DLT calls are being made correctly.
    Parameters
    ----------
    pipeline_instance

    Returns
    -------

    """
    with patch.object(
        pipeline_instance._execution_conf[0].dlt_config,
        "_expectation_function",
        autospec=True,
    ) as mock_expect:
        with patch.object(
            pipeline_instance._execution_conf[0], "table_or_view_func", autospec=True
        ) as mock_table:
            out_df = pipeline_instance.orchestrate()
            mock_table.assert_called()
            mock_expect.assert_called()


def test_dlt_calls_streaming_table_append_flow(
    pipeline_instance, pipeline_config
):  # pragma: no cover
    """
    This test checks that the DLT calls are being made correctly.
    Parameters
    ----------
    pipeline_instance

    Returns
    -------

    """
    pipeline_config["dlt"][0]["is_streaming_table"] = True
    pipeline_config["dlt"][0]["append_config"] = AppendFlowConfig(
        target="dummy"
    ).model_dump()
    pipeline_config['dlt'][0]['write_opts'] = TableWriteOpts(name='test').model_dump()
    pipeline_instance = MyPipeline(init_conf=pipeline_config)

    with patch("dltflow.quality.dlt_meta.dlt.create_streaming_table") as mock_expect:
        with patch("dltflow.quality.dlt_meta.dlt.append_flow") as mock_flow:
            out_df = pipeline_instance.orchestrate()
            mock_flow.assert_called()
            mock_expect.assert_called()


def test_dlt_calls_streaming_table_apply_changes(
    pipeline_instance, pipeline_config
):  # pragma: no cover
    """
    This test checks that the DLT calls are being made correctly.
    Parameters
    ----------
    pipeline_instance

    Returns
    -------

    """
    pipeline_config["dlt"][0]["is_streaming_table"] = True
    pipeline_config["dlt"][0]["apply_chg_config"] = ApplyChangesConfig(
        target="dummy", source="source", keys=["x", "y"]
    ).model_dump()
    pipeline_config['dlt'][0]['write_opts'] = TableWriteOpts(name='test').model_dump()

    with patch("dltflow.quality.dlt_meta.dlt.create_streaming_table") as mock_streaming_table:
        pipeline_instance = MyPipeline(init_conf=pipeline_config)
        assert mock_streaming_table.call_count == 1

        with patch("dltflow.quality.dlt_meta.dlt.apply_changes") as mock_apply_changes:
            out_df = pipeline_instance.orchestrate()
            mock_apply_changes.assert_called()



def test_dlt_calls_streaming_fails(
    pipeline_config, pipeline_instance, spark, sample_df
):  # pragma: no cover
    pipeline_config["dlt"][0]["is_streaming_table"] = True
    pipeline_config["dlt"][0]["apply_chg_config"] = ApplyChangesConfig(
        target="dummy", source="source", keys=["x", "y"]
    ).model_dump()
    pipeline_config["dlt"][0]["append_config"] = AppendFlowConfig(
        target="dummy"
    ).model_dump()

    with pytest.raises(DLTException):
        pipeline_instance = MyPipeline(init_conf=pipeline_config)
