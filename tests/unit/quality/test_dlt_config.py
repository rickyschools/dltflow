"""
tests.unit.quality.test_dlt_config
----------------------------------
This module contains the tests for the DLTConfig class.
"""
import sys
import pytest

from pydantic import ValidationError

sys.path.append("../../../../")

from dltflow.quality.config import DLTConfig, DLTPipelineConfig



def test_dlt_pipeline_config_with_single_dlt_config():
    """Test that the dlt field is either a DLTConfig or a list of DLTConfigs."""
    config = DLTPipelineConfig(dlt=DLTConfig(func_name="orchestrate", name="test", kind="table", expectations=[], expectation_action="allow"))
    assert isinstance(config.dlt, list)
    assert len(config.dlt) == 1
    assert isinstance(config.dlt[0], DLTConfig)


def test_dlt_pipeline_config_with_list_of_dlt_configs():
    """Test that the dlt field is either a DLTConfig or a list of DLTConfigs."""
    config = DLTPipelineConfig(
        dlt=[
            DLTConfig(
                func_name="orchestrate", name="test", kind="table", expectations=[], expectation_action="allow"
            ),
            DLTConfig(
                func_name="orchestrate",name="test2", kind="table", expectations=[], expectation_action="allow"
            )
        ]
    )
    assert isinstance(config.dlt, list)
    assert len(config.dlt) == 2
    assert all(isinstance(dlt, DLTConfig) for dlt in config.dlt)


def test_dlt_pipeline_config_with_invalid_type():
    """Test that the dlt field is either a DLTConfig or a list of DLTConfigs."""
    with pytest.raises(ValidationError):
        DLTPipelineConfig(dlt="not a valid type")


def test_dlt_pipeline_config_with_empty_list():
    """Test that the dlt field is either a DLTConfig or a list of DLTConfigs."""
    with pytest.raises(ValidationError):
        DLTPipelineConfig(dlt=[])
