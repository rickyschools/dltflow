"""
databricks_utils.quality.dlt.dlt_meta.py
----------------------------------------
This module contains a very slim wrapper around the Databricks Live Tables (DLT) API. The DLTMetaMixin class is a
metaclass that wraps a function with DLT expectations. The DLTConfig class is a pydantic model that is used to
configure the DLTMetaMixin class. The Expectation class is a pydantic model that is used to configure the expectations
that will be checked on the dataset.

The intent of this slim wrapper is to:
1. To provide an alternative way of writing DLT pipelines that is framework-driven and not
    only intended for notebook development.
2. A meta-programming (config driven) approach to authoring DLT pipelines.
3. A pattern that fits my (Ricky Schools') personal style of writing code.

Example Pipeline and Configuration:

```python
cfg = {
    "dlt": {
        "func_name": "orchestrate",
        "kind": "table",
        "expectations": [
            {
                "name": "check_addition",
                "inv": "result > 10"
            }
        ],
        "expectation_action": "allow"
    }
}

from databricks_utils.quality import DLTMetaMixin
from databricks_utils.dataframe import default_session

class MyPipeline(DLTMetaMixin):
    def __init__(self, config: dict):
        self.config = config
        self.spark = default_session()
        super().__init__(config)

    def orchestrate(self):
        df = spark.createDataFrame([(1, 2)], ["a", "b"]) \
            .withColumn("result", F.col("a") + F.col("b"))
        return df


if __name__ == "__main__":
    example = MyPipeline(cfg)
    example.orchestrate()
"""

import inspect
import typing as t
import logging
from functools import wraps
from warnings import warn

import dlt
import pydantic as pyd
from pyspark.sql import DataFrame as SparkDataFrame

from .config import DLTConfig, DLTConfigs, DLTExecutionConfig
from .exceptions import DLTException

_DANGEROUS_ACTIONS = {
    '.count()', '.collect()', '.show()', '.head()', '.take()', '.first()', '.toPandas()'
}

_UNSUPPORTED_ACTIONS = {
    '.pivot()'
}


@pyd.dataclasses.dataclass
class ParameterConfig:
    """
    This class is a pydantic model that is used to dynamically
    configure the parameters of a function. This is used in
    conjunction with the DLTConfig class to enforce the
    parameters of a function.
    """
    required: t.Optional[t.List[str]] = pyd.Field(..., description='List of required parameters from a function.')
    optional: t.Optional[t.List[str]] = pyd.Field([], description='List of optional parameters from a function.')


class DLTMetaMixin:
    """
    A metaclass that wraps a function with DLT expectations. This metaclass is designed
    to be used in conjunction with the DLTConfig class. The DLTConfig class is a pydantic model
    that is used to configure the DLTMetaMixin class. The DLTMetaMixin class is a metaclass that
    wraps a function with DLT expectations.

    The intent of this metaclass is to:

    1. To provide an alternative way of writing DLT pipelines that is framework-driven and not
        only intended for notebook development.
    2. A meta-programming (config driven) approach to authoring DLT pipelines.
    """

    def __new__(cls, *args, **kwargs):
        """
        This method is called when the class is instantiated. It is used to dynamically
        configure the class based on the provided configuration.

        Parameters
        ----------
        args
        kwargs
        """
        if 'init_conf' in kwargs:

            cfg = kwargs.get('init_conf')
            new_obj = super().__new__(cls)
            new_obj._logger = logging.getLogger(__name__)
            new_obj._conf = new_obj._get_dlt_config(cfg)
            if hasattr(cfg, 'writer') and 'write_opts' not in cfg.get('dlt', {}):
                writer_conf = cfg.get('writer')
                new_obj._conf.write_opts.tablename = writer_conf.get('table')
                new_obj._conf.write_opts.path = writer_conf.get('path')
            new_obj._execution_conf = new_obj._make_execution_plan(new_obj._conf)
            for conf in new_obj._execution_conf:
                setattr(new_obj, conf.child_func_name, new_obj.apply_dlt(conf.child_func_obj, conf))
            return new_obj

    def _make_execution_plan(self, configs: DLTConfigs):
        """
        This method is used to create an execution plan for the user function.

        Parameters
        ----------
        child_function

        Returns
        -------

        """
        plans = []
        for config in configs:
            plans.append(
                DLTExecutionConfig(
                    dlt_config=config,
                    table_or_view_func=dlt.table if config.kind == "table" else dlt.view,
                    child_func_name=config.func_name,
                    child_func_obj=self._enforce_delta_limitations_and_requirements(config.func_name),
                )
            )
        return plans

    @staticmethod
    def _get_dlt_config(config) -> DLTConfigs:
        """
        A static method to get the DLTConfig object from the provided configuration.

        Parameters
        ----------
        config

        Returns
        -------
        DLTConfig
            The DLTConfig object that was created from the provided configuration.

        """
        if isinstance(config, dict):
            if 'dlt' in config:
                if isinstance(config.get('dlt'), dict):
                    return [DLTConfig(**config.get('dlt'))]
                elif isinstance(config.get('dlt'), list):
                    return [DLTConfig(**c) for c in config.get('dlt')]
        elif issubclass(config, pyd.BaseModel):
            if hasattr(config, "dlt"):
                return [DLTConfig(**config.dlt.model_dump())]

    def _dangerous_code_check(self, code: str):
        """
        This method checks for dangerous code (defined according to Databricks' DLT guidelines) in
        the user-provided code/function.
        Parameters
        ----------
        code

        Returns
        -------

        """
        for dangerous_action in _DANGEROUS_ACTIONS:
            if dangerous_action in str(code):
                warn(
                    f"Found dangerous action of `{dangerous_action}` in the `{self._func_name}` function. "
                    f"Per Databricks' DLT guidelines, these spark commands could have unintended consequences. "
                    f"Please review the documentation for more information on how to handle this. "
                    f"Ignoring this warning could have detrimental effects on quality of the data "
                    f"your team is curating for the business. Tread carefully.\n\n"
                    f""
                    f"https://docs.databricks.com/en/delta-live-tables/python-ref.html#limitations"
                )

    def _unsupported_action_check(self, code: str):
        """
        This method checks for unsupported code (defined according to Databricks' DLT guidelines) in
        the user-provided code/function.
        Parameters
        ----------
        code

        Returns
        -------

        """
        for unsupported_action in _UNSUPPORTED_ACTIONS:
            if unsupported_action in str(code):
                error_message = (
                    f"Found unsupported action of `{unsupported_action}` in the `{self._func_name}` function. "
                    f"Per Databricks' DLT guidelines, these spark commands are not supported. "
                    f"Please review the documentation for more information on how to handle this. "
                    f"Ignoring this warning could have detrimental effects on quality of the data "
                    f"your team is curating for the business. Tread carefully.\n\n"
                    f""
                    f"https://docs.databricks.com/en/delta-live-tables/python-ref.html#limitations"
                )
                print(error_message)
                raise ReferenceError(error_message)

    def _return_type_check(self, signature):
        """
        This method aims to ensure that the return type of the user function is a Spark DataFrame.
        This is done by inspecting the code and typed return annotations.

        Parameters
        ----------
        signature

        Returns
        -------

        """
        if not signature.return_annotation:  # If no return annotation is provided, we can't enforce the return type.
            warn(
                f"No return type annotation was provided for the `{self._func_name}` function. "
                f"Please ensure that the return type is a Spark DataFrame."
            )
        elif not signature.return_annotation == SparkDataFrame:
            msg = ("`dlt` requires that the return type of the function is a Spark DataFrame. Please ensure to "
                   "annotate the return type of the function with `spark.sql.DataFrame` class.")
            raise TypeError(msg)

    def _enforce_delta_limitations_and_requirements(self, func_name: str) -> t.Callable:
        """
        This method checks for dangerous code (defined according to Databricks' DLT guidelines) in the provided code.

        This is done by grabbing the user function by name and inspecting the source code.

        https://docs.databricks.com/en/delta-live-tables/python-ref.html#limitations

        Parameters
        ----------
        func_name: str
            The source code of the user function.

        Returns
        -------
            None

        """
        _func = getattr(self, func_name)
        signature = inspect.signature(_func)
        code = inspect.getsource(_func)
        self._dangerous_code_check(code)
        self._unsupported_action_check(code)
        self._return_type_check(signature)
        return _func

    def table_view_expectation_wrapper(self, child_function, execution_config):
        """
        This method is the "magic" that dynamically and automatically wraps the user function with DLT expectations.

        This is used for non-streaming tables and intermediate views in delta live table pipelines. If the user
        chooses, they can also apply expectations to their dataset.

        Read more about streaming tables in Databricks' documentation:
        https://docs.databricks.com/en/delta-live-tables/python-ref.html#example-define-tables-and-views

        Parameters
        ----------
        child_function

        Returns
        -------

        """
        _table_wrapper = execution_config.table_or_view_func(
            child_function,
            **execution_config.dlt_config.write_opts.model_dump(exclude_none=True)
        )

        if execution_config.dlt_config.dlt_expectations:
            _table_wrapper = execution_config.dlt_config.expectation_function(
                execution_config.dlt_config.dlt_expectations)(
                _table_wrapper)

        return _table_wrapper

    def streaming_table_expectation_wrapper(self, child_function, execution_config):
        """
        This method is a magic wrapper that applies streaming dlt operations to user queries/functions.

        This is used for streaming tables in delta live table pipelines. If the user chooses, they can also apply
        expectations to their dataset.

        Read more about streaming tables in Databricks' documentation:
        https://docs.databricks.com/en/delta-live-tables/python-ref.html

        Append Flow:
        https://docs.databricks.com/en/delta-live-tables/python-ref.html#write-to-a-streaming-table-from-multiple-source-streams

        Apply Changes:
        https://docs.databricks.com/en/delta-live-tables/python-ref.html#write-to-a-streaming-table-from-multiple-source-streams
        https://docs.databricks.com/en/delta-live-tables/cdc.html

        Parameters
        ----------
        child_function

        Returns
        -------

        """
        assert not (execution_config.dlt_config.append_config and execution_config.dlt_config.apply_chg_config), (
            'When handling streaming tables, the `append_config` or `apply_chg_config` '
            'attributes must be provided in the configuration.'
        )
        extra = {}
        if execution_config.dlt_config.dlt_expectations:
            extra = {
                execution_config.dlt_config.expectation_function.__name__: execution_config.dlt_config.dlt_expectations}

        dlt.create_streaming_table(
            **execution_config.dlt_config.write_opts.model_dump(exclude_none=True),
            **extra
        )

        if execution_config.dlt_config.apply_chg_config:
            dlt.apply_changes(
                **execution_config.dlt_config.apply_chg_config.model_dump(exclude_none=True),
            )

            return dlt.view(
                child_function,
                name='streaming_view',
                # **self.read_opts.model_dump(exclude_none=True),
            )
        elif execution_config.dlt_config.append_config:
            return dlt.append_flow(
                child_function,
                **execution_config.dlt_config.append_config.model_dump(exclude_none=True),
            )

    def apply_dlt(self, child_function, execution_config: DLTExecutionConfig):
        """
        This method is the "magic" that dynamically and automatically wraps the user function with DLT expectations.

        The method is a decorator that wraps the user function (specified in configuration) with the DLT
        expectations that were provided in the configuration. The return value of the user function is
        expected to be a Spark DataFrame. If the return value is not a Spark DataFrame,
        a TypeError will be raised.

        Parameters
        ----------
        child_function: t.Callable
            The child function that will be wrapped with DLT expectations.

        execution_config: DLTExecutionConfig
            The configuration that will be used to execute the DLT expectations.

        Returns
        -------

        """

        @wraps(child_function)
        def wrapper(*args, **kwargs):
            self._logger.info(f'Entering wrapped method or {child_function}')

            if not execution_config.dlt_config.is_streaming_table:
                return self.table_view_expectation_wrapper(child_function, execution_config)
            elif execution_config.dlt_config.is_streaming_table:
                return self.streaming_table_expectation_wrapper(child_function, execution_config)
            else:
                raise DLTException(
                    'The provided configuration is not supported. Please ensure that the configuration is correct.'
                )

        return wrapper
