"""
dltflow.quality.dlt_meta.py
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

from dltflow.quality import DLTMetaMixin
from pyspark.pandas.utils import default_session

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
from pyspark.sql import (
    SparkSession,
    DataFrame as SparkDataFrame
)
from pyspark.pandas.utils import default_session

from .config import DLTConfig, DLTConfigs, DLTExecutionConfig
from .exceptions import DLTException

_DANGEROUS_ACTIONS = {
    ".count()",
    ".collect()",
    ".show()",
    ".head()",
    ".take()",
    ".first()",
    ".toPandas()",
}

_UNSUPPORTED_ACTIONS = {".pivot()"}


@pyd.dataclasses.dataclass
class ParameterConfig:
    """
    This class is a pydantic model that is used to dynamically
    configure the parameters of a function. This is used in
    conjunction with the DLTConfig class to enforce the
    parameters of a function.
    """

    required: t.Optional[t.List[str]] = pyd.Field(
        ..., description="List of required parameters from a function."
    )
    optional: t.Optional[t.List[str]] = pyd.Field(
        [], description="List of optional parameters from a function."
    )


class DLTMetaMixin:
    spark: SparkSession = None
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
        Instantiate the class with the provided configuration.

        Parameters
        ----------
        kwargs : dict
            The configuration for the class.

        Returns
        -------
        object
            The instantiated object.
        """
        init_conf = kwargs.get("init_conf")
        obj = super().__new__(cls)
        obj.spark = kwargs.get('spark', SparkSession.getActiveSession() or default_session())
        obj._logger = obj.setup_spark_logger()
        obj._conf = obj._get_dlt_config(init_conf)
        obj._set_write_opts_if_needed(init_conf)
        obj._execution_conf = obj._make_execution_plan(obj._conf)
        obj._set_child_func_attributes()
        return obj

    def setup_spark_logger(self) -> logging.Logger:
        """Configure and return the Spark logger."""
        spark_logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(self.__class__.__name__)
        return spark_logger

    def _set_write_opts_if_needed(self, config):
        """
        Set the write options if the config has a writer attribute and the dlt config doesn't have write_opts.

        Parameters
        ----------
        config : dict
            The configuration for the class.
        """
        if hasattr(config, "writer") and "write_opts" not in config.get("dlt", {}):
            writer_conf = config.get("writer")
            self._conf.write_opts.tablename = writer_conf.get("table")
            self._conf.write_opts.path = writer_conf.get("path")

    def _set_child_func_attributes(self):
        """
        Set the attributes for the child functions based on the execution configuration.
        """
        self._logger.info('Setting child function attributes based on execution plans.')
        for conf in self._execution_conf:
            self._logger.debug(f'Setting child function attributes for {conf.child_func_name}')
            setattr(
                self, conf.child_func_name, self.apply_dlt(conf.child_func_obj, conf)
            )

    def _make_execution_plan(self, configs: DLTConfigs):
        """
        Create an execution plan for the user function.

        Parameters
        ----------
        configs : DLTConfigs
            The configuration for the execution plan.

        Returns
        -------
        list
            The execution plan.
        """
        self._logger.info('Creating execution plan for the user function(s).')
        self._logger.info(f'There are {len(configs)} configurations to apply to our codebase..')
        execution_plan = []
        for config in configs:
            self._logger.debug(f'Dealing with {config.model_dump()}')
            table_or_view_func = dlt.table if config.kind == "table" else dlt.view
            child_func_obj = self._enforce_delta_limitations_and_requirements(
                config.func_name
            )
            _plan = DLTExecutionConfig(
                dlt_config=config,
                table_or_view_func=table_or_view_func,
                child_func_name=config.func_name,
                child_func_obj=child_func_obj,
            )
            execution_plan.append(
                _plan
            )
            self._logger.debug(f'Created execution plan for {config.func_name}.')
        return execution_plan

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
            if "dlt" in config:
                dlt_value = config.get("dlt")
                if isinstance(dlt_value, dict):
                    return [DLTConfig(**dlt_value)]
                elif isinstance(dlt_value, list):
                    return [DLTConfig(**c) for c in dlt_value]
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
        for action in _DANGEROUS_ACTIONS:
            if action in code:
                message = (
                    f"Found dangerous action of `{action}` in the `{self._func_name}` function. "
                    f"Per Databricks' DLT guidelines, these spark commands could have unintended consequences. "
                    f"Please review the documentation for more information on how to handle this. "
                    f"Ignoring this warning could have detrimental effects on quality of the data "
                    f"your team is curating for the business. Tread carefully.\n\n"
                    f"https://docs.databricks.com/en/delta-live-tables/python-ref.html#limitations"
                )
                self._logger.warning(message)

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
        for action in _UNSUPPORTED_ACTIONS:
            if action in str(code):
                msg = (
                    f"Found unsupported action of `{action}` in the `{self._func_name}` function. "
                    f"Please review the documentation for more information on how to handle this. "
                    f"https://docs.databricks.com/en/delta-live-tables/python-ref.html#limitations"
                )
                self._logger.error(msg)
                raise ReferenceError(msg)

    def _return_type_check(self, signature: t.Union[inspect.Signature, t.Callable]):
        """
        Check if the return type of the user function is a Spark DataFrame.

        This method inspects the code and typed return annotations to ensure that the return type
        of the user function is a Spark DataFrame. If no return annotation is provided, a warning
        is issued. If the return type is not a Spark DataFrame, a TypeError is raised.

        Parameters
        ----------
        signature : inspect.Signature
            The signature of the user function.

        Raises
        ------
        TypeError
            If the return type of the user function is not a Spark DataFrame.

        """
        # If no return annotation is provided, we can't enforce the return type.
        if not isinstance(signature, inspect.Signature):
            signature = inspect.signature(signature)
        if not signature.return_annotation:
            warn(
                f"No return type annotation was provided for the `{self._func_name}` function. "
                f"Please ensure that the return type is a Spark DataFrame."
            )
        # If the return type is not a Spark DataFrame, raise a TypeError.
        elif not signature.return_annotation == SparkDataFrame:
            msg = (
                "`dlt` requires that the return type of the function is a Spark DataFrame. "
                "Please ensure to annotate the return type of the function with `spark.sql.DataFrame` class."
            )
            self._logger.error(msg)
            raise TypeError(msg)

    def _enforce_delta_limitations_and_requirements(self, func_name: str) -> t.Callable:
        """
        Checks for dangerous and unsupported code in the user function.

        Parameters
        ----------
        func_name: str
            The name of the user function.

        Returns
        -------
        Callable
            The user function.
        """
        user_func = getattr(self, func_name)
        func_code = inspect.getsource(user_func)

        self._dangerous_code_check(func_code)
        self._unsupported_action_check(func_code)
        self._return_type_check(user_func)

        return user_func

    def table_view_expectation_wrapper(self, child_function, execution_config, *args, *kwargs):
        """
        This method is the "magic" that dynamically and automatically wraps the user function with DLT expectations.

        This is used for non-streaming tables and intermediate views in delta live table pipelines. If the user
        chooses, they can also apply expectations to their dataset.

        Read more about streaming tables in Databricks' documentation:
        https://docs.databricks.com/en/delta-live-tables/python-ref.html#example-define-tables-and-views

        Parameters
        ----------
        child_function: callable
            The child function that will be wrapped with DLT expectations.
        execution_config: DLTExecutionConfig
            The configuration that will be used to execute the DLT expectations.

        Returns
        -------

        """
        self._logger.debug(f'Applying batch table/view DLT functionality to {child_function.__name__}.')
        if execution_config.dlt_config.dlt_expectations:
            self._logger.debug(f'Expectations provided. Applying DLT expectations to {child_function.__name__}.')
            return execution_config.dlt_config.expectation_function(
                execution_config.dlt_config.dlt_expectations
            )(
                execution_config.table_or_view_func(
                    child_function,
                    **execution_config.dlt_config.write_opts.model_dump(exclude_none=True),
                )

            )
        else:
            self._logger.debug(f'Expectations not provided. Applying DLT expectations to {child_function.__name__}.')
            return execution_config.table_or_view_func(
                child_function,
                **execution_config.dlt_config.write_opts.model_dump(exclude_none=True),
            )

    @staticmethod
    def streaming_table_expectation_wrapper(child_function, execution_config):
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
        child_function: callable
            The child function that will be wrapped with DLT expectations.
        execution_config: DLTExecutionConfig
            The configuration that will be used to execute the DLT expectations.

        Returns
        -------

        """
        assert not (
            execution_config.dlt_config.append_config
            and execution_config.dlt_config.apply_chg_config
        ), (
            "When handling streaming tables, the `append_config` or `apply_chg_config` "
            "attributes must be provided in the configuration."
        )
        extra = {}
        if execution_config.dlt_config.dlt_expectations:
            extra = {
                execution_config.dlt_config.expectation_function.__name__: execution_config.dlt_config.dlt_expectations
            }

        dlt.create_streaming_table(
            **execution_config.dlt_config.write_opts.model_dump(exclude_none=True),
            **extra,
        )

        if execution_config.dlt_config.apply_chg_config:
            dlt.apply_changes(
                **execution_config.dlt_config.apply_chg_config.model_dump(
                    exclude_none=True
                ),
            )

            return dlt.view(
                child_function,
                name="streaming_view",
                # **self.read_opts.model_dump(exclude_none=True),
            )
        elif execution_config.dlt_config.append_config:
            return dlt.append_flow(
                child_function,
                **execution_config.dlt_config.append_config.model_dump(
                    exclude_none=True
                ),
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
            """
            A wrapper function that wraps the `child_function` with DLT expectations.

            This function is a decorator that wraps the `child_function` with the DLT expectations that were provided
            in the `execution_config`. The return value of the `child_function` is expected to be a Spark DataFrame.
            If the return value is not a Spark DataFrame, a TypeError will be raised.

            Parameters:
                *args: The positional arguments that will be passed to the `child_function`.
                **kwargs: The keyword arguments that will be passed to the `child_function`.

            Returns:
                The result of the `child_function` wrapped with DLT expectations.

            Raises:
                DLTException: If the provided configuration is not supported.

            """
            self._logger.info(f"Entering wrapped method or {child_function}")

            if not execution_config.dlt_config.is_streaming_table:
                if execution_config.dlt_config.dlt_expectations:
                    self._logger.debug(
                        f'Expectations provided. Applying DLT expectations to {child_function.__name__}.')
                    return execution_config.dlt_config.expectation_function(
                        execution_config.dlt_config.dlt_expectations
                    )(
                        execution_config.table_or_view_func(
                            child_function,
                            **execution_config.dlt_config.write_opts.model_dump(exclude_none=True),
                        )

                    )
                else:
                    self._logger.debug(
                        f'Expectations not provided. Applying DLT expectations to {child_function.__name__}.')
                    return execution_config.table_or_view_func(
                        child_function,
                        **execution_config.dlt_config.write_opts.model_dump(exclude_none=True),
                    )
            elif execution_config.dlt_config.is_streaming_table:
                return self.streaming_table_expectation_wrapper(
                    child_function, execution_config
                )
            else:
                raise DLTException(
                    "The provided configuration is not supported. Please ensure that the configuration is correct."
                )

        return wrapper
