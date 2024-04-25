"""
dltflow.quality.config.py
---------------------------------------------
This module contains the pydantic models for the DLT configuration. The Expectation class is a pydantic model that is
used to declare the expectations that will be checked on the dataset.

supported yaml or json configuration patterns:

```yaml
...
dlt:
    func_name: "write"
    kind: "table"
    expectations:
        - name: "valid_count"
          constraint: "count > 1"
    expectation_action: "drop"
...
```

```json
{
    "dlt": {
        "func_name": "write",
        "kind": "{table | view}",
        "expectations": [
            {
                "name": "valid_count",
                "constraint": "count > 1"
            }
        ],
        "expectation_action": "{allow | fail | drop}"
    }
}
```
"""
import inspect
import typing as t
from collections import ChainMap

import dlt
import pydantic as pyd

from .exceptions import DLTException

_expectation_map = {
    "allow": dlt.expect_all,
    "fail": dlt.expect_all_or_fail,
    "drop": dlt.expect_all_or_drop
}


class Expectation(pyd.BaseModel):
    __pydantic_model_config__ = pyd.ConfigDict(arbitrary_types_allowed=True)
    """
    A class to represent an expectation that will be checked on the dataset. This is a slim
    wrapper around the
    """
    name: str = pyd.Field(description="The name of the expectation that is being checked.")
    constraint: str = pyd.Field(
        description="The constraint (string or Column) that will be checked on the dataset."
    )

    @property
    def dlt_expectation(self):
        """Property to return the expectation as a dictionary."""
        return {self.name: self.constraint}


_dlt_table_params = inspect.signature(dlt.table).parameters.keys()


class PydanticV2BaseModelLazy(pyd.BaseModel):
    """
    A class to represent the base model for the Pydantic models. This class is a slim wrapper around the Pydantic
    """

    def model_dump(self, *args, **kwargs):
        """Method to dump the model as a dictionary."""
        return self.dict(*args, **kwargs)


class CommonWriteOpts(PydanticV2BaseModelLazy):
    """
    A class to represent the common write options that will be used to create the table.
    """
    name: t.Optional[str] = pyd.Field(
        default=None,
        description="The name of the table that will be created.", examples=['my_table', 'my_view']
    )
    comment: t.Optional[str] = pyd.Field(
        default=None,
        description="The comment that will be added to the table.", examples=['This is a table', 'This is a view']
    )
    spark_conf: t.Optional[dict] = pyd.Field(
        default=None,
        description="The spark configuration for the execution of the query."
    )
    database: t.Optional[str] = pyd.Field(
        default=None,
        description="The database where the table will be stored."
    )
    tablename: t.Optional[str] = pyd.Field(
        default=None,
        description="The name of the table that will be created."
    )

    def __init__(self, *args, **kwargs):  # pydantic v1 style post init.
        """
        Initialize the CommonWriteOpts class.
        Parameters
        ----------
        args
        kwargs
        """
        super().__init__(*args, **kwargs)

        # Do things after Pydantic validation
        if self.name is None and all([self.tablename is not None, self.database is not None]):
            self.name = f"{self.database}.{self.tablename}"


class TableProperties(PydanticV2BaseModelLazy):
    """
    A class to represent the table properties that will be added to the table.
    https://docs.databricks.com/en/delta-live-tables/properties.html#delta-live-tables-table-properties
    """
    __pydantic_model_config__ = pyd.ConfigDict(extra='allow')
    managed_optimize: str = pyd.Field(
        default='true',
        serialization_alias='pipelines.autoOptimize.managed',
        description='Enables or disables automatically scheduled optimization of this table. Default=true'
    )
    z_order_cols: str = pyd.Field(
        default=None,
        serialization_alias='pipelines.autoOptimize.zOrderCols',
        description='An optional string containing a comma-separated list of column names to z-order '
                    'this table by. For example, pipelines.autoOptimize.zOrderCols = "year,month" \n\n'
                    'Default = None'
    )
    allow_reset: str = pyd.Field(
        default='true',
        serialization_alias='pipelines.reset.allowed',
        description='Controls whether a full refresh is allowed for this table.\n\nDefault=true'
    )


class TableWriteOpts(CommonWriteOpts):
    """
    A class to represent the write options that will be used to create the table.

    https://docs.databricks.com/en/delta-live-tables/python-ref.html#python-delta-live-tables-properties
    """
    __pydantic_model_config__ = pyd.ConfigDict(extra='allow')
    table_properties: t.Optional[dict] = pyd.Field(
        default_factory=lambda: TableProperties(),
        description="The table properties that will be added to the table."
    )
    path: t.Optional[str] = pyd.Field(
        default_factory=lambda: None,
        description="The path where the table will be stored."
    )
    partition_cols: t.Optional[t.List[str]] = pyd.Field(
        default_factory=lambda: None,
        description="The partition columns that will be added to the table."
    )


class AppendFlowConfig(PydanticV2BaseModelLazy):
    """
    A class to represent the configuration of the append flow.
    """
    target: str = pyd.Field(description="The target table name")
    name: t.Optional[str] = pyd.Field(default=None, description="The name of the append flow")
    spark_conf: t.Optional[dict] = pyd.Field(default=None,
                                             description="The spark configuration for the execution of the query")
    comment: t.Optional[str] = pyd.Field(default=None, description="The comment that will be added to the table")


class ApplyChangesConfig(PydanticV2BaseModelLazy):
    """
    A class to represent the configuration of the apply changes flow.
    """
    target: str = pyd.Field(..., description="The target table name")
    source: str = pyd.Field(..., description="The source table name")
    keys: t.List[str] = pyd.Field(..., description="The keys to join the source and target tables")
    sequence_by: t.Optional[str] = pyd.Field(default=None, description="The column to order data by")
    ignore_null_updates: t.Optional[bool] = pyd.Field(default=False, description="Flag to ignore null updates")
    apply_as_deletes: t.Optional[bool] = pyd.Field(default=None, description="Flag to apply as deletes")
    apply_as_truncates: t.Optional[bool] = pyd.Field(default=None, description="Flag to apply as truncates")
    column_list: t.Optional[t.List[str]] = pyd.Field(default=None, description="The list of columns to apply changes")
    except_column_list: t.Optional[t.List[str]] = pyd.Field(default=None, description="The list of columns to exclude")
    stored_as_scd_type: t.Optional[str] = pyd.Field(default=None, description="The type of SCD to store the data as")
    track_history_column_list: t.Optional[t.List[str]] = pyd.Field(
        default=None,
        description="The fields to exclude from history tracking"
    )
    track_history_except_column_list: t.Optional[t.List[str]] = pyd.Field(
        default=None,
        description="The fields to exclude from history tracking"
    )


class DLTConfig(PydanticV2BaseModelLazy):
    """
    A class to represent the configuration of the DLTMetaMixin.
    This class is a pydantic model that is used to declare:

    1. The name of the function that is being wrapped by the DLTMetaMixin.
    2. The type of DLT object that will be persisted in the data platform. table | view
    3. The expectations that will be checked on the dataset.
    4. The action that will be taken on the dataset. {allow, fail, drop}

    Upon initialization, the DLTConfig class will:

    1. Store the `expect_all`, `expect_all_or_fail`, or `expect_all_or_drop` function in the `_expectation_function`
    private attribute.
    2. Store the `dlt.table` or `dlt.view` function in the `_table_or_view_func` private attribute.

    These two elements are used to actually execute and implement the DLT functionality/expectation.
    """
    func_name: str = pyd.Field(description="The name of the function that is being wrapped by the DLTMetaMixin")
    kind: str = pyd.Field(description='The type of DLT object that will be persisted in the data platform.')
    expectations: t.List[Expectation] = pyd.Field(description="The expectations that will be checked on the dataset.")
    expectation_action: str = pyd.Field(description="The action that will be taken on the dataset.")
    _expectation_function: t.Callable = pyd.PrivateAttr(default=None)
    _table_or_view_func: t.Callable = pyd.PrivateAttr(default=None)
    write_opts: t.Optional[TableWriteOpts] = pyd.Field(
        default_factory=lambda: TableWriteOpts(),
        description="The write options that will be used to create the table."
    )
    is_streaming_table: t.Optional[bool] = pyd.Field(default=False,
                                                     description="Flag to indicate if the table is streaming.")
    append_config: AppendFlowConfig = pyd.Field(default=None,
                                                description="The configuration for the append flow.")
    apply_chg_config: ApplyChangesConfig = pyd.Field(default=None,
                                                     description="The configuration for the apply changes flow.")

    def __init__(self, *args, **kwargs):  # pydantic v1 style post init.
        """
        Initialize the DLTConfig class.
        Parameters
        ----------
        args
        kwargs
        """
        super().__init__(*args, **kwargs)

        # Do things after Pydantic validation
        self._expectation_function: t.Callable = _expectation_map.get(self.expectation_action)
        self._table_or_view_func: t.Callable = dlt.table if self.kind == "table" else dlt.view
        if self.is_streaming_table:
            if sum([self.append_config is not None, self.apply_chg_config is not None]) != 1:
                raise DLTException("Only one of append_config or apply_chg_config should be provided")

    @pyd.validator('expectation_action')
    def validate_expectation_action(cls, value):
        """Validator to ensure that the expectation_action is one of the supported actions."""
        action_choices = ['allow', 'fail', 'drop']
        if value not in action_choices:
            raise ValueError(
                f"The expectation_action of `{value}` is not supported. "
                f"Please choose from one of the following: `{', '.join(action_choices)}`"
            )
        return value

    @pyd.validator('kind')
    def validate_kind(cls, value):
        """Validator to ensure that the kind is one of the supported kinds."""
        kind_choices = ['table', 'view']
        if value not in kind_choices:
            raise ValueError(
                f"The kind of `{value}` is not supported. "
                f"Please choose from one of the following: `{', '.join(kind_choices)}`"
            )
        return value

    @property
    def table_or_view_func(self) -> t.Callable:
        """Property to return the table or view function based on the kind of DLT object."""
        return self._table_or_view_func

    @property
    def expectation_function(self) -> t.Callable:
        """Property to return the expectation function based on the expectation action."""
        return self._expectation_function

    @property
    def dlt_expectations(self) -> dict:
        """Property to return the expectations as a dictionary."""
        return dict(ChainMap(*[e.dlt_expectation for e in self.expectations]))


class DLTExecutionConfig(PydanticV2BaseModelLazy):
    """
    A class to represent the configuration of the DLT execution.
    """
    dlt_config: DLTConfig = pyd.Field(..., description="The DLT configuration.")
    table_or_view_func: t.Callable = pyd.Field(..., description="The table or view function.")
    child_func_name: str = pyd.Field(...,
                                     description="The name of the user defined function that will be wrapped with DLT stuff.")
    child_func_obj: t.Callable = pyd.Field(...,
                                           description="The user defined function that will be wrapped with DLT stuff.")


DLTConfigs = t.List[DLTConfig]
