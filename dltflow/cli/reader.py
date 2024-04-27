"""
dltflow.reader.py
-------------------
This module is basically a copy and paste of the dbx.api.config_reader module with some modifications to support
the dltflow deployment configuration file format.

The main difference is that the dltflowDeploymentConfig class is used instead of the DeploymentConfig class.
"""

import inspect  # pragma: no cover

from dbx.api import config_reader  # pragma: no cover
from dltflow.utils import dbx_echo  # pragma: no cover

config_reader_source = (
    inspect.getsource(config_reader)
    .replace("'Deployment'", "'DLTFlowDeployment'")
    .replace("DeploymentConfig", "DLTFlowDeploymentConfig")
    .replace("EnvironmentDeploymentInfo", "DLTFlowEnvironmentDeploymentInfo")
    .replace("from dbx.models.deployment", "from dltflow.cli.models.deployment")
)  # pragma: no cover

dbx_echo(
    "⚠️🪄[red]Dynamically refactoring `dbx` config reader code to support `dltflow`."
)  # pragma: no cover

exec(config_reader_source.strip())  # pragma: no cover
