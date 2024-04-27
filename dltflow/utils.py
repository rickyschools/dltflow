"""
dltflow.utils
-------------
This module contains utility functions for the `dltflow` package.

It's contents are dynamically refactored from the `dbx` package.
"""

import inspect  # noqa

from dbx import utils  # noqa

utils_source_code = inspect.getsource(utils)  # noqa
utils_source_code = utils_source_code.replace("[dbx]", "[dltflow]")  # noqa

exec(utils_source_code)  # noqa

dbx_echo("⚠️🪄[red]Dynamically refactoring `dbx` utils code to support `dltflow`.")
