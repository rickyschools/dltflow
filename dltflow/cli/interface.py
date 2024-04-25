"""
dltflow.interface.py
----------------------
This module is the entry point for the dltflow CLI. It defines the CLI commands and their respective subcommands.
"""

import click

from dltflow.cli.initialize import init
from dltflow.cli.deploy import deploy_py_dlt2


@click.group('dltflow')
def cli():
    pass


cli.add_command(cmd=init)
cli.add_command(cmd=deploy_py_dlt2)

if __name__ == '__main__':
    cli()
