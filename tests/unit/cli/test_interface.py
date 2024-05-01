import pytest
from click.testing import CliRunner
from dltflow.cli.interface import cli


def test_cli_help():
    """Test that the `cli` function prints help text."""
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert 'Show this message and exit.' in result.output


def test_cli_init_help():
    """Test that the `init` function prints help text."""
    runner = CliRunner()
    result = runner.invoke(cli, ['init', '--help'])
    assert result.exit_code == 0
    assert 'Initialize a new dltflow project' in result.output


def test_cli_deploy_help():
    """Test that the `deploy` function prints help text."""
    runner = CliRunner()
    result = runner.invoke(cli, ['deploy-py-dlt', '--help'])
    assert result.exit_code == 0
    assert ' Deploy a DLT pipeline' in result.output
