"""
Tests for the CLI functionality.
"""
import pytest
from click.testing import CliRunner
from sparvi.cli.main import cli

def test_cli_help():
    """Test that the CLI help command works."""
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert 'Usage:' in result.output
    assert 'profile' in result.output
    assert 'validate' in result.output

def test_cli_version():
    """Test that the CLI version command works."""
    runner = CliRunner()
    result = runner.invoke(cli, ['--version'])
    assert result.exit_code == 0
    assert 'version' in result.output

def test_cli_profile_command(sample_db_path):
    """Test the CLI profile command with minimal output."""
    runner = CliRunner()
    result = runner.invoke(cli, ['profile', sample_db_path, 'employees', '--format', 'minimal'])
    assert result.exit_code == 0
    assert 'employees' in result.output
    assert 'Row count' in result.output

def test_cli_validate_command(sample_db_path):
    """Test the CLI validate command with default rules."""
    runner = CliRunner()
    result = runner.invoke(cli, ['validate', sample_db_path, 'products', '--generate-defaults'])
    assert result.exit_code == 0
    assert 'Validation Rules Summary' in result.output