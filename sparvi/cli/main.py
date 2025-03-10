# sparvi/cli/main.py

import os
import sys
from typing import Optional

import click
from rich.console import Console

from sparvi.version import __version__
from sparvi.cli.profile import profile
from sparvi.cli.validate import validate
from sparvi.utils.env import get_connection_from_env

console = Console()


@click.group(help="Sparvi CLI - Data profiling and validation")
@click.version_option(version=__version__)
@click.option(
    "--verbose", "-v", is_flag=True, help="Enable verbose output", default=False
)
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """Main CLI entrypoint for Sparvi.

    Example:
      sparvi profile "snowflake://user:pass@account/database/schema?warehouse=wh" customers
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


# Add subcommands
cli.add_command(profile)
cli.add_command(validate)


@cli.command()
def info() -> None:
    """Display information about the Sparvi installation."""
    console.print("[bold]Sparvi Core[/bold]", f"version {__version__}")
    console.print("\n[bold]System Information:[/bold]")
    console.print(f"  Python version: {sys.version.split()[0]}")
    console.print(f"  Platform: {sys.platform}")

    # Show database connection info if available from environment
    connection_str = get_connection_from_env()
    if connection_str:
        # Sanitize the connection string to avoid showing credentials
        sanitized_conn = sanitize_connection_string(connection_str)
        console.print("\n[bold]Default Connection:[/bold]")
        console.print(f"  {sanitized_conn}")


def sanitize_connection_string(connection_string: str) -> str:
    """Sanitize a connection string to hide sensitive information."""
    import re

    # Replace password in various connection string formats
    patterns = [
        # Standard SQLAlchemy URL format
        (r'(://[^:]+:)([^@]+)(@)', r'\1*****\3'),
        # Snowflake specific
        (r'(snowflake://[^:]+:)([^@]+)(@)', r'\1*****\3'),
        # Connection params with password
        (r'(password=)([^&]+)(&|$)', r'\1*****\3'),
        # Connection params with pwd
        (r'(pwd=)([^&]+)(&|$)', r'\1*****\3')
    ]

    result = connection_string
    for pattern, replacement in patterns:
        result = re.sub(pattern, replacement, result)

    return result


@cli.command()
@click.option(
    "--connection",
    help="Test a specific database connection string"
)
def test_connection(connection: Optional[str]) -> None:
    """Test database connection using environment variables or provided string."""
    from sqlalchemy import create_engine, inspect

    # If no connection provided, try to get from environment
    if not connection:
        connection = get_connection_from_env()
        if not connection:
            console.print("[bold red]Error:[/bold red] No connection string provided and no environment variables set.")
            console.print("Please provide a connection string or set environment variables.")
            console.print("\nFor Snowflake, set the following environment variables:")
            console.print("  SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE")
            sys.exit(1)

    # Test the connection
    console.print(f"Testing connection to: [bold]{sanitize_connection_string(connection)}[/bold]")

    try:
        engine = create_engine(connection)
        inspector = inspect(engine)

        # Try to get table list to confirm connection works
        with engine.connect() as conn:
            tables = inspector.get_table_names()

        console.print("[bold green]✓ Connection successful![/bold green]")
        console.print(f"Found {len(tables)} tables.")

        if tables:
            console.print("\n[bold]Available tables:[/bold]")
            for table in sorted(tables)[:10]:  # Show first 10 tables
                console.print(f"  - {table}")

            if len(tables) > 10:
                console.print(f"  ... and {len(tables) - 10} more")

    except Exception as e:
        console.print("[bold red]✗ Connection failed![/bold red]")
        console.print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    cli()