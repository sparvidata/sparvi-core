# sparvi/cli/dbt.py
"""
dbt Integration CLI Commands

Upload dbt artifacts (manifest.json, run_results.json) to Sparvi platform.
"""

import os
import json
import sys
from pathlib import Path
from typing import Optional

import click
import httpx
from rich.console import Console
from rich.table import Table

console = Console()


@click.group(name="dbt")
def dbt():
    """dbt integration commands for uploading artifacts to Sparvi"""
    pass


@dbt.command()
@click.option(
    "--manifest-path",
    type=click.Path(exists=True),
    help="Path to manifest.json file. Defaults to ./target/manifest.json"
)
@click.option(
    "--run-results-path",
    type=click.Path(exists=True),
    help="Path to run_results.json file. Defaults to ./target/run_results.json"
)
@click.option(
    "--project-name",
    help="dbt project name. If not provided, will be read from manifest.json"
)
@click.option(
    "--connection-id",
    help="Sparvi connection ID to link this project to (optional)"
)
@click.option(
    "--api-key",
    help="Sparvi API key. Can also be set via SPARVI_API_KEY environment variable"
)
@click.option(
    "--api-url",
    help="Sparvi API URL. Defaults to SPARVI_API_URL env var or https://app.sparvi.io",
    default=None
)
def upload(
    manifest_path: Optional[str],
    run_results_path: Optional[str],
    project_name: Optional[str],
    connection_id: Optional[str],
    api_key: Optional[str],
    api_url: Optional[str]
):
    """
    Upload dbt artifacts to Sparvi platform.

    This command should be run after 'dbt run' or 'dbt test' to upload
    the generated artifacts to Sparvi for visualization and monitoring.

    Example:
        # Basic usage (looks for manifest in ./target/)
        sparvi dbt upload --api-key=sk_live_...

        # Specify custom paths
        sparvi dbt upload --manifest-path=/path/to/manifest.json --api-key=sk_live_...

        # Use environment variable for API key
        export SPARVI_API_KEY=sk_live_...
        sparvi dbt upload

        # Link to existing connection
        sparvi dbt upload --connection-id=abc-123-def --api-key=sk_live_...
    """
    try:
        # Get API key from parameter or environment
        api_key = api_key or os.getenv("SPARVI_API_KEY")
        if not api_key:
            console.print("[bold red]Error:[/bold red] API key is required.")
            console.print("Provide via --api-key flag or SPARVI_API_KEY environment variable")
            console.print("\nGenerate an API key at: https://app.sparvi.io/settings/api-keys")
            sys.exit(1)

        # Get API URL from parameter or environment
        api_url = api_url or os.getenv("SPARVI_API_URL", "https://app.sparvi.io")
        upload_url = f"{api_url}/api/integrations/dbt/upload"

        # Auto-discover manifest.json if not provided
        if not manifest_path:
            default_manifest = Path("./target/manifest.json")
            if default_manifest.exists():
                manifest_path = str(default_manifest)
                console.print(f"Found manifest.json at {manifest_path}")
            else:
                console.print("[bold red]Error:[/bold red] manifest.json not found.")
                console.print("Run 'dbt run' or 'dbt compile' first, or specify --manifest-path")
                sys.exit(1)

        # Auto-discover run_results.json if not provided
        if not run_results_path:
            default_run_results = Path("./target/run_results.json")
            if default_run_results.exists():
                run_results_path = str(default_run_results)
                console.print(f"Found run_results.json at {run_results_path}")

        # Read manifest to get project name if not provided
        with open(manifest_path, 'r') as f:
            manifest_data = json.load(f)

        if not project_name:
            project_name = manifest_data.get("metadata", {}).get("project_name")
            if not project_name:
                # Fallback to project_id if project_name not available
                project_name = manifest_data.get("metadata", {}).get("project_id")

            if not project_name:
                console.print("[bold red]Error:[/bold red] Could not determine project name from manifest.")
                console.print("Please provide --project-name")
                sys.exit(1)

        console.print(f"\n[bold]Uploading dbt artifacts for project:[/bold] {project_name}")

        # Prepare multipart upload
        files = {
            "manifest": open(manifest_path, "rb")
        }

        if run_results_path:
            files["run_results"] = open(run_results_path, "rb")

        data = {
            "project_name": project_name
        }

        if connection_id:
            data["connection_id"] = connection_id

        # Upload with progress
        with console.status("[bold green]Uploading artifacts..."):
            with httpx.Client(timeout=60.0) as client:
                response = client.post(
                    upload_url,
                    headers={"Authorization": f"Bearer {api_key}"},
                    files=files,
                    data=data
                )

        # Close file handles
        for file in files.values():
            file.close()

        # Handle response
        if response.status_code == 201:
            result = response.json()
            console.print("[bold green]✓ Upload successful![/bold green]\n")

            # Display stats in a nice table
            stats = result.get("stats", {})
            table = Table(title="Upload Summary")
            table.add_column("Metric", style="cyan")
            table.add_column("Count", style="green")

            table.add_row("Models", str(stats.get("models_count", 0)))
            table.add_row("Tests", str(stats.get("tests_count", 0)))
            table.add_row("Sources", str(stats.get("sources_count", 0)))
            table.add_row("Lineage Edges", str(stats.get("lineage_count", 0)))

            console.print(table)

            console.print(f"\n[bold]Project ID:[/bold] {result.get('project_id')}")
            console.print(f"[bold]Connection ID:[/bold] {result.get('connection_id')}")
            console.print(f"\nView in Sparvi: {api_url}/dbt/projects/{result.get('project_id')}")

        elif response.status_code == 401:
            console.print("[bold red]✗ Authentication failed[/bold red]")
            console.print("Your API key is invalid or expired.")
            console.print("Generate a new key at: https://app.sparvi.io/settings/api-keys")
            sys.exit(1)

        elif response.status_code == 403:
            console.print("[bold red]✗ Permission denied[/bold red]")
            console.print("Your API key does not have the 'dbt:upload' permission.")
            sys.exit(1)

        else:
            console.print(f"[bold red]✗ Upload failed[/bold red] (HTTP {response.status_code})")
            try:
                error_detail = response.json().get("error", response.text)
                console.print(f"Error: {error_detail}")
            except:
                console.print(f"Error: {response.text}")
            sys.exit(1)

    except FileNotFoundError as e:
        console.print(f"[bold red]Error:[/bold red] File not found: {e.filename}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        console.print(f"[bold red]Error:[/bold red] Invalid JSON in manifest file")
        console.print(str(e))
        sys.exit(1)
    except httpx.RequestError as e:
        console.print(f"[bold red]Error:[/bold red] Network error: {str(e)}")
        console.print("Check your internet connection and API URL")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        import traceback
        console.print(traceback.format_exc())
        sys.exit(1)


@dbt.command(name="list")
@click.option(
    "--api-key",
    help="Sparvi API key. Can also be set via SPARVI_API_KEY environment variable"
)
@click.option(
    "--api-url",
    help="Sparvi API URL. Defaults to SPARVI_API_URL env var or https://app.sparvi.io",
    default=None
)
def list_projects(api_key: Optional[str], api_url: Optional[str]):
    """List all dbt projects in your Sparvi organization"""
    try:
        # Get API key
        api_key = api_key or os.getenv("SPARVI_API_KEY")
        if not api_key:
            console.print("[bold red]Error:[/bold red] API key is required.")
            sys.exit(1)

        # Get API URL
        api_url = api_url or os.getenv("SPARVI_API_URL", "https://app.sparvi.io")
        list_url = f"{api_url}/api/integrations/dbt/projects"

        # Fetch projects
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                list_url,
                headers={"Authorization": f"Bearer {api_key}"}
            )

        if response.status_code == 200:
            result = response.json()
            projects = result.get("projects", [])

            if not projects:
                console.print("No dbt projects found in your organization.")
                return

            # Display in table
            table = Table(title="dbt Projects")
            table.add_column("Project Name", style="cyan")
            table.add_column("dbt Version", style="green")
            table.add_column("Last Upload", style="yellow")
            table.add_column("Project ID", style="dim")

            for project in projects:
                table.add_row(
                    project.get("project_name", "Unknown"),
                    project.get("dbt_version", "N/A"),
                    project.get("last_parsed_at", "Never")[:19] if project.get("last_parsed_at") else "Never",
                    project.get("id", "")[:8]
                )

            console.print(table)

        else:
            console.print(f"[bold red]Error:[/bold red] Failed to fetch projects (HTTP {response.status_code})")
            sys.exit(1)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)
