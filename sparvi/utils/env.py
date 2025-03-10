import os
from typing import Dict, Optional, Any
from urllib.parse import quote_plus


def get_snowflake_connection_from_env() -> str:
    """
    Get Snowflake connection string from environment variables.

    Required environment variables:
    - SNOWFLAKE_USER: Snowflake username
    - SNOWFLAKE_PASSWORD: Snowflake password
    - SNOWFLAKE_ACCOUNT: Snowflake account identifier
    - SNOWFLAKE_DATABASE: Default database to use

    Optional environment variables:
    - SNOWFLAKE_SCHEMA: Schema to use (defaults to PUBLIC)
    - SNOWFLAKE_WAREHOUSE: Warehouse to use (defaults to COMPUTE_WH)
    - SNOWFLAKE_ROLE: Role to assume (defaults to None)

    Returns:
        str: Formatted SQLAlchemy connection string for Snowflake

    Raises:
        ValueError: If required environment variables are missing
    """
    required_vars = ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_DATABASE"]

    # Check if all required variables are present
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    # Get required variables
    user = os.environ["SNOWFLAKE_USER"]
    password = os.environ["SNOWFLAKE_PASSWORD"]
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    database = os.environ["SNOWFLAKE_DATABASE"]

    # URL encode username and password to handle special characters
    user_encoded = quote_plus(user)
    password_encoded = quote_plus(password)

    # Get optional variables with defaults
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    role = os.environ.get("SNOWFLAKE_ROLE")

    # Build the base connection string
    connection_string = f"snowflake://{user_encoded}:{password_encoded}@{account}/{database}/{schema}?warehouse={warehouse}"

    # Add role if specified
    if role:
        connection_string += f"&role={role}"

    return connection_string


def get_snowflake_config_from_env() -> Dict[str, Any]:
    """
    Get Snowflake configuration dictionary from environment variables.

    Returns:
        Dict[str, Any]: Configuration dictionary with Snowflake settings
    """
    config = {
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        # Additional configuration options
        "application": "Sparvi",
        "session_parameters": {
            "QUERY_TAG": "SPARVI_PROFILER",
            # Set optimized session parameters
            "USE_CACHED_RESULT": True,
            "STATEMENT_TIMEOUT_IN_SECONDS": int(os.environ.get("SNOWFLAKE_TIMEOUT_SECONDS", "300"))
        }
    }

    # Remove None values to avoid passing them to Snowflake
    return {k: v for k, v in config.items() if v is not None}


def get_connection_from_env() -> Optional[str]:
    """
    Get database connection string from environment variables.

    Checks for the following environment variables in order:
    1. DATABASE_URL - Generic connection string
    2. Snowflake-specific variables
    3. Other database-specific variables (future implementation)

    Returns:
        Optional[str]: Database connection string or None if not available
    """
    # Check for generic DATABASE_URL first
    if "DATABASE_URL" in os.environ:
        return os.environ["DATABASE_URL"]

    # Check for Snowflake configuration
    snowflake_vars = ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_DATABASE"]
    if all(var in os.environ for var in snowflake_vars):
        return get_snowflake_connection_from_env()

    # Other database connections could be added here

    # No connection variables found
    return None