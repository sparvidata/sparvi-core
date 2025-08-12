"""
Connection management module for database adapters with Snowflake optimizations.
"""
import os
import urllib.parse
from typing import Dict, Any, Optional, Union

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from sparvi.db.adapters import get_adapter_for_connection

# Default configuration for Snowflake
SNOWFLAKE_CONFIG = {
    "warehouse_size": "X-SMALL",
    "timeout": 60,
    "role": None,  # Default to user's default role
    "query_tag": "Sparvi_Profiler",
}


class ConnectionManager:
    """Base connection manager class for database connections."""

    def __init__(self, connection_string: str):
        """
        Initialize connection manager with a connection string.

        Args:
            connection_string: SQLAlchemy-compatible connection string
        """
        self.connection_string = connection_string
        self.params = {}
        self.dialect = self._get_dialect_from_connection_string()

    def _get_dialect_from_connection_string(self) -> str:
        """Extract the database dialect from the connection string."""
        try:
            return self.connection_string.split("://")[0].lower()
        except (IndexError, AttributeError):
            return "unknown"

    def get_engine(self, **kwargs) -> Engine:
        """
        Create and return a SQLAlchemy engine for the connection.

        Args:
            **kwargs: Additional arguments to pass to create_engine

        Returns:
            SQLAlchemy Engine instance
        """
        return create_engine(self.connection_string, **kwargs)


class SnowflakeConnectionManager(ConnectionManager):
    """Manages Snowflake connections with optimized settings."""

    def __init__(self, connection_string: str):
        """
        Initialize Snowflake connection manager.

        Args:
            connection_string: Snowflake connection string
                               (snowflake://user:password@account/database/schema?warehouse=wh)
        """
        super().__init__(connection_string)
        self._parse_connection_params()

    def _parse_connection_params(self) -> None:
        """Parse connection parameters from the connection string."""
        try:
            # Extract parts of the connection string
            parts = self.connection_string.split("://")[1].split("@")
            auth_part = parts[0]
            location_part = parts[1]

            # Extract user and password
            if ":" in auth_part:
                self.params["user"], self.params["password"] = auth_part.split(":", 1)
            else:
                self.params["user"] = auth_part
                self.params["password"] = None

            # Extract account, database, schema
            location_parts = location_part.split("/")
            self.params["account"] = location_parts[0]

            if len(location_parts) > 1:
                self.params["database"] = location_parts[1]

            if len(location_parts) > 2:
                # Handle schema and query parameters
                schema_and_params = location_parts[2]
                if "?" in schema_and_params:
                    self.params["schema"], query_string = schema_and_params.split("?", 1)
                    query_params = urllib.parse.parse_qs(query_string)
                    # Convert lists to single values
                    for key, value in query_params.items():
                        self.params[key] = value[0] if len(value) == 1 else value
                else:
                    self.params["schema"] = schema_and_params

        except Exception as e:
            # In case of parsing errors, just leave params empty
            self.params = {}

    def get_engine(self, **kwargs) -> Engine:
        """
        Create SQLAlchemy engine with optimized Snowflake settings.

        Args:
            **kwargs: Additional arguments to pass to create_engine

        Returns:
            SQLAlchemy Engine instance optimized for Snowflake
        """
        connect_args = kwargs.pop("connect_args", {})

        # Add Snowflake-specific connection arguments
        connect_args.update({
            "application": "Sparvi",
            "session_parameters": {
                "QUERY_TAG": SNOWFLAKE_CONFIG["query_tag"]
            }
        })

        # Set default warehouse if not specified
        if "warehouse" not in self.params and not connect_args.get("warehouse"):
            warehouse = kwargs.pop("warehouse", SNOWFLAKE_CONFIG["warehouse_size"])
            connect_args["warehouse"] = warehouse

        # Set role if specified
        if SNOWFLAKE_CONFIG["role"] and "role" not in connect_args:
            connect_args["role"] = SNOWFLAKE_CONFIG["role"]

        # Set login timeout
        if "login_timeout" not in connect_args:
            connect_args["login_timeout"] = SNOWFLAKE_CONFIG["timeout"]

        # Create engine with connect_args
        return create_engine(
            self.connection_string,
            connect_args=connect_args,
            poolclass=kwargs.pop("poolclass", NullPool),  # Default to NullPool for Snowflake
            **kwargs
        )


class DuckDBConnectionManager(ConnectionManager):
    """Manages DuckDB connections."""

    def get_engine(self, **kwargs) -> Engine:
        """
        Create SQLAlchemy engine for DuckDB.

        Args:
            **kwargs: Additional arguments to pass to create_engine

        Returns:
            SQLAlchemy Engine instance for DuckDB
        """
        # Simply pass through to base implementation
        return super().get_engine(**kwargs)


class PostgresConnectionManager(ConnectionManager):
    """Manages PostgreSQL connections."""

    def get_engine(self, **kwargs) -> Engine:
        """
        Create SQLAlchemy engine for PostgreSQL with some optimizations.

        Args:
            **kwargs: Additional arguments to pass to create_engine

        Returns:
            SQLAlchemy Engine instance for PostgreSQL
        """
        connect_args = kwargs.pop("connect_args", {})

        # Some reasonable defaults for PostgreSQL
        if "connect_timeout" not in connect_args:
            connect_args["connect_timeout"] = 30

        # Use None as poolclass to use SQLAlchemy's default QueuePool
        return create_engine(
            self.connection_string,
            connect_args=connect_args,
            **kwargs
        )


class BigQueryConnectionManager(ConnectionManager):
    """Manages BigQuery connections."""

    def get_engine(self, **kwargs) -> Engine:
        """
        Create SQLAlchemy engine for BigQuery with optimizations.

        Args:
            **kwargs: Additional arguments to pass to create_engine

        Returns:
            SQLAlchemy Engine instance for BigQuery
        """
        connect_args = kwargs.pop("connect_args", {})

        # BigQuery-specific connection arguments
        connect_args.update({
            "client_info": {"application_name": "Sparvi"},
        })

        # Set default location if not specified
        if "location" not in connect_args:
            connect_args["location"] = "US"

        # Set reasonable billing limit
        if "maximum_bytes_billed" not in connect_args:
            connect_args["maximum_bytes_billed"] = 1000000000  # 1GB

        return create_engine(
            self.connection_string,
            connect_args=connect_args,
            poolclass=kwargs.pop("poolclass", NullPool),  # BigQuery works well with NullPool
            **kwargs
        )


class RedshiftConnectionManager(ConnectionManager):
    """Manages Redshift connections."""

    def get_engine(self, **kwargs) -> Engine:
        """
        Create SQLAlchemy engine for Redshift with optimizations.

        Args:
            **kwargs: Additional arguments to pass to create_engine

        Returns:
            SQLAlchemy Engine instance for Redshift
        """
        connect_args = kwargs.pop("connect_args", {})

        # Redshift-specific connection arguments
        connect_args.update({
            "application_name": "Sparvi",
        })

        # Set SSL by default for security
        if "sslmode" not in connect_args:
            connect_args["sslmode"] = "require"

        # Set connection timeout
        if "connect_timeout" not in connect_args:
            connect_args["connect_timeout"] = 30

        return create_engine(
            self.connection_string,
            connect_args=connect_args,
            **kwargs
        )


def get_connection_manager(connection_string: str) -> ConnectionManager:
    """
    Factory function to get the appropriate connection manager.

    Args:
        connection_string: Database connection string

    Returns:
        ConnectionManager instance for the specific database type
    """
    dialect = connection_string.split("://")[0].lower()

    if "snowflake" in dialect:
        return SnowflakeConnectionManager(connection_string)
    elif "postgresql" in dialect or "postgres" in dialect:
        return PostgresConnectionManager(connection_string)
    elif "duckdb" in dialect:
        return DuckDBConnectionManager(connection_string)
    elif "bigquery" in dialect:
        return BigQueryConnectionManager(connection_string)
    elif "redshift" in dialect:
        return RedshiftConnectionManager(connection_string)
    else:
        # Default to base implementation for other database types
        return ConnectionManager(connection_string)


def create_db_engine(connection_string: str, **kwargs) -> Engine:
    """
    Create a database engine using the appropriate connection manager.

    Args:
        connection_string: Database connection string
        **kwargs: Additional arguments to pass to the connection manager

    Returns:
        SQLAlchemy Engine instance
    """
    manager = get_connection_manager(connection_string)
    return manager.get_engine(**kwargs)