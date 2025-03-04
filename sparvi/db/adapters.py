from sqlalchemy import create_engine, text
from typing import Optional, Dict, Any, List, Tuple, Union


class SqlAdapter:
    """Base adapter for database-specific SQL dialect handling."""

    @staticmethod
    def get_adapter(connection_string_or_engine):
        """
        Factory method that returns the appropriate adapter for the given connection.

        Args:
            connection_string_or_engine: SQLAlchemy connection string or engine

        Returns:
            An instance of the appropriate adapter class
        """
        # Handle connection string
        if isinstance(connection_string_or_engine, str):
            # Get the dialect name from the connection string without creating the engine
            dialect = connection_string_or_engine.split(':')[0].lower()

            # Handle unknown dialect without creating engine
            if dialect not in ['postgresql', 'postgres', 'duckdb', 'sqlite', 'snowflake']:
                return GenericAdapter()

            # For known dialects, create the engine
            engine = create_engine(connection_string_or_engine)
        else:
            # If an engine was passed, use it directly
            engine = connection_string_or_engine
            dialect = engine.dialect.name.lower()

        # Return the appropriate adapter based on dialect
        if 'snowflake' in dialect:
            return SnowflakeAdapter()
        elif 'postgres' in dialect or 'postgresql' in dialect:
            return PostgresAdapter()
        elif 'duckdb' in dialect:
            return DuckDBAdapter()
        elif 'sqlite' in dialect:
            return SQLiteAdapter()
        else:
            # Default to a generic adapter
            return GenericAdapter()

    def get_dialect_name(self, engine) -> str:
        """Get the dialect name from the engine."""
        return engine.dialect.name.lower()

    def percentile_query(self, column: str, percentile: float) -> str:
        """
        Generate SQL for calculating percentiles.

        Args:
            column: Column name
            percentile: Percentile value (0-1)

        Returns:
            SQL query fragment for percentile calculation
        """
        raise NotImplementedError("Subclasses must implement percentile_query")

    def regex_match(self, column: str, pattern: str) -> str:
        """
        Generate SQL for regex matching.

        Args:
            column: Column name
            pattern: Regex pattern

        Returns:
            SQL query fragment for regex matching
        """
        raise NotImplementedError("Subclasses must implement regex_match")

    def date_diff(self, unit: str, start_date: str, end_date: str) -> str:
        """
        Generate SQL for date difference calculation.

        Args:
            unit: Time unit ('day', 'month', 'year', etc.)
            start_date: Start date column or expression
            end_date: End date column or expression

        Returns:
            SQL query fragment for date difference
        """
        raise NotImplementedError("Subclasses must implement date_diff")

    def length_function(self, column: str) -> str:
        """
        Generate SQL for string length calculation.

        Args:
            column: Column name

        Returns:
            SQL query fragment for string length
        """
        raise NotImplementedError("Subclasses must implement length_function")

    def stddev_function(self, column: str) -> str:
        """
        Generate SQL for standard deviation calculation.

        Args:
            column: Column name

        Returns:
            SQL query fragment for standard deviation
        """
        raise NotImplementedError("Subclasses must implement stddev_function")

    def is_numeric_type(self, col_type: str) -> bool:
        """
        Check if a column type is numeric.

        Args:
            col_type: Column type as string

        Returns:
            True if the column type is numeric, False otherwise
        """
        col_type = col_type.lower()
        return any(t in col_type for t in ['int', 'float', 'numeric', 'double', 'decimal'])

    def is_date_type(self, col_type: str) -> bool:
        """
        Check if a column type is a date/time type.

        Args:
            col_type: Column type as string

        Returns:
            True if the column type is a date/time type, False otherwise
        """
        col_type = col_type.lower()
        return any(t in col_type for t in ['date', 'time', 'timestamp'])

    def is_text_type(self, col_type: str) -> bool:
        """
        Check if a column type is a text type.

        Args:
            col_type: Column type as string

        Returns:
            True if the column type is a text type, False otherwise
        """
        col_type = col_type.lower()
        return any(t in col_type for t in ['varchar', 'char', 'text', 'string'])


class DuckDBAdapter(SqlAdapter):
    """Adapter for DuckDB."""

    def percentile_query(self, column: str, percentile: float) -> str:
        return f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {column})"

    def regex_match(self, column: str, pattern: str) -> str:
        return f"{column} ~ '{pattern}'"

    def date_diff(self, unit: str, start_date: str, end_date: str) -> str:
        return f"DATEDIFF('{unit}', {start_date}, {end_date})"

    def length_function(self, column: str) -> str:
        return f"LENGTH({column})"

    def stddev_function(self, column: str) -> str:
        return f"STDDEV({column})"


class PostgresAdapter(SqlAdapter):
    """Adapter for PostgreSQL."""

    def percentile_query(self, column: str, percentile: float) -> str:
        return f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {column})"

    def regex_match(self, column: str, pattern: str) -> str:
        return f"{column} ~ '{pattern}'"

    def date_diff(self, unit: str, start_date: str, end_date: str) -> str:
        if unit.lower() == 'day':
            return f"DATE_PART('day', {end_date}::timestamp - {start_date}::timestamp)"
        elif unit.lower() == 'month':
            return f"(DATE_PART('year', {end_date}::timestamp) - DATE_PART('year', {start_date}::timestamp)) * 12 + (DATE_PART('month', {end_date}::timestamp) - DATE_PART('month', {start_date}::timestamp))"
        elif unit.lower() == 'year':
            return f"DATE_PART('year', {end_date}::timestamp) - DATE_PART('year', {start_date}::timestamp)"
        else:
            # Default to days
            return f"DATE_PART('day', {end_date}::timestamp - {start_date}::timestamp)"

    def length_function(self, column: str) -> str:
        return f"LENGTH({column})"

    def stddev_function(self, column: str) -> str:
        return f"STDDEV({column})"


class SnowflakeAdapter(SqlAdapter):
    """Adapter for Snowflake."""

    def percentile_query(self, column: str, percentile: float) -> str:
        return f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {column})"

    def regex_match(self, column: str, pattern: str) -> str:
        return f"REGEXP_LIKE({column}, '{pattern}')"

    def date_diff(self, unit: str, start_date: str, end_date: str) -> str:
        return f"DATEDIFF('{unit}', {start_date}, {end_date})"

    def length_function(self, column: str) -> str:
        return f"LENGTH({column})"

    def stddev_function(self, column: str) -> str:
        return f"STDDEV({column})"


class SQLiteAdapter(SqlAdapter):
    """Adapter for SQLite."""

    def percentile_query(self, column: str, percentile: float) -> str:
        # SQLite doesn't support percentile_cont natively
        # This is a very simple approximation
        return f"{column}"

    def regex_match(self, column: str, pattern: str) -> str:
        return f"{column} REGEXP '{pattern}'"

    def date_diff(self, unit: str, start_date: str, end_date: str) -> str:
        if unit.lower() == 'day':
            return f"JULIANDAY({end_date}) - JULIANDAY({start_date})"
        elif unit.lower() == 'month':
            return f"(CAST(STRFTIME('%Y', {end_date}) AS INTEGER) - CAST(STRFTIME('%Y', {start_date}) AS INTEGER)) * 12 + (CAST(STRFTIME('%m', {end_date}) AS INTEGER) - CAST(STRFTIME('%m', {start_date}) AS INTEGER))"
        elif unit.lower() == 'year':
            return f"CAST(STRFTIME('%Y', {end_date}) AS INTEGER) - CAST(STRFTIME('%Y', {start_date}) AS INTEGER)"
        else:
            # Default to days
            return f"JULIANDAY({end_date}) - JULIANDAY({start_date})"

    def length_function(self, column: str) -> str:
        return f"LENGTH({column})"

    def stddev_function(self, column: str) -> str:
        # SQLite doesn't have a built-in stddev function
        # Consider implementing with a user-defined function if needed
        return f"0"


class GenericAdapter(SqlAdapter):
    """Generic adapter for other databases."""

    def percentile_query(self, column: str, percentile: float) -> str:
        # Most modern SQL databases support this syntax
        return f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {column})"

    def regex_match(self, column: str, pattern: str) -> str:
        # Use LIKE as a fallback for regex (though it's limited)
        return f"{column} LIKE '%{pattern}%'"

    def date_diff(self, unit: str, start_date: str, end_date: str) -> str:
        # Generic date difference, might not work universally
        return f"{end_date} - {start_date}"

    def length_function(self, column: str) -> str:
        return f"LENGTH({column})"

    def stddev_function(self, column: str) -> str:
        return f"STDDEV({column})"


# Utility functions
def get_dialect_name(engine) -> str:
    """Get the dialect name from the engine."""
    return engine.dialect.name.lower()


def is_supported_dialect(dialect_name: str) -> bool:
    """Check if the dialect is explicitly supported."""
    supported = ['duckdb', 'postgresql', 'postgres', 'snowflake']
    return any(s in dialect_name for s in supported)


def get_adapter_for_connection(connection_string_or_engine) -> SqlAdapter:
    """Get the appropriate adapter for a connection string or engine."""
    return SqlAdapter.get_adapter(connection_string_or_engine)