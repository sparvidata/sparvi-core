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
            if dialect not in ['postgresql', 'postgres', 'duckdb', 'sqlite', 'snowflake', 'bigquery', 'redshift']:
                return GenericAdapter()

            # For known dialects, create the engine
            engine = create_engine(connection_string_or_engine)
        else:
            # If an engine was passed, use it directly
            engine = connection_string_or_engine
            dialect = engine.dialect.name.lower()

        # Return the appropriate adapter based on dialect
        # Prioritize Snowflake as the default
        if 'snowflake' in dialect:
            return SnowflakeAdapter()
        elif 'postgres' in dialect or 'postgresql' in dialect:
            return PostgresAdapter()
        elif 'redshift' in dialect:
            return RedshiftAdapter()
        elif 'bigquery' in dialect:
            return BigQueryAdapter()
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

    def sample_query(self, table: str, limit: int) -> str:
        """
        Generate SQL for efficiently sampling rows from a table.

        Args:
            table: Table name
            limit: Maximum number of rows to sample

        Returns:
            SQL query for efficient sampling
        """
        return f"SELECT * FROM {table} LIMIT {limit}"

    def aggregate_array(self, column: str) -> str:
        """
        Generate SQL for aggregating values into an array.

        Args:
            column: Column name

        Returns:
            SQL fragment for array aggregation
        """
        raise NotImplementedError("Subclasses must implement aggregate_array")

    def is_numeric_type(self, col_type: str) -> bool:
        """
        Check if a column type is numeric.

        Args:
            col_type: Column type as string

        Returns:
            True if the column type is numeric, False otherwise
        """
        col_type = col_type.lower()
        return any(t in col_type for t in ['int', 'float', 'numeric', 'double', 'decimal', 'number'])

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

    def sample_query(self, table: str, limit: int) -> str:
        # Use Snowflake's efficient sampling
        return f"SELECT * FROM {table} SAMPLE ({limit} ROWS)"

    def aggregate_array(self, column: str) -> str:
        return f"ARRAY_AGG({column})"

    def optimize_query(self, query: str) -> str:
        """Apply Snowflake-specific query optimizations."""
        # This can be expanded with more optimizations
        return query


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

    def aggregate_array(self, column: str) -> str:
        return f"LIST({column})"


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

    def aggregate_array(self, column: str) -> str:
        return f"ARRAY_AGG({column})"


class RedshiftAdapter(SqlAdapter):
    """Adapter for Amazon Redshift."""

    def percentile_query(self, column: str, percentile: float) -> str:
        # Redshift does not support PERCENTILE_CONT directly, use approximate percentile
        return f"APPROXIMATE PERCENTILE_DISC({percentile}) WITHIN GROUP (ORDER BY {column})"

    def regex_match(self, column: str, pattern: str) -> str:
        # Redshift uses REGEXP for regex matching
        return f"{column} REGEXP '{pattern}'"

    def date_diff(self, unit: str, start_date: str, end_date: str) -> str:
        # Redshift uses DATEDIFF function
        return f"DATEDIFF({unit}, {start_date}, {end_date})"

    def length_function(self, column: str) -> str:
        return f"LEN({column})"

    def stddev_function(self, column: str) -> str:
        return f"STDDEV_SAMP({column})"

    def sample_query(self, table: str, limit: int) -> str:
        # Use Redshift's random sampling
        return f"SELECT * FROM {table} ORDER BY RANDOM() LIMIT {limit}"

    def aggregate_array(self, column: str) -> str:
        # Redshift doesn't directly support array aggregation, use LISTAGG as workaround
        return f"LISTAGG({column}, ',')"


class BigQueryAdapter(SqlAdapter):
    """Adapter for Google BigQuery."""

    def percentile_query(self, column: str, percentile: float) -> str:
        # BigQuery uses PERCENTILE_CONT for percentile calculation
        return f"PERCENTILE_CONT({column}, {percentile}) OVER()"

    def regex_match(self, column: str, pattern: str) -> str:
        # BigQuery uses REGEXP_CONTAINS for regex matching
        return f"REGEXP_CONTAINS({column}, r'{pattern}')"

    def date_diff(self, unit: str, start_date: str, end_date: str) -> str:
        # BigQuery uses DATE_DIFF function
        bq_unit = unit.upper()
        return f"DATE_DIFF({end_date}, {start_date}, {bq_unit})"

    def length_function(self, column: str) -> str:
        return f"LENGTH({column})"

    def stddev_function(self, column: str) -> str:
        return f"STDDEV({column})"

    def sample_query(self, table: str, limit: int) -> str:
        # Use BigQuery's TABLESAMPLE clause
        return f"SELECT * FROM {table} TABLESAMPLE SYSTEM (10 PERCENT) LIMIT {limit}"

    def aggregate_array(self, column: str) -> str:
        return f"ARRAY_AGG({column})"


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

    def aggregate_array(self, column: str) -> str:
        # SQLite doesn't support array aggregation natively
        return f"GROUP_CONCAT({column}, ',')"


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

    def aggregate_array(self, column: str) -> str:
        # Default implementation, will need to be overridden for specific databases
        return f"ARRAY_AGG({column})"


# Utility functions
def get_dialect_name(engine) -> str:
    """Get the dialect name from the engine."""
    return engine.dialect.name.lower()


def is_supported_dialect(dialect_name: str) -> bool:
    """Check if the dialect is explicitly supported."""
    supported = ['snowflake', 'duckdb', 'postgresql', 'postgres', 'redshift', 'bigquery']
    return any(s in dialect_name for s in supported)


def get_adapter_for_connection(connection_string_or_engine) -> SqlAdapter:
    """Get the appropriate adapter for a connection string or engine."""
    return SqlAdapter.get_adapter(connection_string_or_engine)