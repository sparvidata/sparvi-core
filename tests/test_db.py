"""
Tests for multi-database support in Sparvi.
"""
import pytest
import os
from sqlalchemy import create_engine, text
from sparvi.db.adapters import SqlAdapter, get_adapter_for_connection
from sparvi.profiler.profile_engine import profile_table
from sparvi.validations.validator import run_validations
from sparvi.validations.default_validations import get_default_validations


def test_adapter_factory():
    """Test that the adapter factory returns the correct adapter type for different connection strings."""
    # DuckDB adapter
    duckdb_adapter = SqlAdapter.get_adapter("duckdb:///memory")
    assert duckdb_adapter.__class__.__name__ == "DuckDBAdapter"

    # PostgreSQL adapter
    postgres_adapter = SqlAdapter.get_adapter("postgresql://user:pass@localhost/mydatabase")
    assert postgres_adapter.__class__.__name__ == "PostgresAdapter"

    # Snowflake adapter
    snowflake_adapter = SqlAdapter.get_adapter("snowflake://user:pass@account/database")
    assert snowflake_adapter.__class__.__name__ == "SnowflakeAdapter"

    # SQLite adapter
    sqlite_adapter = SqlAdapter.get_adapter("sqlite:///test.db")
    assert sqlite_adapter.__class__.__name__ == "SQLiteAdapter"

    # Generic adapter for unknown connection type
    generic_adapter = SqlAdapter.get_adapter("unknown://localhost")
    assert generic_adapter.__class__.__name__ == "GenericAdapter"


def test_adapter_sql_generation():
    """Test that adapters generate appropriate SQL for different dialects."""
    duckdb_adapter = SqlAdapter.get_adapter("duckdb:///memory")
    postgres_adapter = SqlAdapter.get_adapter("postgresql://user:pass@localhost/mydatabase")
    snowflake_adapter = SqlAdapter.get_adapter("snowflake://user:pass@account/database")

    # Test percentile query generation
    assert "PERCENTILE_CONT" in duckdb_adapter.percentile_query("column", 0.5)
    assert "PERCENTILE_CONT" in postgres_adapter.percentile_query("column", 0.5)
    assert "PERCENTILE_CONT" in snowflake_adapter.percentile_query("column", 0.5)

    # Test regex matching
    assert "~" in duckdb_adapter.regex_match("column", "[0-9]+")
    assert "~" in postgres_adapter.regex_match("column", "[0-9]+")
    assert "REGEXP_LIKE" in snowflake_adapter.regex_match("column", "[0-9]+")

    # Test date difference
    assert "DATEDIFF" in duckdb_adapter.date_diff("day", "start_date", "end_date")
    assert "DATE_PART" in postgres_adapter.date_diff("day", "start_date", "end_date")
    assert "DATEDIFF" in snowflake_adapter.date_diff("day", "start_date", "end_date")


@pytest.mark.skipif(not os.environ.get("POSTGRES_TEST_CONNECTION"),
                    reason="Postgres test connection string not provided")
def test_postgres_profile():
    """Test profile functionality with PostgreSQL."""
    connection_string = os.environ.get("POSTGRES_TEST_CONNECTION")

    # Assuming a test table exists in your PostgreSQL database
    table_name = "test_employees"

    # Run profile
    profile = profile_table(connection_string, table_name, include_samples=True)

    # Basic assertions
    assert profile is not None
    assert profile["table"] == table_name
    assert "completeness" in profile
    assert "numeric_stats" in profile


@pytest.mark.skipif(not os.environ.get("SNOWFLAKE_TEST_CONNECTION"),
                    reason="Snowflake test connection string not provided")
def test_snowflake_profile():
    """Test profile functionality with Snowflake."""
    connection_string = os.environ.get("SNOWFLAKE_TEST_CONNECTION")

    # Assuming a test table exists in your Snowflake database
    table_name = "test_employees"

    # Run profile
    profile = profile_table(connection_string, table_name, include_samples=True)

    # Basic assertions
    assert profile is not None
    assert profile["table"] == table_name
    assert "completeness" in profile
    assert "numeric_stats" in profile


@pytest.mark.skipif(not os.environ.get("POSTGRES_TEST_CONNECTION"),
                    reason="Postgres test connection string not provided")
def test_postgres_validations():
    """Test validation functionality with PostgreSQL."""
    connection_string = os.environ.get("POSTGRES_TEST_CONNECTION")

    # Assuming a test table exists in your PostgreSQL database
    table_name = "test_employees"

    # Generate default validations
    validations = get_default_validations(connection_string, table_name)

    # Basic assertions
    assert len(validations) > 0

    # Run a simple validation rule
    rules = [{
        "name": "check_row_count",
        "description": "Ensure table has at least one row",
        "query": f"SELECT COUNT(*) FROM {table_name}",
        "operator": "greater_than",
        "expected_value": 0
    }]

    results = run_validations(connection_string, rules)

    # Basic assertions
    assert len(results) == 1
    assert "rule_name" in results[0]
    assert "is_valid" in results[0]


@pytest.mark.skipif(not os.environ.get("SNOWFLAKE_TEST_CONNECTION"),
                    reason="Snowflake test connection string not provided")
def test_snowflake_validations():
    """Test validation functionality with Snowflake."""
    connection_string = os.environ.get("SNOWFLAKE_TEST_CONNECTION")

    # Assuming a test table exists in your Snowflake database
    table_name = "test_employees"

    # Generate default validations
    validations = get_default_validations(connection_string, table_name)

    # Basic assertions
    assert len(validations) > 0

    # Run a simple validation rule
    rules = [{
        "name": "check_row_count",
        "description": "Ensure table has at least one row",
        "query": f"SELECT COUNT(*) FROM {table_name}",
        "operator": "greater_than",
        "expected_value": 0
    }]

    results = run_validations(connection_string, rules)

    # Basic assertions
    assert len(results) == 1
    assert "rule_name" in results[0]
    assert "is_valid" in results[0]


@pytest.fixture
def setup_database_connections():
    """Setup database connections for integration tests."""
    connections = {}

    # Add DuckDB connection (always available)
    connections["duckdb"] = create_engine("duckdb:///:memory:")

    # Add PostgreSQL connection if available
    postgres_conn = os.environ.get("POSTGRES_TEST_CONNECTION")
    if postgres_conn:
        connections["postgres"] = create_engine(postgres_conn)

    # Add Snowflake connection if available
    snowflake_conn = os.environ.get("SNOWFLAKE_TEST_CONNECTION")
    if snowflake_conn:
        connections["snowflake"] = create_engine(snowflake_conn)

    return connections


def test_create_test_tables(setup_database_connections):
    """Test creating test tables in all available databases."""
    connections = setup_database_connections

    # Create test tables in each database
    for db_type, engine in connections.items():
        try:
            with engine.connect() as conn:
                # Create a simple test table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS test_employees (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(100),
                        age INTEGER,
                        salary NUMERIC(10, 2),
                        department VARCHAR(50),
                        hire_date DATE
                    )
                """))

                # Insert some test data
                conn.execute(text("""
                    INSERT INTO test_employees (id, name, age, salary, department, hire_date)
                    VALUES 
                        (1, 'John Doe', 30, 50000.00, 'HR', '2020-01-15'),
                        (2, 'Jane Smith', 35, 60000.00, 'IT', '2019-05-20'),
                        (3, 'Bob Johnson', 40, 70000.00, 'Finance', '2018-11-10'),
                        (4, 'Alice Brown', NULL, 55000.00, 'Marketing', '2021-03-05'),
                        (5, 'Tom Wilson', 28, NULL, 'IT', '2022-07-01')
                """))

                # Verify table was created
                result = conn.execute(text("SELECT COUNT(*) FROM test_employees")).fetchone()
                assert result[0] == 5

                print(f"Created and populated test table in {db_type}")

        except Exception as e:
            print(f"Error setting up test table in {db_type}: {str(e)}")