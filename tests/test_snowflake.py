import pytest
import os
import sqlalchemy as sa
from unittest import mock
from sqlalchemy import create_engine, text

from sparvi.db.adapters import (
    get_adapter_for_connection,
    SnowflakeAdapter,
    DuckDBAdapter,
    PostgresAdapter
)
from sparvi.profiler.profile_engine import profile_table
from sparvi.validations.validator import run_validations
from sparvi.validations.default_validations import get_default_validations


# Mock environmental variables for testing
@pytest.fixture
def mock_snowflake_env():
    with mock.patch.dict(os.environ, {
        "SNOWFLAKE_USER": "test_user",
        "SNOWFLAKE_PASSWORD": "test_password",
        "SNOWFLAKE_ACCOUNT": "test_account",
        "SNOWFLAKE_DATABASE": "test_db",
        "SNOWFLAKE_SCHEMA": "test_schema",
        "SNOWFLAKE_WAREHOUSE": "test_wh"
    }):
        yield


# Mock Snowflake connection
@pytest.fixture
def mock_snowflake_engine():
    # Create a mock engine that simulates Snowflake
    mock_engine = mock.MagicMock()
    mock_engine.dialect.name = "snowflake"

    # Mock the connect method to return a connection with execute method
    mock_conn = mock.MagicMock()
    mock_result = mock.MagicMock()
    mock_result.fetchone.return_value = (10,)  # Mock row count
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    return mock_engine


# Test adapter selection
def test_adapter_selection():
    """Test that appropriate adapters are selected based on connection string."""
    # Test Snowflake adapter selection
    snowflake_adapter = get_adapter_for_connection("snowflake://user:pass@account/database/schema")
    assert isinstance(snowflake_adapter, SnowflakeAdapter)

    # Test DuckDB adapter selection (should still work)
    duckdb_adapter = get_adapter_for_connection("duckdb:///path/to/db.duckdb")
    assert isinstance(duckdb_adapter, DuckDBAdapter)

    # Test PostgreSQL adapter selection
    postgres_adapter = get_adapter_for_connection("postgresql://user:pass@localhost/database")
    assert isinstance(postgres_adapter, PostgresAdapter)


# Test Snowflake SQL generation
def test_snowflake_sql_dialect():
    """Test that the Snowflake adapter generates correct SQL syntax."""
    adapter = SnowflakeAdapter()

    # Test PERCENTILE query
    percentile_sql = adapter.percentile_query("revenue", 0.95)
    assert "PERCENTILE_CONT(0.95)" in percentile_sql
    assert "ORDER BY revenue" in percentile_sql

    # Test regex matching
    regex_sql = adapter.regex_match("email", ".*@example\\.com")
    assert "REGEXP_LIKE" in regex_sql
    assert "email" in regex_sql
    assert "'.*@example\\.com'" in regex_sql

    # Test date difference
    date_diff_sql = adapter.date_diff("day", "start_date", "end_date")
    assert "DATEDIFF('day', start_date, end_date)" in date_diff_sql

    # Test any new methods you've added to the adapter
    if hasattr(adapter, "sample_query"):
        sample_sql = adapter.sample_query("customers", 10)
        assert "SAMPLE (10 ROWS)" in sample_sql


# Test environment variable handling (if implemented)
def test_snowflake_env_connection(mock_snowflake_env):
    """Test building connection string from environment variables."""
    from sparvi.utils.env import get_snowflake_connection_from_env

    conn_str = get_snowflake_connection_from_env()
    assert "snowflake://test_user:test_password@test_account/test_db/test_schema" in conn_str
    assert "warehouse=test_wh" in conn_str


# Test Snowflake connection management
def test_snowflake_connection_manager():
    """Test the Snowflake connection manager functionality."""
    from sparvi.db.connection import SnowflakeConnectionManager

    # Test connection string parsing
    conn_str = "snowflake://user:pass@account/database/schema?warehouse=compute_wh"
    manager = SnowflakeConnectionManager(conn_str)

    # Verify parsed parameters
    assert manager.params.get("user") == "user"
    assert manager.params.get("account") == "account"
    assert manager.params.get("database") == "database"
    assert manager.params.get("schema") == "schema"
    assert manager.params.get("warehouse") == "compute_wh"

    # Test engine creation with defaults
    with mock.patch("sparvi.db.connection.create_engine") as mock_create_engine:
        engine = manager.get_engine()
        mock_create_engine.assert_called_once()
        # Verify connect_args was passed
        args = mock_create_engine.call_args[1].get("connect_args", {})
        assert "application" in args
        assert args["application"] == "Sparvi"
        assert "session_parameters" in args
        assert "QUERY_TAG" in args["session_parameters"]


# Test profiling with Snowflake
@mock.patch('sparvi.profiler.profile_engine.create_engine')
def test_profile_table_snowflake(mock_create_engine, mock_snowflake_engine):
    """Test profiling a table with Snowflake."""
    mock_create_engine.return_value = mock_snowflake_engine

    # Mock the adapter creation
    with mock.patch('sparvi.profiler.profile_engine.get_adapter_for_connection') as mock_get_adapter:
        mock_adapter = SnowflakeAdapter()
        mock_get_adapter.return_value = mock_adapter

        # Mock inspector to return column info
        mock_inspector = mock.MagicMock()
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": sa.types.Integer()},
            {"name": "name", "type": sa.types.String()},
            {"name": "created_at", "type": sa.types.DateTime()}
        ]
        with mock.patch('sqlalchemy.inspect') as mock_inspect:
            mock_inspect.return_value = mock_inspector

            # Run the profiler
            result = profile_table(
                "snowflake://user:pass@account/database/schema?warehouse=compute_wh",
                "customers"
            )

            # Verify profile results
            assert result is not None
            assert result["table"] == "customers"

            # Verify Snowflake-specific optimizations were applied
            # Check if ALTER SESSION commands were executed
            calls = mock_snowflake_engine.connect().__enter__().execute.call_args_list
            session_calls = [call for call in calls if "ALTER SESSION" in str(call)]

            # This will depend on what optimizations you implemented
            if len(session_calls) > 0:
                assert any("USE_CACHED_RESULT" in str(call) for call in session_calls)


# Test validation rule generation with Snowflake
@mock.patch('sparvi.validations.default_validations.create_engine')
def test_default_validations_snowflake(mock_create_engine, mock_snowflake_engine):
    """Test generating default validations with Snowflake."""
    mock_create_engine.return_value = mock_snowflake_engine

    # Mock the adapter creation
    with mock.patch('sparvi.validations.default_validations.get_adapter_for_connection') as mock_get_adapter:
        mock_adapter = SnowflakeAdapter()
        mock_get_adapter.return_value = mock_adapter

        # Mock inspector to return table info
        mock_inspector = mock.MagicMock()
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": sa.types.Integer(), "nullable": False},
            {"name": "revenue", "type": sa.types.Numeric(), "nullable": True},
            {"name": "email", "type": sa.types.String(), "nullable": True}
        ]
        mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["id"]}
        mock_inspector.get_foreign_keys.return_value = []

        with mock.patch('sqlalchemy.inspect') as mock_inspect:
            mock_inspect.return_value = mock_inspector

            # Generate default validations
            validations = get_default_validations(
                "snowflake://user:pass@account/database/schema?warehouse=compute_wh",
                "customers"
            )

            # Verify validation rules
            assert len(validations) > 0

            # Check for standard validation types
            validation_names = [v["name"] for v in validations]
            assert any("not_empty" in name for name in validation_names)
            assert any("pk_unique" in name for name in validation_names)
            assert any("email" in name and "valid_email" in name for name in validation_names)
            assert any("revenue" in name and "positive" in name for name in validation_names)


# Integration tests (skip if no actual Snowflake connection available)
@pytest.mark.skipif(not os.environ.get("SNOWFLAKE_USER"), reason="Snowflake credentials not available")
def test_real_snowflake_connection():
    """Test connecting to a real Snowflake instance."""
    from sparvi.utils.env import get_snowflake_connection_from_env

    try:
        conn_str = get_snowflake_connection_from_env()
        engine = create_engine(conn_str)

        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.fetchone()[0] == 1

        # Get table list
        inspector = sa.inspect(engine)
        tables = inspector.get_table_names()

        if tables:
            # If tables exist, try to profile the first one
            sample_table = tables[0]
            profile = profile_table(conn_str, sample_table, include_samples=False)
            assert profile["table"] == sample_table
            assert "row_count" in profile

            # Try to generate validations
            validations = get_default_validations(conn_str, sample_table)
            assert len(validations) > 0

            # Try to run validations
            sample_rule = {
                "name": "test_snowflake_rule",
                "description": "Test rule for Snowflake",
                "query": f"SELECT COUNT(*) FROM {sample_table}",
                "operator": "greater_than",
                "expected_value": 0
            }

            results = run_validations(conn_str, [sample_rule])
            assert len(results) == 1
            assert "is_valid" in results[0]

    except Exception as e:
        pytest.fail(f"Real Snowflake connection test failed: {str(e)}")


# Metadata tests
def test_version_dependencies():
    """Test that package metadata includes Snowflake dependencies."""
    # Check if setup.py includes Snowflake dependencies
    import sparvi

    # If your package includes a __requires__ attribute or similar
    if hasattr(sparvi, "__requires__"):
        requires = sparvi.__requires__
        assert any("snowflake" in req.lower() for req in requires)

    # Alternatively, you could check that the package can be imported
    try:
        import snowflake.sqlalchemy
        # If we get here, the dependency is available
        assert True
    except ImportError:
        pytest.fail("snowflake-sqlalchemy dependency is not installed")


# CLI tests
def test_cli_snowflake_defaults():
    """Test CLI defaults for Snowflake."""
    from click.testing import CliRunner
    from sparvi.cli.main import cli

    runner = CliRunner()

    # Test the help output to verify Snowflake examples are included
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "snowflake://" in result.output

    # Test the profile command help
    result = runner.invoke(cli, ["profile", "--help"])
    assert result.exit_code == 0
    # Verify Snowflake is mentioned in examples
    assert "snowflake://" in result.output