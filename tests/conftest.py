"""
Test fixtures and configuration for Sparvi Core tests.
"""
import os
import pytest
import tempfile
import duckdb
import pandas as pd

@pytest.fixture
def sample_db_path():
    """Create a temporary in-memory DuckDB database with sample data for testing."""
    # Use in-memory database for testing
    db_path = ":memory:"
    conn = duckdb.connect(db_path)

    # Create test data
    employees_data = {
        "id": range(1, 11),
        "name": [f"Employee {i}" for i in range(1, 11)],
        "age": [25, 30, 35, None, 45, 50, 55, 60, None, 70],
        "salary": [50000, 60000, None, 80000, 90000, 100000, 110000, 120000, None, 140000],
        "department": ["HR", "IT", "Finance", "IT", "HR", "Finance", "HR", None, "IT", "Finance"]
    }

    # Create the employees table
    employees_df = pd.DataFrame(employees_data)
    conn.execute("CREATE TABLE employees AS SELECT * FROM employees_df")

    # Create a second table for testing validations
    products_data = {
        "product_id": range(1, 6),
        "name": ["Product A", "Product B", "Product C", "Product D", "Product E"],
        "price": [10.99, 20.50, 5.99, 100.00, -1.00],  # One negative price for validation testing
        "category": ["Electronics", "Clothing", "Food", "Electronics", "Clothing"]
    }

    products_df = pd.DataFrame(products_data)
    conn.execute("CREATE TABLE products AS SELECT * FROM products_df")

    # Create a file in a temporary directory
    temp_dir = tempfile.mkdtemp()
    db_file = os.path.join(temp_dir, "test.duckdb")

    # Save the in-memory database to a file
    conn.execute(f"EXPORT DATABASE '{db_file}'")
    conn.close()

    # Return the path to the database
    yield f"duckdb:///{db_file}"

    # Clean up after the test
    try:
        os.remove(db_file)
        os.rmdir(temp_dir)
    except:
        pass  # Ignore cleanup errors