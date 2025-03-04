import os
import pytest
import duckdb
import pandas as pd
import tempfile
import uuid

@pytest.fixture(scope="session")
def sample_db_path():
    """Create a DuckDB database file in the user home directory."""
    # Create a test directory in the current project folder
    test_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data")
    os.makedirs(test_dir, exist_ok=True)
    
    # Create a unique filename
    db_file = os.path.join(test_dir, f"test_{uuid.uuid4().hex}.duckdb")
    
    # Create connection and tables
    conn = duckdb.connect(db_file)
    
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
    conn.close()
    
    # Return the connection string for the database file
    yield f"duckdb:///{db_file}"
    
    # Clean up
    try:
        os.remove(db_file)
    except:
        pass  # Ignore cleanup errors