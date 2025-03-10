# Sparvi Core

[![PyPI version](https://badge.fury.io/py/sparvi-core.svg)](https://badge.fury.io/py/sparvi-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> Like a hawk keeping watch over your data, Sparvi monitors data pipelines, detects anomalies, tracks schema changes, and ensures data integrity with sharp precision.

Sparvi Core is a Python library for data profiling and validation in modern data warehouses. It helps data engineers and analysts maintain high-quality data by monitoring schema changes, detecting anomalies, and validating data against custom rules.

## Features

### Data Profiling

- **Automated Metrics**: Compute essential quality metrics (null rates, duplicates, outliers) to understand your data's health at a glance
- **Schema Analysis**: Detect column types, relationships, and constraints
- **Distribution Analysis**: Understand the distribution of values in your data
- **Historical Comparisons**: Compare current profiles with previous runs to detect changes
- **Anomaly Detection**: Automatically detect anomalies in your data

### Data Validation

- **Custom Validation Rules**: Define and run your own validation rules
- **SQL-Based Rules**: Use SQL to define validation queries
- **Default Rules Generator**: Automatically generate sensible validation rules based on your data
- **Detailed Results**: Get comprehensive information about validation failures

## Installation

```bash
# Basic installation
pip install sparvi-core

# With optimized support for Snowflake (recommended for Snowflake users)
pip install sparvi-core[snowflake]

# With support for PostgreSQL
pip install sparvi-core[postgres]

# With all extras
pip install sparvi-core[snowflake,postgres]
```

## Quick Start

### Command Line Interface

Profile a table:

```bash
# Basic profiling with Snowflake
sparvi profile "snowflake://user:pass@account/database/schema?warehouse=wh" orders

# Basic profiling with DuckDB
sparvi profile "duckdb:///path/to/database.duckdb" employees

# Save the profile to a file
sparvi profile "postgresql://user:pass@localhost/mydatabase" customers --output profile.json

# Compare with a previous profile
sparvi profile "snowflake://user:pass@account/database/schema?warehouse=wh" orders --compare previous_profile.json
```

Validate a table:

```bash
# Generate and run default validations
sparvi validate "snowflake://user:pass@account/database/schema?warehouse=wh" orders --generate-defaults

# Save the default rules to a YAML file
sparvi validate "duckdb:///path/to/database.duckdb" employees --generate-defaults --save-defaults rules.yaml

# Run validations from a file
sparvi validate "postgresql://user:pass@localhost/mydatabase" customers --rules rules.yaml

# Save validation results to a file
sparvi validate "snowflake://user:pass@account/database/schema?warehouse=wh" orders --rules rules.yaml --output results.json
```

### Creating Custom Validations

You can create custom validation rules in YAML format:

```yaml
rules:
- name: check_orders_not_empty
  description: Ensure orders table has at least one row
  query: SELECT COUNT(*) FROM orders
  operator: greater_than
  expected_value: 0

- name: check_revenue_positive
  description: Ensure all order revenue is positive
  query: SELECT COUNT(*) FROM orders WHERE revenue <= 0
  operator: equals
  expected_value: 0

- name: check_shipping_addresses_valid
  description: Ensure shipping addresses contain valid format
  query: >
    SELECT COUNT(*) FROM orders 
    WHERE shipping_address IS NOT NULL 
    AND shipping_address NOT LIKE '%_%,%_%'
  operator: equals
  expected_value: 0
```

Save these rules to a file (e.g., `my_rules.yaml`) and run them with:

```bash
sparvi validate "your_connection_string" orders --rules my_rules.yaml
```

### Python API

Profile a table:

```python
from sparvi.profiler.profile_engine import profile_table

# Run a profile on a Snowflake table
profile = profile_table("snowflake://user:pass@account/database/schema?warehouse=wh", "orders")

# Or use environment variables (recommended for production)
import os
os.environ["SNOWFLAKE_USER"] = "your_user"
os.environ["SNOWFLAKE_PASSWORD"] = "your_password"
os.environ["SNOWFLAKE_ACCOUNT"] = "your_account"
os.environ["SNOWFLAKE_DATABASE"] = "your_database"
os.environ["SNOWFLAKE_SCHEMA"] = "your_schema"
os.environ["SNOWFLAKE_WAREHOUSE"] = "your_warehouse"

# Then profile using environment variables
profile = profile_table(table="orders")  # Connection string is optional if env vars are set

# Check completeness
for column, stats in profile["completeness"].items():
    print(f"{column}: {stats['null_percentage']}% null, {stats['distinct_percentage']}% distinct")

# Check for anomalies
for anomaly in profile.get("anomalies", []):
    print(f"Anomaly: {anomaly['description']}")

# Check for schema shifts
for shift in profile.get("schema_shifts", []):
    print(f"Schema shift: {shift['description']}")
```

Validate a table:

```python
from sparvi.validations.validator import run_validations, load_rules_from_file
from sparvi.validations.default_validations import get_default_validations

# Generate default validation rules for a Snowflake table
rules = get_default_validations("snowflake://user:pass@account/database/schema?warehouse=wh", "orders")

# Run the validations
results = run_validations("snowflake://user:pass@account/database/schema?warehouse=wh", rules)

# Check results
for result in results:
    status = "PASS" if result["is_valid"] else "FAIL"
    print(f"{result['rule_name']}: {status}")
    if not result["is_valid"]:
        print(f"  Expected: {result['expected_value']}, Actual: {result['actual_value']}")
```

## Validation Framework

Sparvi Core's validation framework allows you to define, run, and manage data quality rules across your data warehouse.

### Using Default Validations

The easiest way to get started with validations is to use the default rules generator:

```python
from sparvi.validations.default_validations import get_default_validations
from sparvi.validations.validator import run_validations

# Generate default validation rules based on table structure
rules = get_default_validations("snowflake://user:pass@account/database/schema?warehouse=wh", "orders")

# Run the validations
results = run_validations("snowflake://user:pass@account/database/schema?warehouse=wh", rules)

# Export the default rules for later use
from sparvi.validations.validator import export_rules
export_rules(rules, "order_rules.yaml", format="yaml")
```

Default validations include checks for:
- Empty tables
- Primary key uniqueness
- Missing required fields
- Negative values in numeric columns
- Invalid date ranges
- Format validation for emails, phone numbers, etc.
- Statistical outliers

### Creating Custom Validations in YAML

YAML files provide a clean, readable way to define validation rules. Create a file like `my_rules.yaml`:

```yaml
rules:
- name: check_orders_not_empty
  description: Ensure orders table has at least one row
  query: SELECT COUNT(*) FROM orders
  operator: greater_than
  expected_value: 0

- name: check_revenue_positive
  description: Ensure all order revenue is positive
  query: SELECT COUNT(*) FROM orders WHERE revenue <= 0
  operator: equals
  expected_value: 0

- name: check_high_value_orders_reviewed
  description: Ensure all high-value orders are reviewed
  query: >
    SELECT COUNT(*) FROM orders 
    WHERE total_amount > 10000 
    AND review_status IS NULL
  operator: equals
  expected_value: 0
  
- name: check_shipping_timeframe
  description: Check if shipping dates make sense
  query: >
    SELECT COUNT(*) FROM orders
    WHERE ship_date < order_date
    OR ship_date > DATEADD(day, 30, order_date)
  operator: equals
  expected_value: 0
  
- name: check_customer_distribution
  description: Make sure no single customer represents >50% of orders
  query: >
    WITH customer_counts AS (
      SELECT customer_id, COUNT(*) AS order_count,
      COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders) AS percentage
      FROM orders
      GROUP BY customer_id
    )
    SELECT COUNT(*) FROM customer_counts WHERE percentage > 50
  operator: equals
  expected_value: 0
```

Run these validations using the CLI:

```bash
sparvi validate "snowflake://user:pass@account/database/schema?warehouse=wh" orders --rules my_rules.yaml --output results.json
```

Or from Python:

```python
from sparvi.validations.validator import load_rules_from_file, run_validations

# Load rules from YAML file
rules = load_rules_from_file("my_rules.yaml")

# Run validations
results = run_validations("your_connection_string", rules)
```

### Creating Custom Validations in Python

You can programmatically create and run custom validation rules:

```python
from sparvi.validations.validator import run_validations, export_rules

# Define custom validation rules
custom_rules = [
    {
        "name": "check_orders_recent_data",
        "description": "Ensure orders table has data for current month",
        "query": "SELECT COUNT(*) FROM orders WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE())",
        "operator": "greater_than",
        "expected_value": 0
    },
    {
        "name": "check_high_value_orders_have_approval",
        "description": "Ensure all orders >$10,000 have manager approval",
        "query": """
            SELECT COUNT(*) FROM orders 
            WHERE total_amount > 10000 
            AND manager_approval_id IS NULL
        """,
        "operator": "equals",
        "expected_value": 0
    }
]

# Run the custom validations
results = run_validations("snowflake://user:pass@account/database/schema?warehouse=wh", custom_rules)

# Export the rules for future use
export_rules(custom_rules, "high_value_order_rules.yaml", format="yaml")

# Later, load and run the rules
from sparvi.validations.validator import load_rules_from_file
saved_rules = load_rules_from_file("high_value_order_rules.yaml")
results = run_validations("your_connection_string", saved_rules)
```

### Available Validation Operators

Sparvi supports several comparison operators for validation rules:

- `equals`: Actual value must exactly match expected value
- `greater_than`: Actual value must be greater than expected value
- `less_than`: Actual value must be less than expected value
- `between`: Actual value must be between a range (provide expected_value as [min, max])

### Working with Validation Results

Validation results contain detailed information about each rule:

```python
# Analyze validation results
for result in results:
    status = "PASS" if result["is_valid"] else "FAIL"
    print(f"{result['rule_name']}: {status}")
    
    if not result["is_valid"]:
        print(f"  Description: {result['description']}")
        print(f"  Expected: {result['expected_value']}, Actual: {result['actual_value']}")

# Save results to JSON for reporting
import json
with open("validation_results.json", "w") as f:
    json.dump({"results": results}, f, indent=2)
```

Example results JSON:

```json
{
  "results": [
    {
      "rule_name": "check_orders_not_empty",
      "is_valid": true,
      "actual_value": 1245,
      "expected_value": 0,
      "description": "Ensure orders table has at least one row"
    },
    {
      "rule_name": "check_revenue_positive",
      "is_valid": false,
      "actual_value": 3,
      "expected_value": 0,
      "description": "Ensure all order revenue is positive"
    }
  ]
}
```

## Data Profiling

Sparvi Core provides comprehensive data profiling capabilities to help you understand your data's quality, structure, and characteristics.

### Basic Profiling

```python
from sparvi.profiler.profile_engine import profile_table

# Profile a table
profile = profile_table("snowflake://user:pass@account/database/schema?warehouse=wh", "customers")

# Access profile metrics
print(f"Row count: {profile['row_count']}")
print(f"Duplicate count: {profile['duplicate_count']}")

# Column completeness
for column, stats in profile["completeness"].items():
    print(f"{column}: {stats['null_percentage']}% null, {stats['distinct_percentage']}% distinct")
```

### Profiling Features

Sparvi's profiler collects a wide range of metrics:

1. **Basic Statistics**
   - Row count
   - Duplicate row count
   - Column counts

2. **Completeness Analysis**
   - NULL value percentages
   - Distinct value percentages
   - Most frequent values

3. **Numeric Column Analysis**
   - Min/max values
   - Mean, median
   - Standard deviation
   - Quartiles (Q1, Q3)
   - Outlier detection

4. **Text Column Analysis**
   - Min/max/average lengths
   - Pattern recognition (emails, phone numbers, etc.)
   - Common prefixes/suffixes

5. **Date Column Analysis**
   - Date ranges
   - Distribution over time
   - Seasonality detection

6. **Sample Data Collection** (optional)
   - Representative samples for visual inspection

### Using Profile Output

Profile results are returned as a nested dictionary that you can explore, save, or analyze:

```python
# Save profile results to JSON
import json
with open("customer_profile.json", "w") as f:
    json.dump(profile, f, indent=2)

# Load saved profile for comparison
with open("customer_profile.json", "r") as f:
    historical_profile = json.load(f)

# Run profile with historical comparison
new_profile = profile_table(
    "snowflake://user:pass@account/database/schema?warehouse=wh", 
    "customers",
    historical_data=historical_profile
)

# Check for anomalies detected against historical data
if new_profile.get("anomalies"):
    print(f"Found {len(new_profile['anomalies'])} anomalies vs. historical data:")
    for anomaly in new_profile["anomalies"]:
        print(f"- {anomaly['description']} (severity: {anomaly['severity']})")
```

### Command Line Profiling

Profile tables directly from the command line:

```bash
# Basic profiling
sparvi profile "snowflake://user:pass@account/database/schema?warehouse=wh" customers

# Save profile output
sparvi profile "snowflake://user:pass@account/database/schema?warehouse=wh" customers --output customer_profile.json

# Compare with historical profile
sparvi profile "snowflake://user:pass@account/database/schema?warehouse=wh" customers --compare previous_profile.json

# Format options (detailed, default, minimal)
sparvi profile "snowflake://user:pass@account/database/schema?warehouse=wh" customers --format detailed
```

The CLI renders nicely formatted tables in your terminal with color-coding for issues, making it easy to spot problems quickly.

## Database Compatibility

### Snowflake Considerations (Recommended Database)

When working with Snowflake, Sparvi automatically:

- Uses Snowflake's efficient `SAMPLE (n ROWS)` syntax for better performance
- Optimizes session parameters with `USE_CACHED_RESULT = TRUE`
- Employs Snowflake's `REGEXP_LIKE` function for pattern matching
- Sets appropriate query tags for monitoring
- Leverages Snowflake's native date functions

To connect to Snowflake, you can:
- Provide a connection string: `snowflake://user:pass@account/database/schema?warehouse=wh`
- Or use environment variables:
  ```
  SNOWFLAKE_USER=your_user
  SNOWFLAKE_PASSWORD=your_password
  SNOWFLAKE_ACCOUNT=your_account
  SNOWFLAKE_DATABASE=your_database
  SNOWFLAKE_SCHEMA=your_schema
  SNOWFLAKE_WAREHOUSE=your_warehouse
  ```

### PostgreSQL Considerations

When working with PostgreSQL, keep in mind:

- For date difference functions, we use PostgreSQL's `DATE_PART` function
- Regex pattern matching uses PostgreSQL's `~` operator
- When using the `FILTER` clause, ensure you have PostgreSQL 9.4 or higher

### DuckDB Considerations

DuckDB is ideal for local analysis and development:

- No server setup required
- Fast local processing
- Compatible with most SQL features
- Perfect for testing validation rules before running on production data

### Testing Your Setup

To verify your database connection and functionality, you can use:

```bash
# Test connection using CLI
sparvi test-connection --connection "your_connection_string"

# Or use environment variables
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_DATABASE=your_database
sparvi test-connection
```

## Environment Variable Support

Sparvi Core supports using environment variables for sensitive connection information:

```bash
# For Snowflake (recommended)
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_DATABASE=your_database
export SNOWFLAKE_SCHEMA=public
export SNOWFLAKE_WAREHOUSE=compute_wh

# Generic connection string
export DATABASE_URL="your_connection_string"
```

When using the CLI or Python API without specifying a connection string, Sparvi will automatically use these environment variables.

## Contributing

- Contributions are welcome! Please feel free to submit a Pull Request
- For development setup, install with development dependencies: `pip install -e ".[dev]"`
- Run tests with: `pytest`

## License
Apache License 2.0