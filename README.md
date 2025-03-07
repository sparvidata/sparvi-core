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

# With support for Snowflake
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
# Basic profiling
sparvi profile "duckdb:///path/to/database.duckdb" employees

# Save the profile to a file
sparvi profile "postgresql://user:pass@localhost/mydatabase" customers --output profile.json

# Compare with a previous profile
sparvi profile "snowflake://user:pass@account/database/schema?warehouse=wh" orders --compare previous_profile.json
```

Validate a table:

```bash
# Generate and run default validations
sparvi validate "duckdb:///path/to/database.duckdb" employees --generate-defaults

# Save the default rules to a YAML file
sparvi validate "duckdb:///path/to/database.duckdb" employees --generate-defaults --save-defaults rules.yaml

# Run validations from a file
sparvi validate "postgresql://user:pass@localhost/mydatabase" customers --rules rules.yaml

# Save validation results to a file
sparvi validate "snowflake://user:pass@account/database/schema?warehouse=wh" orders --rules rules.yaml --output results.json
```

### Python API

Profile a table:

```python
from sparvi.profiler.profile_engine import profile_table

# Run a profile
profile = profile_table("duckdb:///path/to/database.duckdb", "employees")

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

# Generate default validation rules
rules = get_default_validations("duckdb:///path/to/database.duckdb", "employees")

# Run the validations
results = run_validations("duckdb:///path/to/database.duckdb", rules)

# Check results
for result in results:
    status = "PASS" if result["is_valid"] else "FAIL"
    print(f"{result['rule_name']}: {status}")
    if not result["is_valid"]:
        print(f"  Expected: {result['expected_value']}, Actual: {result['actual_value']}")
```

## Multi-Database Support

Sparvi Core now has enhanced support for multiple database engines:

- **DuckDB**: Included by default, ideal for local analysis
- **PostgreSQL**: Install with `pip install sparvi-core[postgres]`
- **Snowflake**: Install with `pip install sparvi-core[snowflake]`

The library uses database-specific adapters to ensure that SQL queries are optimized for each database engine. This provides consistent results while taking advantage of each database's specific features.

For example, Sparvi automatically adapts:
- Regular expression syntax
- Date/time functions
- Percentile calculations
- String operations

This means you can profile and validate your data using the same API regardless of the underlying database.

## Database Compatibility

### PostgreSQL Considerations

When working with PostgreSQL, keep in mind:

- For date difference functions, we use PostgreSQL's `DATE_PART` function
- Regex pattern matching uses PostgreSQL's `~` operator
- When using the `FILTER` clause, ensure you have PostgreSQL 9.4 or higher

### Snowflake Considerations

When working with Snowflake, keep in mind:

- Regex pattern matching uses Snowflake's `REGEXP_LIKE` function
- String functions may behave slightly differently than in PostgreSQL or DuckDB
- To optimize performance with large Snowflake tables, consider using warehouse sizing options

### Testing Your Setup

To verify your database connection and functionality, you can use:

```python
from sparvi.db.adapters import get_adapter_for_connection

# Test connection with a simple query
engine = create_engine("your_connection_string")
adapter = get_adapter_for_connection(engine)
print(f"Connected to: {adapter.__class__.__name__}")
```

### Contributing

- Contributions are welcome! Please feel free to submit a Pull Request

### License
Apache License 2.0