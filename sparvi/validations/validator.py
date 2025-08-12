import json
import os
import yaml
from pathlib import Path
from typing import Dict, List, Any, Union, Optional

from sqlalchemy import create_engine, text
from sparvi.db.adapters import get_adapter_for_connection


def load_rules_from_file(file_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """
    Load validation rules from a YAML or JSON file

    Args:
        file_path: Path to YAML or JSON file with validation rules

    Returns:
        List of validation rule dictionaries

    Raises:
        ValueError: If the file format is not supported or file is invalid
    """
    path = Path(file_path) if isinstance(file_path, str) else file_path

    if not path.exists():
        raise ValueError(f"File not found: {path}")

    if path.suffix.lower() in ['.yaml', '.yml']:
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
    elif path.suffix.lower() == '.json':
        with open(path, 'r') as f:
            data = json.load(f)
    else:
        raise ValueError(f"Unsupported file format: {path.suffix}. Use .yaml, .yml, or .json")

    # Check if the data has the expected format (list of rules or dict with rules key)
    if isinstance(data, list):
        rules = data
    elif isinstance(data, dict) and 'rules' in data:
        rules = data['rules']
    else:
        raise ValueError("Invalid rule file format. Expected a list of rules or a dict with a 'rules' key")

    # Validate each rule
    for rule in rules:
        required_fields = ['name', 'query']
        missing_fields = [field for field in required_fields if field not in rule]

        if missing_fields:
            raise ValueError(f"Rule is missing required fields: {', '.join(missing_fields)}")

        # Set default values for optional fields
        if 'description' not in rule:
            rule['description'] = f"Validation rule: {rule['name']}"

        if 'operator' not in rule:
            rule['operator'] = 'equals'

        if 'expected_value' not in rule:
            rule['expected_value'] = 0

    return rules


def run_validations(connection_str: str, validation_rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Run custom validation rules defined by the user.
    Each rule should have a name, query, and expected result.
    """
    import os
    if "DATABASE_URL" not in os.environ:
        # Set a default or log a warning
        os.environ["DATABASE_URL"] = connection_str  # Use the connection_string that's passed to the function
        print(f"Warning: DATABASE_URL was not set, using provided connection string instead")

    results = []

    try:
        engine = create_engine(connection_str)
        # Test the connection
        with engine.connect() as conn:
            pass  # Just test if connection works

        # Get the adapter for this connection type
        adapter = get_adapter_for_connection(engine)

        for rule in validation_rules:
            try:
                with engine.connect() as conn:
                    # Use the adapter for any database-specific operations
                    # For example, if needed for query transformation:
                    # transformed_query = adapter.transform_query(rule["query"])

                    query_result = conn.execute(text(rule["query"])).fetchone()
                    actual_value = query_result[0] if query_result else None

                    is_valid = False
                    if rule["operator"] == "equals" or rule["operator"] == "==":
                        is_valid = actual_value == rule["expected_value"]
                    elif rule["operator"] == "greater_than" or rule["operator"] == ">":
                        is_valid = actual_value > rule["expected_value"]
                    elif rule["operator"] == "less_than" or rule["operator"] == "<":
                        is_valid = actual_value < rule["expected_value"]
                    elif rule["operator"] == "greater_than_or_equal" or rule["operator"] == ">=":
                        is_valid = actual_value >= rule["expected_value"]
                    elif rule["operator"] == "less_than_or_equal" or rule["operator"] == "<=":
                        is_valid = actual_value <= rule["expected_value"]
                    elif rule["operator"] == "not_equals" or rule["operator"] == "!=":
                        is_valid = actual_value != rule["expected_value"]
                    elif rule["operator"] == "between":
                        is_valid = rule["expected_value"][0] <= actual_value <= rule["expected_value"][1]

                    results.append({
                        "name": rule["name"],
                        "is_valid": is_valid,
                        "actual_value": actual_value,
                        "expected_value": rule["expected_value"],
                        "description": rule.get("description", "")
                    })
            except Exception as e:
                results.append({
                    "name": rule["name"],
                    "is_valid": False,
                    "error": str(e),
                    "description": rule.get("description", "")
                })
    except Exception as e:
        # If the engine creation or adapter fails, return failure for all rules
        for rule in validation_rules:
            results.append({
                "name": rule["name"],
                "is_valid": False,
                "error": f"Database connection error: {str(e)}",
                "description": rule.get("description", "")
            })

    return results


def export_rules(rules: List[Dict[str, Any]], file_path: Union[str, Path], format: str = 'yaml') -> None:
    """
    Export validation rules to a file

    Args:
        rules: List of validation rule dictionaries
        file_path: Path to save the rules
        format: Either 'yaml' or 'json'

    Raises:
        ValueError: If the format is not supported
    """
    path = Path(file_path) if isinstance(file_path, str) else file_path

    # Create parent directories if they don't exist
    path.parent.mkdir(parents=True, exist_ok=True)

    # Convert rules to the appropriate format
    if format.lower() == 'yaml':
        with open(path, 'w') as f:
            yaml.dump({'rules': rules}, f, sort_keys=False)
    elif format.lower() == 'json':
        with open(path, 'w') as f:
            json.dump({'rules': rules}, f, indent=2)
    else:
        raise ValueError(f"Unsupported format: {format}. Use 'yaml' or 'json'")