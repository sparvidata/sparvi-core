"""
Tests for the validator module.
"""
import pytest
from sparvi.validations.validator import run_validations
from sparvi.validations.default_validations import get_default_validations

#
def test_simple_validation(sample_db_path):
    """Test running a simple validation rule."""
    # Define a simple validation rule
    rules = [{
        "name": "check_employee_count",
        "description": "Check that employees table has at least 5 rows",
        "query": "SELECT COUNT(*) FROM employees",
        "operator": "greater_than",
        "expected_value": 5
    }]

    # Run the validation
    results = run_validations(sample_db_path, rules)

    # Check results
    assert len(results) == 1
    assert results[0]["rule_name"] == "check_employee_count"
    assert results[0]["is_valid"] == True


def test_failing_validation(sample_db_path):
    """Test a validation rule that should fail."""
    # Define a validation rule that should fail
    rules = [{
        "name": "check_no_nulls",
        "description": "Check that no departments are NULL",
        "query": "SELECT COUNT(*) FROM employees WHERE department IS NULL",
        "operator": "equals",
        "expected_value": 0
    }]

    # Run the validation
    results = run_validations(sample_db_path, rules)

    # Check results - should fail because there is a NULL department
    assert len(results) == 1
    assert results[0]["rule_name"] == "check_no_nulls"
    assert results[0]["is_valid"] == False


def test_default_validations(sample_db_path):
    """Test generating and running default validations."""
    # Get default validations
    default_rules = get_default_validations(sample_db_path, "products")

    # Check that we got some rules
    assert len(default_rules) > 0

    # There should be a rule checking for negative prices
    price_rules = [r for r in default_rules if "price" in r["name"] and "positive" in r["name"]]
    assert len(price_rules) > 0

    # Run validations and check results
    results = run_validations(sample_db_path, price_rules)
    assert len(results) > 0

    # The validation should fail because product E has a negative price
    price_check_result = results[0]
    assert price_check_result["is_valid"] == False