"""
Tests for the profile_engine module.
"""
import pytest
from sparvi.profiler.profile_engine import profile_table


def test_basic_profile(sample_db_path):
    """Test basic profiling of a table."""
    # Profile the employees table
    profile = profile_table(sample_db_path, "employees", include_samples=True)

    # Basic assertions
    assert profile is not None
    assert profile["table"] == "employees"
    assert profile["row_count"] == 10

    # Check completeness calculations
    assert "completeness" in profile
    assert "name" in profile["completeness"]
    assert profile["completeness"]["name"]["nulls"] == 0
    assert profile["completeness"]["age"]["nulls"] == 2

    # Check numeric stats
    assert "numeric_stats" in profile
    assert "salary" in profile["numeric_stats"]
    assert profile["numeric_stats"]["salary"]["min"] == 50000
    assert profile["numeric_stats"]["salary"]["max"] == 140000

    # Check sample data
    assert "samples" in profile
    assert len(profile["samples"]) > 0


def test_comparison_with_historical_data(sample_db_path):
    """Test profiling with historical data for comparison."""
    # Create mock historical data
    historical_data = {
        "table": "employees",
        "row_count": 8,  # Different from current (10)
        "completeness": {
            "name": {"nulls": 0, "null_percentage": 0},
            "age": {"nulls": 1, "null_percentage": 12.5},  # Different from current
            "salary": {"nulls": 1, "null_percentage": 12.5},
            "department": {"nulls": 0, "null_percentage": 0}  # Different from current
        },
        "numeric_stats": {
            "salary": {"avg": 70000}  # Different from current
        }
    }

    # Profile with historical data
    profile = profile_table(sample_db_path, "employees", historical_data)

    # Check for detected anomalies
    assert "anomalies" in profile
    assert len(profile["anomalies"]) > 0

    # Check specific anomaly types
    anomaly_types = [a["type"] for a in profile["anomalies"]]
    assert "row_count" in anomaly_types  # Should detect row count change