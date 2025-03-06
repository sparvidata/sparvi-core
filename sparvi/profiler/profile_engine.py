import datetime
import json
from typing import Dict, Any, List, Optional, Tuple, Union

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text

from sparvi.db.adapters import get_adapter_for_connection


# Update the profile_table function to default to no samples
def profile_table(
        connection_str: str,
        table: str,
        historical_data: Optional[Dict[str, Any]] = None,
        include_samples: bool = False  # Default to False
) -> Dict[str, Any]:
    """
    Profile a table for data completeness, uniqueness, distribution, and numeric stats.
    By default, does NOT include row-level data for privacy.

    Args:
        connection_str: Database connection string
        table: Table name to profile
        historical_data: Optional dictionary containing historical profile data for comparison
        include_samples: Whether to include sample data in the profile (default: False)

    Returns:
        Dictionary containing profiling results
    """
    print(f"Starting profiling for table: {table}")
    print(f"Include samples: {include_samples}")

    engine = create_engine(connection_str)
    adapter = get_adapter_for_connection(engine)  # Get the appropriate SQL adapter
    inspector = inspect(engine)
    columns = inspector.get_columns(table)
    column_names = [col["name"] for col in columns]

    # Categorize columns using adapter methods for type checking
    numeric_cols = [col["name"] for col in columns if
                    adapter.is_numeric_type(str(col["type"]))]

    text_cols = [col["name"] for col in columns if
                 adapter.is_text_type(str(col["type"]))]

    date_cols = [col["name"] for col in columns if
                 adapter.is_date_type(str(col["type"]))]

    with engine.connect() as conn:
        # Core profile logic remains the same...

        # Sample Data (only if explicitly requested and include_samples is True)
        samples = []
        if include_samples:
            print("Getting data samples - NOTE: These will not be stored")
            sample_query = f"SELECT * FROM {table} LIMIT 100"
            try:
                sample_results = conn.execute(text(sample_query))
                if sample_results:
                    # Convert to list of dictionaries
                    columns = sample_results.keys()
                    rows = sample_results.fetchall()
                    samples = [dict(zip(columns, row)) for row in rows]
            except Exception as e:
                print(f"Error getting samples: {str(e)}")

    # Construct the profile dictionary without samples by default
    profile = {
        "table": table,
        "timestamp": datetime.datetime.now().isoformat(),
        "row_count": row_count,
        "duplicate_count": duplicate_count,
        "completeness": {
            col: {
                "nulls": null_counts[col],
                "null_percentage": round((null_counts[col] / row_count) * 100, 2) if row_count > 0 else 0,
                "distinct_count": distinct_counts[col],
                "distinct_percentage": round((distinct_counts[col] / row_count) * 100, 2) if row_count > 0 else 0
            }
            for col in column_names
        },
        "numeric_stats": numeric_stats,
        "text_patterns": text_patterns,
        "text_length_stats": text_length_stats,
        "date_stats": date_stats,
        "frequent_values": frequent_values,
        "outliers": outliers,
    }

    # Add samples only if explicitly requested - these won't be stored in Supabase
    if include_samples and samples:
        profile["samples"] = samples
        print(f"Added {len(samples)} sample rows to profile (will be used for display only)")

    # Compare with historical data to detect anomalies
    anomalies = []
    schema_shifts = []
    if historical_data:
        print("Comparing with historical data...")
        # Rest of anomaly detection logic remains the same...

    # Add anomalies to profile
    profile["anomalies"] = anomalies
    profile["schema_shifts"] = schema_shifts

    # Prepare trend data structure (will be populated from historical runs)
    profile["trends"] = {
        "row_counts": [],
        "null_rates": {},
        "duplicates": []
    }

    print(f"Profiling completed for table: {table}")
    return profile


def detect_schema_shifts(current_profile: Dict, historical_profile: Dict) -> List[Dict]:
    """
    Detect schema changes between current and historical profiles.
    Returns a list of detected shifts with descriptions.
    """
    shifts = []

    # Get current and historical columns
    current_columns = set(current_profile["completeness"].keys())
    historical_columns = set(historical_profile.get("completeness", {}).keys())

    # Check for added columns
    added_columns = current_columns - historical_columns
    for col in added_columns:
        shifts.append({
            "type": "column_added",
            "column": col,
            "description": f"New column added: {col}",
            "severity": "info",
            "timestamp": current_profile["timestamp"]
        })

    # Check for removed columns
    removed_columns = historical_columns - current_columns
    for col in removed_columns:
        shifts.append({
            "type": "column_removed",
            "column": col,
            "description": f"Column removed: {col}",
            "severity": "high",
            "timestamp": current_profile["timestamp"]
        })

    # Check for data type changes (inferred from statistics)
    common_columns = current_columns.intersection(historical_columns)
    for col in common_columns:
        # Check if column shifted between numeric and non-numeric
        was_numeric = col in historical_profile.get("numeric_stats", {})
        is_numeric = col in current_profile.get("numeric_stats", {})

        if was_numeric and not is_numeric:
            shifts.append({
                "type": "type_changed",
                "column": col,
                "description": f"Column {col} changed from numeric to non-numeric",
                "from_type": "numeric",
                "to_type": "non-numeric",
                "severity": "high",
                "timestamp": current_profile["timestamp"]
            })
        elif not was_numeric and is_numeric:
            shifts.append({
                "type": "type_changed",
                "column": col,
                "description": f"Column {col} changed from non-numeric to numeric",
                "from_type": "non-numeric",
                "to_type": "numeric",
                "severity": "high",
                "timestamp": current_profile["timestamp"]
            })

        # Check if column shifted between date and non-date
        was_date = col in historical_profile.get("date_stats", {})
        is_date = col in current_profile.get("date_stats", {})

        if was_date and not is_date:
            shifts.append({
                "type": "type_changed",
                "column": col,
                "description": f"Column {col} changed from date to non-date",
                "from_type": "date",
                "to_type": "non-date",
                "severity": "high",
                "timestamp": current_profile["timestamp"]
            })

#        Check if a text column's max length has changed significantly
        if col in current_profile.get("text_length_stats", {}) and col in historical_profile.get("text_length_stats", {}):
            current_max = current_profile["text_length_stats"][col].get("max_length")
            historical_max = historical_profile["text_length_stats"][col].get("max_length")

            if current_max is not None and historical_max is not None:
                if current_max > historical_max * 1.5:  # 50% increase in max length
                    shifts.append({
                        "type": "length_increased",
                        "column": col,
                        "description": f"Column {col} max length increased from {historical_max} to {current_max}",
                        "from_length": historical_max,
                        "to_length": current_max,
                        "severity": "medium",
                        "timestamp": current_profile["timestamp"]
                    })

    return shifts