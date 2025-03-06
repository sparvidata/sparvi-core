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
        # Create separate queries for basic metrics
        row_count_query = f"SELECT COUNT(*) FROM {table}"
        null_counts_query = f"SELECT {', '.join([f'SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls' for col in column_names])} FROM {table}"
        distinct_counts_query = f"SELECT {', '.join([f'COUNT(DISTINCT {col}) AS {col}_distinct' for col in column_names])} FROM {table}"

        # Execute separate queries for clarity and reliability
        print("Executing row count query...")
        row_count = conn.execute(text(row_count_query)).fetchone()[0]
        print(f"Row count: {row_count}")

        print("Executing null counts query...")
        null_counts_result = conn.execute(text(null_counts_query)).fetchone()

        print("Executing distinct counts query...")
        distinct_counts_result = conn.execute(text(distinct_counts_query)).fetchone()

        # Duplicate check
        print("Checking for duplicates...")
        dup_check = f"""
        SELECT COUNT(*) AS duplicate_rows FROM (
            SELECT COUNT(*) as count FROM {table} GROUP BY {', '.join(column_names)} HAVING count > 1
        ) AS duplicates
        """
        try:
            duplicates_result = conn.execute(text(dup_check)).fetchone()
            duplicate_count = duplicates_result[0] if duplicates_result else 0
        except Exception as e:
            print(f"Error checking for duplicates: {str(e)}")
            duplicate_count = 0

        # Process null counts and distinct counts from results
        null_counts = {}
        distinct_counts = {}

        for i, col in enumerate(column_names):
            null_counts[col] = null_counts_result[i] if i < len(null_counts_result) else 0
            distinct_counts[col] = distinct_counts_result[i] if i < len(distinct_counts_result) else 0

        # Numeric statistics
        print("Calculating numeric statistics...")
        numeric_stats = {}
        for col in numeric_cols:
            # [... numeric stats calculation code ...]
            # Keeping this part brief for clarity
            numeric_stats[col] = {
                "min": 0, "max": 0, "avg": 0, "sum": 0,
                "stdev": 0, "q1": 0, "median": 0, "q3": 0
            }

        # Text Lengths
        print("Calculating text statistics...")
        text_length_stats = {}
        for col in text_cols:
            # [... text stats calculation code ...]
            text_length_stats[col] = {
                "min_length": 0, "max_length": 0, "avg_length": 0
            }

        # Pattern recognition for text columns
        print("Analyzing text patterns...")
        text_patterns = {}
        # [... text pattern analysis code ...]

        # Date range check for date columns
        print("Analyzing date columns...")
        date_stats = {}
        # [... date stats calculation code ...]

        # Most Frequent Values
        print("Finding most frequent values...")
        frequent_values = {}
        # [... frequent values calculation code ...]

        # Get outliers for numeric columns
        print("Detecting outliers...")
        outliers = {}
        # [... outlier detection code ...]

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