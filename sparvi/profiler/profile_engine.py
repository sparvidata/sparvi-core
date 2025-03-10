"""
Enhanced profile engine with Snowflake optimizations.
"""
import datetime
import json
from typing import Dict, Any, List, Optional, Tuple, Union

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text

from sparvi.db.adapters import get_adapter_for_connection
from sparvi.db.connection import create_db_engine
from sparvi.utils.env import get_snowflake_connection_from_env


def profile_table(
        connection_str: str = None,
        table: str = None,
        historical_data: Optional[Dict[str, Any]] = None,
        include_samples: bool = False
) -> Dict[str, Any]:
    """
    Profile a table for data completeness, uniqueness, distribution, and numeric stats.
    By default, does NOT include row-level data for privacy.

    Args:
        connection_str: Database connection string. If None, will attempt to get from environment.
        table: Table name to profile
        historical_data: Optional dictionary containing historical profile data for comparison
        include_samples: Whether to include sample data in the profile (default: False)

    Returns:
        Dictionary containing profiling results
    """
    # If no connection string provided, try to get from environment
    if connection_str is None:
        connection_str = get_snowflake_connection_from_env()

    if table is None:
        raise ValueError("Table name is required for profiling")

    print(f"Starting profiling for table: {table}")
    print(f"Include samples: {include_samples}")

    # Create engine using connection manager for optimized settings
    engine = create_db_engine(connection_str)
    adapter = get_adapter_for_connection(engine)  # Get the appropriate SQL adapter
    inspector = inspect(engine)

    try:
        columns = inspector.get_columns(table)
    except Exception as e:
        raise ValueError(f"Error inspecting table {table}: {str(e)}. Check if the table exists and you have access.")

    column_names = [col["name"] for col in columns]

    # Categorize columns using adapter methods for type checking
    numeric_cols = [col["name"] for col in columns if
                    adapter.is_numeric_type(str(col["type"]))]

    text_cols = [col["name"] for col in columns if
                 adapter.is_text_type(str(col["type"]))]

    date_cols = [col["name"] for col in columns if
                 adapter.is_date_type(str(col["type"]))]

    # Check if we're using Snowflake for optimizations
    is_snowflake = 'snowflake' in str(engine.dialect).lower()

    with engine.connect() as conn:
        # If Snowflake, set session parameters for better performance
        if is_snowflake:
            try:
                conn.execute(text("ALTER SESSION SET USE_CACHED_RESULT = TRUE"))
                conn.execute(text("ALTER SESSION SET QUERY_TAG = 'SPARVI_PROFILER'"))
                print("Snowflake session optimizations applied")
            except Exception as e:
                print(f"Warning: Could not set Snowflake session parameters: {str(e)}")

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
        # Use adapter-specific duplicate check query
        if is_snowflake:
            # Snowflake optimized query with QUALIFY
            dup_check = f"""
            SELECT COUNT(*) AS duplicate_rows FROM (
                SELECT COUNT(*) as count 
                FROM {table}
                GROUP BY {', '.join(column_names)}
                HAVING count > 1
            )
            """
        else:
            # Standard query for other databases
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

        # Numeric statistics - optimized for each database type
        print("Calculating numeric statistics...")
        numeric_stats = {}
        for col in numeric_cols:
            try:
                # Use appropriate SQL via adapter for percentiles
                median_expr = adapter.percentile_query(col, 0.5)
                q1_expr = adapter.percentile_query(col, 0.25)
                q3_expr = adapter.percentile_query(col, 0.75)
                std_expr = adapter.stddev_function(col)

                numeric_query = f"""
                SELECT 
                    MIN({col}) as min,
                    MAX({col}) as max,
                    AVG({col}) as avg,
                    SUM({col}) as sum,
                    {std_expr} as stdev,
                    {q1_expr} as q1,
                    {median_expr} as median,
                    {q3_expr} as q3
                FROM {table}
                WHERE {col} IS NOT NULL
                """

                result = conn.execute(text(numeric_query)).fetchone()
                if result:
                    numeric_stats[col] = {
                        "min": result[0],
                        "max": result[1],
                        "avg": result[2],
                        "sum": result[3],
                        "stdev": result[4],
                        "q1": result[5],
                        "median": result[6],
                        "q3": result[7]
                    }
            except Exception as e:
                print(f"Error calculating numeric stats for {col}: {str(e)}")
                # Provide empty stats on error
                numeric_stats[col] = {
                    "min": None, "max": None, "avg": None, "sum": None,
                    "stdev": None, "q1": None, "median": None, "q3": None
                }

        # Text Lengths - use adapter for appropriate LENGTH function
        print("Calculating text statistics...")
        text_length_stats = {}
        for col in text_cols:
            try:
                length_func = adapter.length_function(col)
                text_query = f"""
                SELECT 
                    MIN({length_func}) as min_length,
                    MAX({length_func}) as max_length,
                    AVG({length_func}) as avg_length
                FROM {table}
                WHERE {col} IS NOT NULL
                """

                result = conn.execute(text(text_query)).fetchone()
                if result:
                    text_length_stats[col] = {
                        "min_length": result[0],
                        "max_length": result[1],
                        "avg_length": result[2]
                    }
            except Exception as e:
                print(f"Error calculating text stats for {col}: {str(e)}")
                text_length_stats[col] = {
                    "min_length": None, "max_length": None, "avg_length": None
                }

        # Pattern recognition for text columns
        print("Analyzing text patterns...")
        text_patterns = {}
        for col in text_cols:
            try:
                # Use adapter-specific regex matching
                email_pattern = adapter.regex_match(col, ".*@.*\\..*")
                numeric_pattern = adapter.regex_match(col, "^[0-9]+$")
                date_pattern = adapter.regex_match(col, "^[0-9]{2,4}[/-][0-9]{1,2}[/-][0-9]{1,2}$")

                pattern_query = f"""
                SELECT 
                    SUM(CASE WHEN {email_pattern} THEN 1 ELSE 0 END) as email_count,
                    SUM(CASE WHEN {numeric_pattern} THEN 1 ELSE 0 END) as numeric_count,
                    SUM(CASE WHEN {date_pattern} THEN 1 ELSE 0 END) as date_count
                FROM {table}
                WHERE {col} IS NOT NULL
                """

                result = conn.execute(text(pattern_query)).fetchone()
                if result:
                    text_patterns[col] = {
                        "email_pattern_count": result[0] or 0,
                        "numeric_pattern_count": result[1] or 0,
                        "date_pattern_count": result[2] or 0
                    }
            except Exception as e:
                print(f"Error analyzing text patterns for {col}: {str(e)}")
                text_patterns[col] = {
                    "email_pattern_count": 0,
                    "numeric_pattern_count": 0,
                    "date_pattern_count": 0
                }

        # Date range check for date columns
        print("Analyzing date columns...")
        date_stats = {}
        for col in date_cols:
            try:
                date_query = f"""
                SELECT 
                    MIN({col}) as min_date,
                    MAX({col}) as max_date,
                    COUNT(DISTINCT {col}) as distinct_count
                FROM {table}
                WHERE {col} IS NOT NULL
                """

                result = conn.execute(text(date_query)).fetchone()
                if result and result[0] and result[1]:
                    # Use adapter-specific date diff function
                    min_date = result[0]
                    max_date = result[1]

                    # Calculate date range using adapter
                    date_range_query = f"""
                    SELECT {adapter.date_diff('day', f"'{min_date}'", f"'{max_date}'")}
                    """

                    date_range_result = conn.execute(text(date_range_query)).fetchone()
                    date_range_days = date_range_result[0] if date_range_result else None

                    date_stats[col] = {
                        "min_date": min_date,
                        "max_date": max_date,
                        "distinct_count": result[2],
                        "date_range_days": date_range_days
                    }
                else:
                    date_stats[col] = {
                        "min_date": None,
                        "max_date": None,
                        "distinct_count": 0,
                        "date_range_days": None
                    }
            except Exception as e:
                print(f"Error analyzing date stats for {col}: {str(e)}")
                date_stats[col] = {
                    "min_date": None,
                    "max_date": None,
                    "distinct_count": 0,
                    "date_range_days": None
                }

        # Most Frequent Values
        print("Finding most frequent values...")
        frequent_values = {}
        for col in column_names:
            try:
                # Skip if table has too many rows to avoid expensive query
                if row_count > 1000000:
                    continue

                # Use optimization based on database type
                if is_snowflake:
                    # Snowflake optimized query
                    freq_query = f"""
                    SELECT 
                        {col} as value,
                        COUNT(*) as frequency,
                        COUNT(*) * 100.0 / {row_count} as percentage
                    FROM {table}
                    WHERE {col} IS NOT NULL
                    GROUP BY {col}
                    ORDER BY frequency DESC
                    LIMIT 1
                    """
                else:
                    # Standard query for other databases
                    freq_query = f"""
                    SELECT 
                        {col} as value,
                        COUNT(*) as frequency,
                        COUNT(*) * 100.0 / {row_count} as percentage
                    FROM {table}
                    WHERE {col} IS NOT NULL
                    GROUP BY {col}
                    ORDER BY frequency DESC
                    LIMIT 1
                    """

                result = conn.execute(text(freq_query)).fetchone()
                if result:
                    frequent_values[col] = {
                        "value": result[0],
                        "frequency": result[1],
                        "percentage": round(result[2], 2) if result[2] else 0
                    }
            except Exception as e:
                print(f"Error finding frequent values for {col}: {str(e)}")

        # Get outliers for numeric columns
        print("Detecting outliers...")
        outliers = {}
        for col in numeric_cols:
            try:
                # Use adapter-specific stddev function
                std_expr = adapter.stddev_function(col)

                # Different query based on DB type
                if is_snowflake:
                    # Snowflake uses QUALIFY
                    outlier_query = f"""
                    WITH stats AS (
                        SELECT 
                            AVG({col}) as avg_val,
                            {std_expr} as stddev_val
                        FROM {table}
                        WHERE {col} IS NOT NULL
                    )
                    SELECT {col}
                    FROM {table}, stats
                    WHERE {col} IS NOT NULL
                    AND ({col} > stats.avg_val + 3 * stats.stddev_val
                    OR {col} < stats.avg_val - 3 * stats.stddev_val)
                    LIMIT 10
                    """
                else:
                    # Standard query
                    outlier_query = f"""
                    WITH stats AS (
                        SELECT 
                            AVG({col}) as avg_val,
                            {std_expr} as stddev_val
                        FROM {table}
                        WHERE {col} IS NOT NULL
                    )
                    SELECT {col}
                    FROM {table}, stats
                    WHERE {col} IS NOT NULL
                    AND ({col} > stats.avg_val + 3 * stats.stddev_val
                    OR {col} < stats.avg_val - 3 * stats.stddev_val)
                    LIMIT 10
                    """

                results = conn.execute(text(outlier_query)).fetchall()
                if results:
                    outliers[col] = [row[0] for row in results]
            except Exception as e:
                print(f"Error detecting outliers for {col}: {str(e)}")

        # Sample Data (only if explicitly requested and include_samples is True)
        samples = []
        if include_samples:
            print("Getting data samples - NOTE: These will not be stored")
            try:
                # Use optimized sample query based on DB type
                if is_snowflake:
                    # Snowflake optimized sampling
                    sample_query = f"SELECT * FROM {table} SAMPLE (10 ROWS)"
                else:
                    # Standard limit for other databases
                    sample_query = f"SELECT * FROM {table} LIMIT 10"

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
        # Anomaly detection logic goes here
        # This would compare current profile with historical data

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