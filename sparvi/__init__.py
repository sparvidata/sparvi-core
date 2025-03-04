"""
Sparvi Core - Data profiling and validation engine for modern data warehouses

Sparvi helps data engineers and analysts maintain high-quality data by monitoring
schema changes, detecting anomalies, and validating data against custom rules.
Like a hawk keeping watch over your data, Sparvi ensures data integrity with
precision and vigilance.

Main components:
- Profiler: Analyze and monitor data quality metrics
- Validator: Define and run custom validation rules
- CLI: Command-line interface for easy integration
"""

from sparvi.version import __version__

# Define public API
__all__ = ["__version__"]

# Import core functionality
try:
    from sparvi.profiler.profile_engine import profile_table
    from sparvi.validations.validator import run_validations, load_rules_from_file
    from sparvi.validations.default_validations import get_default_validations

    # Add to public API
    __all__.extend([
        "profile_table",
        "run_validations",
        "load_rules_from_file",
        "get_default_validations"
    ])
except ImportError:
    pass  # Allow partial imports