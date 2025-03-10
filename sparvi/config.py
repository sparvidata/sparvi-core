"""
Configuration settings for Sparvi Core.
"""
import os
import logging
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('sparvi.config')

# Default settings
DEFAULT_SETTINGS = {
    # Global defaults
    "default_connection_type": "snowflake",
    "sample_row_limit": 100,
    "history_retention_days": 30,
    "log_level": "INFO",

    # Snowflake specific settings (default database)
    "snowflake": {
        "warehouse_size": "X-SMALL",
        "timeout": 60,
        "role": "ACCOUNTADMIN",
        "query_tag": "Sparvi_Profiler",
    },

    # DuckDB specific settings
    "duckdb": {
        "memory_limit": "4GB",
        "threads": -1,  # Use all available
    },

    # PostgreSQL specific settings
    "postgres": {
        "application_name": "Sparvi",
        "connect_timeout": 10,
    },

    # BigQuery specific settings
    "bigquery": {
        "location": "US",
        "maximum_bytes_billed": 1000000000,  # 1GB
    },

    # Redshift specific settings
    "redshift": {
        "timeout": 30,
        "ssl": True,
    },

    # Validation settings
    "validation": {
        "default_operator": "equals",
        "max_rules": 100,
        "max_history": 50,
    },

    # Profiling settings
    "profiling": {
        "include_samples": False,
        "sample_method": "random",
        "anomaly_threshold": 3.0,  # Standard deviations
        "numeric_distribution_buckets": 10,
        "text_pattern_detection": True,
    }
}

# User config file location
USER_CONFIG_LOCATIONS = [
    os.path.expanduser("~/.sparvi/config.yaml"),
    os.path.expanduser("~/.config/sparvi/config.yaml"),
    "sparvi.yaml",
    ".sparvi.yaml",
]


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from default settings, environment, and optionally a config file.

    Args:
        config_path: Optional path to a configuration file.

    Returns:
        Merged configuration dictionary.
    """
    config = DEFAULT_SETTINGS.copy()

    # Try to load from file if specified
    if config_path:
        try:
            import yaml
            with open(config_path, 'r') as f:
                file_config = yaml.safe_load(f)
                if file_config:
                    # Deep merge the configs
                    _deep_merge(config, file_config)
            logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.warning(f"Failed to load config file {config_path}: {str(e)}")
    else:
        # Try default locations if no explicit path
        for loc in USER_CONFIG_LOCATIONS:
            if os.path.exists(loc):
                try:
                    import yaml
                    with open(loc, 'r') as f:
                        file_config = yaml.safe_load(f)
                        if file_config:
                            # Deep merge the configs
                            _deep_merge(config, file_config)
                    logger.info(f"Loaded configuration from {loc}")
                    break
                except Exception as e:
                    logger.warning(f"Failed to load config file {loc}: {str(e)}")

    # Override with environment variables
    _override_from_env(config)

    return config


def _deep_merge(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    """
    Deep merge source dict into target dict.

    Args:
        target: Target dictionary to merge into.
        source: Source dictionary to merge from.
    """
    for key, value in source.items():
        if key in target and isinstance(target[key], dict) and isinstance(value, dict):
            _deep_merge(target[key], value)
        else:
            target[key] = value


def _override_from_env(config: Dict[str, Any]) -> None:
    """
    Override configuration with environment variables.

    Environment variables are mapped as:
    SPARVI_SECTION_KEY=value -> config[section][key] = value

    Args:
        config: Configuration dictionary to update.
    """
    sparvi_env_vars = {k: v for k, v in os.environ.items() if k.startswith('SPARVI_')}

    for env_var, value in sparvi_env_vars.items():
        parts = env_var.split('_')[1:]  # Remove SPARVI_ prefix

        if len(parts) == 1:
            # Top level config
            config[parts[0].lower()] = _parse_env_value(value)
        elif len(parts) >= 2:
            # Section config
            section = parts[0].lower()
            key = '_'.join(parts[1:]).lower()

            if section not in config:
                config[section] = {}

            config[section][key] = _parse_env_value(value)


def _parse_env_value(value: str) -> Any:
    """
    Parse environment variable value into the appropriate type.

    Args:
        value: String value from environment variable.

    Returns:
        Parsed value as appropriate type.
    """
    if value.lower() == 'true':
        return True
    elif value.lower() == 'false':
        return False
    elif value.lower() == 'none':
        return None

    try:
        # Try to convert to int
        return int(value)
    except ValueError:
        try:
            # Try to convert to float
            return float(value)
        except ValueError:
            # Return as string
            return value


# Global configuration
_config = None


def get_config(reload: bool = False, config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Get the global configuration, loading it if needed.

    Args:
        reload: Force reload the configuration.
        config_path: Optional path to a configuration file.

    Returns:
        Configuration dictionary.
    """
    global _config
    if _config is None or reload:
        _config = load_config(config_path)
    return _config


def set_config_value(section: str, key: str, value: Any) -> None:
    """
    Set a configuration value in the global config.

    Args:
        section: Configuration section.
        key: Configuration key.
        value: Value to set.
    """
    config = get_config()

    if section not in config:
        config[section] = {}

    config[section][key] = value