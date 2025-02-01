"""Configuration management for the Keboola Storage Daemon.

This module handles loading and validating configuration from environment variables
and optional config files (JSON/YAML).
"""

import os
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv

class ConfigurationError(Exception):
    """Raised when there are issues with configuration loading or validation."""
    pass

class Config:
    """Configuration manager for the daemon.
    
    Handles loading configuration from environment variables and optional config files.
    Environment variables take precedence over config file values.
    """
    
    REQUIRED_ENV_VARS = {
        'KEBOOLA_STACK_URL': 'Keboola Stack endpoint URL',
        'KEBOOLA_API_TOKEN': 'Keboola Storage API token',
        'WATCHED_DIRECTORY': 'Directory to monitor for changes',
    }

    OPTIONAL_ENV_VARS = {
        'LOG_LEVEL': 'INFO',  # Default log level
        'LOG_FILE': 'daemon.log',  # Default log file name
        'CONFIG_FILE': None,  # Optional config file path
    }

    def __init__(self):
        """Initialize configuration with default values."""
        self._config: Dict[str, Any] = {}
        self._load_config()

    def _load_env_vars(self) -> None:
        """Load and validate environment variables."""
        # Load .env file if it exists
        load_dotenv()

        # Check required environment variables
        missing_vars = []
        for var, description in self.REQUIRED_ENV_VARS.items():
            value = os.getenv(var)
            if not value:
                missing_vars.append(f"{var} ({description})")
            else:
                self._config[var.lower()] = value

        if missing_vars:
            raise ConfigurationError(
                "Missing required environment variables:\n" + 
                "\n".join(f"- {var}" for var in missing_vars)
            )

        # Load optional environment variables with defaults
        for var, default in self.OPTIONAL_ENV_VARS.items():
            self._config[var.lower()] = os.getenv(var, default)

    def _load_config_file(self) -> None:
        """Load configuration from JSON/YAML file if specified."""
        config_file = self._config.get('config_file')
        if not config_file:
            return

        config_path = Path(config_file)
        if not config_path.exists():
            raise ConfigurationError(f"Config file not found: {config_file}")

        try:
            with open(config_path, 'r') as f:
                if config_path.suffix.lower() in ['.yaml', '.yml']:
                    file_config = yaml.safe_load(f)
                elif config_path.suffix.lower() == '.json':
                    file_config = json.load(f)
                else:
                    raise ConfigurationError(
                        f"Unsupported config file format: {config_path.suffix}"
                    )

                # Update config with file values, but don't override env vars
                for key, value in file_config.items():
                    key = key.lower()
                    if key not in self._config:
                        self._config[key] = value

        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise ConfigurationError(f"Error parsing config file: {e}")

    def _load_config(self) -> None:
        """Load configuration from all sources."""
        self._load_env_vars()
        self._load_config_file()
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate the loaded configuration."""
        # Validate watched directory exists
        watched_dir = Path(self._config['watched_directory'])
        if not watched_dir.exists():
            try:
                watched_dir.mkdir(parents=True)
            except Exception as e:
                raise ConfigurationError(
                    f"Cannot create watched directory: {watched_dir}\n{str(e)}"
                )

        # Validate log level
        valid_log_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        log_level = self._config['log_level'].upper()
        if log_level not in valid_log_levels:
            raise ConfigurationError(
                f"Invalid log level: {log_level}. "
                f"Must be one of: {', '.join(valid_log_levels)}"
            )
        self._config['log_level'] = log_level

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        return self._config.get(key.lower(), default)

    def __getitem__(self, key: str) -> Any:
        """Get configuration value by key (dictionary-style access)."""
        return self._config[key.lower()]

    def __contains__(self, key: str) -> bool:
        """Check if configuration contains a key."""
        return key.lower() in self._config
