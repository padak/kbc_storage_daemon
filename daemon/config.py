"""Configuration module for the Keboola Storage Daemon."""

import os
from pathlib import Path
from typing import Dict, Optional, Union

from dotenv import load_dotenv

class ConfigurationError(Exception):
    """Custom exception for configuration errors."""
    pass

class Config:
    """Configuration handler for the daemon."""
    
    def __init__(self, env_file: Optional[Union[str, Path]] = None):
        """Initialize configuration from environment variables.
        
        Args:
            env_file: Optional path to .env file
        """
        # Load environment variables from .env file if provided
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()  # Look for .env in current directory
        
        # Required configuration
        self._config = {
            'keboola_api_token': self._get_required('KEBOOLA_API_TOKEN'),
            'keboola_stack_url': self._get_required('KEBOOLA_STACK_URL'),
            'watched_directory': self._get_required('WATCHED_DIRECTORY'),
        }
        
        # Optional configuration with defaults
        self._config.update({
            'log_level': self._get_log_level(),
            'log_file': os.getenv('LOG_FILE', 'daemon.log'),
            'log_dir': os.getenv('LOG_DIR'),
            'compression_threshold_mb': float(os.getenv('COMPRESSION_THRESHOLD_MB', '50')),
            'max_retries': int(os.getenv('MAX_RETRIES', '3')),
            'initial_retry_delay': float(os.getenv('INITIAL_RETRY_DELAY', '1.0')),
            'max_retry_delay': float(os.getenv('MAX_RETRY_DELAY', '30.0')),
            'retry_backoff': float(os.getenv('RETRY_BACKOFF', '2.0'))
        })
    
    def _get_required(self, key: str) -> str:
        """Get a required environment variable.
        
        Args:
            key: Environment variable name
            
        Returns:
            The environment variable value
            
        Raises:
            ConfigurationError: If the environment variable is not set
        """
        value = os.getenv(key)
        if value is None:
            raise ConfigurationError(f"Required environment variable {key} is not set")
        return value.strip()
    
    def _get_log_level(self) -> str:
        """Get and validate log level from environment.
        
        Returns:
            Valid log level string
            
        Raises:
            ConfigurationError: If log level is invalid
        """
        valid_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        level = os.getenv('LOG_LEVEL', 'INFO').strip().upper()
        
        # Remove any comments that might be in the value
        level = level.split('#')[0].strip()
        
        if level not in valid_levels:
            raise ConfigurationError(
                f"Invalid log level: {level}. "
                f"Must be one of: {', '.join(valid_levels)}"
            )
        return level
    
    def __getitem__(self, key: str) -> Union[str, float, int]:
        """Get a configuration value.
        
        Args:
            key: Configuration key
            
        Returns:
            The configuration value
            
        Raises:
            KeyError: If the configuration key does not exist
        """
        return self._config[key]
    
    def get(self, key: str, default: Optional[Union[str, float, int]] = None) -> Optional[Union[str, float, int]]:
        """Get a configuration value with a default.
        
        Args:
            key: Configuration key
            default: Default value if key does not exist
            
        Returns:
            The configuration value or default
        """
        return self._config.get(key, default)
    
    def __str__(self) -> str:
        """String representation of the configuration.
        
        Returns:
            Configuration as a string, with sensitive values masked
        """
        # Create a copy of the config with sensitive values masked
        masked_config = self._config.copy()
        masked_config['keboola_api_token'] = '***'
        
        return str(masked_config)
