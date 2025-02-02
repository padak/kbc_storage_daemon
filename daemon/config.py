"""Configuration module for the Keboola Storage Daemon."""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

from dotenv import load_dotenv

class ConfigurationError(Exception):
    """Custom exception for configuration errors."""
    pass

class SyncMode:
    """Enumeration of supported synchronization modes."""
    FULL_LOAD = "full_load"
    INCREMENTAL = "incremental"
    STREAMING = "streaming"

    @classmethod
    def is_valid(cls, mode: str) -> bool:
        """Check if a sync mode is valid."""
        return mode in {cls.FULL_LOAD, cls.INCREMENTAL, cls.STREAMING}

class FileMapping:
    """Represents a file-to-table mapping configuration."""
    
    def __init__(self, mapping_dict: Dict):
        """Initialize mapping from dictionary.
        
        Args:
            mapping_dict: Dictionary containing mapping configuration
            
        Raises:
            ConfigurationError: If mapping configuration is invalid
        """
        self.file_path = self._validate_str(mapping_dict, 'file_path')
        self.bucket_id = self._validate_str(mapping_dict, 'bucket_id')
        self.table_id = self._validate_str(mapping_dict, 'table_id')
        self.sync_mode = self._validate_sync_mode(mapping_dict)
        self.enabled = mapping_dict.get('enabled', True)
        self.options = mapping_dict.get('options', {})

    def _validate_str(self, mapping: Dict, key: str) -> str:
        """Validate and return a required string field."""
        if key not in mapping or not isinstance(mapping[key], str):
            raise ConfigurationError(f"Missing or invalid {key} in mapping")
        return mapping[key]

    def _validate_sync_mode(self, mapping: Dict) -> str:
        """Validate and return the sync mode."""
        mode = self._validate_str(mapping, 'sync_mode')
        if not SyncMode.is_valid(mode):
            raise ConfigurationError(
                f"Invalid sync_mode: {mode}. "
                f"Must be one of: {SyncMode.FULL_LOAD}, {SyncMode.INCREMENTAL}, {SyncMode.STREAMING}"
            )
        return mode

    def to_dict(self) -> Dict:
        """Convert mapping to dictionary."""
        return {
            'file_path': self.file_path,
            'bucket_id': self.bucket_id,
            'table_id': self.table_id,
            'sync_mode': self.sync_mode,
            'enabled': self.enabled,
            'options': self.options
        }

class Config:
    """Configuration handler for the daemon."""
    
    def __init__(
        self,
        env_file: Optional[Union[str, Path]] = None,
        config_file: Optional[Union[str, Path]] = None
    ):
        """Initialize configuration from environment variables and config file.
        
        Args:
            env_file: Optional path to .env file
            config_file: Optional path to config.json file
        """
        # Load environment variables
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()

        # Load sensitive configuration from environment
        self._env_config = {
            'keboola_api_token': self._get_required('KEBOOLA_API_TOKEN'),
            'keboola_stack_url': self._get_required('KEBOOLA_STACK_URL'),
        }

        # Load config.json if provided, otherwise use defaults
        self._config = self._load_config_file(config_file) if config_file else {}
        
        # Set default settings if not provided in config file
        self._set_default_settings()
        
        # Initialize mappings
        self._mappings = [
            FileMapping(mapping) 
            for mapping in self._config.get('mappings', [])
        ]

    def _load_config_file(self, config_file: Union[str, Path]) -> Dict:
        """Load and validate the config file.
        
        Args:
            config_file: Path to config.json
            
        Returns:
            Configuration dictionary
            
        Raises:
            ConfigurationError: If config file is invalid or cannot be read
        """
        try:
            with open(config_file) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ConfigurationError(f"Failed to load config file: {e}")

    def _set_default_settings(self):
        """Set default settings if not provided in config file."""
        default_settings = self._config.get('default_settings', {})
        
        self._config['default_settings'] = {
            'watched_directory': default_settings.get(
                'watched_directory',
                self._get_required('WATCHED_DIRECTORY')  # Fallback to env var
            ),
            'log_level': default_settings.get('log_level', 'INFO'),
            'log_file': default_settings.get('log_file', 'daemon.log'),
            'log_dir': default_settings.get('log_dir'),
            'compression_threshold_mb': float(default_settings.get('compression_threshold_mb', 50)),
            'max_retries': int(default_settings.get('max_retries', 3)),
            'initial_retry_delay': float(default_settings.get('initial_retry_delay', 1.0)),
            'max_retry_delay': float(default_settings.get('max_retry_delay', 30.0)),
            'retry_backoff': float(default_settings.get('retry_backoff', 2.0))
        }

    def _get_required(self, key: str) -> str:
        """Get a required environment variable."""
        value = os.getenv(key)
        if value is None:
            raise ConfigurationError(f"Required environment variable {key} is not set")
        return value.strip()

    @property
    def mappings(self) -> List[FileMapping]:
        """Get the list of file mappings."""
        return self._mappings

    def get_mapping_for_file(self, file_path: str) -> Optional[FileMapping]:
        """Get mapping configuration for a file path."""
        for mapping in self._mappings:
            if mapping.enabled and mapping.file_path == file_path:
                return mapping
        return None

    def __getitem__(self, key: str) -> Union[str, float, int]:
        """Get a configuration value."""
        if key in self._env_config:
            return self._env_config[key]
        if key in self._config.get('default_settings', {}):
            return self._config['default_settings'][key]
        raise KeyError(f"Configuration key not found: {key}")
    
    def get(self, key: str, default: Optional[Union[str, float, int]] = None) -> Optional[Union[str, float, int]]:
        """Get a configuration value with a default."""
        try:
            return self[key]
        except KeyError:
            return default

    def __str__(self) -> str:
        """String representation of the configuration."""
        # Create a copy of the config with sensitive values masked
        config_str = {
            'env_config': {
                'keboola_api_token': '***',
                'keboola_stack_url': self._env_config['keboola_stack_url']
            },
            'default_settings': self._config.get('default_settings', {}),
            'mappings': [m.to_dict() for m in self._mappings]
        }
        return str(config_str)
