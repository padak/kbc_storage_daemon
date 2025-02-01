# Utility functions for the Keboola Storage Daemon.
#
# This module provides logging setup and other utility functions.

import logging
import logging.handlers
from pathlib import Path
from typing import Optional
from pythonjsonlogger import jsonlogger

def setup_logging(
    log_file: str,
    log_level: str,
    max_bytes: int = 100 * 1024 * 1024,  # 100MB
    backup_count: int = 5,
    log_dir: Optional[str] = None
) -> logging.Logger:
    """Set up logging with rotation and JSON formatting.
    
    Args:
        log_file: Name of the log file
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        max_bytes: Maximum size of each log file
        backup_count: Number of backup files to keep
        log_dir: Directory for log files (created if doesn't exist)
    
    Returns:
        Logger instance configured with file handler and JSON formatter
    """
    logger = logging.getLogger('kbc_daemon')
    logger.setLevel(log_level.upper())

    # Create log directory if specified
    if log_dir:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        log_file = str(log_path / log_file)

    # Create rotating file handler
    handler = logging.handlers.RotatingFileHandler(
        filename=log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )

    # Create JSON formatter with timestamp
    class CustomJsonFormatter(jsonlogger.JsonFormatter):
        def add_fields(self, log_record, record, message_dict):
            super().add_fields(log_record, record, message_dict)
            if not log_record.get('timestamp'):
                log_record['timestamp'] = self.formatTime(record)
            if log_record.get('level'):
                log_record['level'] = log_record['level'].upper()
            else:
                log_record['level'] = record.levelname

    formatter = CustomJsonFormatter(
        '%(timestamp)s %(level)s %(name)s %(message)s'
    )
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Log startup message
    logger.info(
        'Logging system initialized',
        extra={
            'log_file': log_file,
            'log_level': log_level,
            'max_bytes': max_bytes,
            'backup_count': backup_count
        }
    )

    return logger

def format_bytes(size: int) -> str:
    """Format byte size to human readable string.
    
    Args:
        size: Size in bytes
    
    Returns:
        Formatted string (e.g., "1.23 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def sanitize_bucket_name(name: str) -> str:
    """Sanitize folder name to valid Keboola bucket name.
    
    Args:
        name: Original folder name
    
    Returns:
        Sanitized name valid for Keboola bucket
    """
    # Replace invalid characters with underscore
    sanitized = ''.join(c if c.isalnum() else '_' for c in name.lower())
    # Remove consecutive underscores
    sanitized = '_'.join(filter(None, sanitized.split('_')))
    return sanitized

def get_file_encoding(file_path: str) -> str:
    """Detect file encoding, defaulting to UTF-8.
    
    Args:
        file_path: Path to the file
    
    Returns:
        Detected encoding
    """
    # TODO: Implement more sophisticated encoding detection if needed
    return 'utf-8'
