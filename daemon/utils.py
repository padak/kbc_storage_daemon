# Utility functions for the Keboola Storage Daemon.
#
# This module provides logging setup and other utility functions.

import os
import gzip
import shutil
import logging
import logging.handlers
import time
from functools import wraps
from pathlib import Path
from typing import Optional, Union, BinaryIO, Callable, Any, TypeVar
from pythonjsonlogger import jsonlogger
import csv
import tempfile
from time import sleep

# Type variable for generic return type
T = TypeVar('T')

def with_retries(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 30.0,
    backoff_factor: float = 2.0,
    logger: Optional[logging.Logger] = None
) -> Callable:
    """Decorator for retrying operations with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        backoff_factor: Multiplier for exponential backoff
        logger: Optional logger for retry attempts
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if logger:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed",
                            extra={
                                'function': func.__name__,
                                'error': str(e),
                                'retry_delay': delay
                            }
                        )
                    
                    if attempt < max_attempts - 1:
                        sleep(delay)
                        delay = min(delay * backoff_factor, max_delay)
            
            if logger:
                logger.error(
                    f"All {max_attempts} attempts failed",
                    extra={
                        'function': func.__name__,
                        'error': str(last_exception)
                    }
                )
            
            raise last_exception
            
        return wrapper
    return decorator

def setup_logging(
    log_dir: Optional[str] = None,
    log_file: Optional[str] = None,
    log_level: str = 'INFO'
) -> logging.Logger:
    """Set up logging with JSON formatting and file rotation.
    
    Args:
        log_dir: Directory for log files
        log_file: Name of the log file
        log_level: Logging level
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger('keboola.storage.daemon')
    logger.setLevel(log_level)
    
    # Create formatter
    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s',
        timestamp=True
    )
    
    # Set up file handler if log file is specified
    if log_file:
        # Create log directory if specified and doesn't exist
        if log_dir:
            log_dir_path = Path(log_dir)
            log_dir_path.mkdir(parents=True, exist_ok=True)
            log_path = log_dir_path / log_file
        else:
            log_path = Path(log_file)
            
        # Create file handler with rotation
        handler = logging.handlers.RotatingFileHandler(
            filename=str(log_path),
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=5,
            encoding='utf-8'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    # Always add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def format_bytes(size: int) -> str:
    """Format byte size to human readable string.
    
    Args:
        size: Size in bytes
    
    Returns:
        Formatted string (e.g., "1.5 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"

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
    """Detect file encoding, handling UTF-8 with BOM.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Detected encoding
    """
    with open(file_path, 'rb') as f:
        raw = f.read(4)
        if raw.startswith(b'\xef\xbb\xbf'):
            return 'utf-8-sig'
    return 'utf-8'

def compress_file(
    file_path: Path,
    threshold_bytes: int,
    logger: Optional[logging.Logger] = None
) -> Optional[Path]:
    """Compress file if it exceeds the size threshold.
    
    Args:
        file_path: Path to the file
        threshold_bytes: Size threshold in bytes
        logger: Optional logger instance
        
    Returns:
        Path to compressed file if compression was performed,
        None if no compression was needed
    """
    try:
        file_size = file_path.stat().st_size
        
        if file_size > threshold_bytes:
            if logger:
                logger.info(
                    'Compressing file',
                    extra={
                        'file': str(file_path),
                        'size': format_bytes(file_size),
                        'threshold': format_bytes(threshold_bytes)
                    }
                )
            
            # Create temporary file with .gz extension
            temp_fd, temp_path = tempfile.mkstemp(suffix='.gz')
            os.close(temp_fd)
            
            # Compress the file
            with open(file_path, 'rb') as f_in:
                with gzip.open(temp_path, 'wb') as f_out:
                    f_out.writelines(f_in)
            
            compressed_size = Path(temp_path).stat().st_size
            
            if logger:
                logger.info(
                    'File compressed',
                    extra={
                        'original_size': format_bytes(file_size),
                        'compressed_size': format_bytes(compressed_size),
                        'compression_ratio': f"{(file_size - compressed_size) / file_size:.1%}"
                    }
                )
            
            return Path(temp_path)
        
        return None
        
    except Exception as e:
        if logger:
            logger.error(
                'Error compressing file',
                extra={
                    'file': str(file_path),
                    'error': str(e)
                }
            )
        raise

def get_compressed_reader(file_path: Path):
    """Get appropriate file reader based on compression.
    
    Args:
        file_path: Path to the file
        
    Returns:
        File reader object
    """
    if str(file_path).endswith('.gz'):
        return gzip.open(file_path, 'rb')
    return open(file_path, 'rb')
