"""Directory monitoring for the Keboola Storage Daemon.

This module handles filesystem events using watchdog and processes
directory/file changes accordingly.
"""

import os
import csv
import gzip
from pathlib import Path
from typing import Optional
import logging
from watchdog.observers import Observer
from watchdog.events import (
    FileSystemEventHandler,
    FileCreatedEvent,
    FileModifiedEvent,
    DirCreatedEvent
)

from .utils import format_bytes, get_file_encoding, sanitize_bucket_name

class StorageEventHandler(FileSystemEventHandler):
    """Handles filesystem events and processes them for Keboola Storage."""

    def __init__(self, logger: logging.Logger):
        """Initialize the event handler.
        
        Args:
            logger: Logger instance for event logging
        """
        self.logger = logger
        self._processing = set()  # Track files being processed to avoid duplicates

    def on_created(self, event):
        """Handle creation events for directories and files."""
        if event.is_directory:
            self._handle_directory_created(event)
        elif self._is_csv_file(event.src_path):
            self._handle_file_created(event)

    def on_modified(self, event):
        """Handle file modification events."""
        if not event.is_directory and self._is_csv_file(event.src_path):
            self._handle_file_modified(event)

    def _handle_directory_created(self, event: DirCreatedEvent):
        """Process new directory creation.
        
        Creates corresponding bucket in Keboola Storage.
        """
        dir_path = Path(event.src_path)
        bucket_name = sanitize_bucket_name(dir_path.name)
        
        self.logger.info(
            'Directory created',
            extra={
                'event': 'directory_created',
                'path': str(dir_path),
                'bucket_name': bucket_name
            }
        )
        
        # TODO: Create bucket in Keboola Storage
        # This will be implemented when we add Keboola Storage client

    def _handle_file_created(self, event: FileCreatedEvent):
        """Process new CSV file creation.
        
        Creates new table in Keboola Storage and performs initial load.
        """
        if event.src_path in self._processing:
            return
        
        self._processing.add(event.src_path)
        try:
            file_path = Path(event.src_path)
            
            self.logger.info(
                'CSV file created',
                extra={
                    'event': 'file_created',
                    'path': str(file_path),
                    'size': format_bytes(file_path.stat().st_size)
                }
            )
            
            # Validate CSV and get its properties
            dialect, header = self._analyze_csv(file_path)
            if not dialect or not header:
                return
            
            # TODO: Create table in Keboola Storage and perform initial load
            # This will be implemented when we add Keboola Storage client
            
        except Exception as e:
            self.logger.error(
                'Error processing new CSV file',
                extra={
                    'event': 'file_created_error',
                    'path': event.src_path,
                    'error': str(e)
                }
            )
        finally:
            self._processing.remove(event.src_path)

    def _handle_file_modified(self, event: FileModifiedEvent):
        """Process CSV file modifications.
        
        Validates header consistency and performs full load to existing table.
        """
        if event.src_path in self._processing:
            return
            
        self._processing.add(event.src_path)
        try:
            file_path = Path(event.src_path)
            
            self.logger.info(
                'CSV file modified',
                extra={
                    'event': 'file_modified',
                    'path': str(file_path),
                    'size': format_bytes(file_path.stat().st_size)
                }
            )
            
            # Validate CSV and check header consistency
            dialect, header = self._analyze_csv(file_path)
            if not dialect or not header:
                return
                
            # TODO: Perform full load to existing table
            # This will be implemented when we add Keboola Storage client
            
        except Exception as e:
            self.logger.error(
                'Error processing modified CSV file',
                extra={
                    'event': 'file_modified_error',
                    'path': event.src_path,
                    'error': str(e)
                }
            )
        finally:
            self._processing.remove(event.src_path)

    def _is_csv_file(self, path: str) -> bool:
        """Check if the file is a CSV file."""
        return path.lower().endswith('.csv')

    def _analyze_csv(self, file_path: Path) -> tuple[Optional[csv.Dialect], Optional[list[str]]]:
        """Analyze CSV file to detect dialect and validate header.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Tuple of (dialect, header) if valid, (None, None) if invalid
        """
        try:
            # Read a sample of the file to detect dialect
            sample_size = 1024 * 1024  # 1MB sample
            with open(file_path, 'r', encoding=get_file_encoding(str(file_path))) as f:
                sample = f.read(sample_size)
            
            dialect = csv.Sniffer().sniff(sample)
            has_header = csv.Sniffer().has_header(sample)
            
            if not has_header:
                self.logger.error(
                    'CSV file has no header',
                    extra={
                        'event': 'csv_validation_error',
                        'path': str(file_path),
                        'error': 'No header detected'
                    }
                )
                return None, None
            
            # Read the header
            with open(file_path, 'r', encoding=get_file_encoding(str(file_path))) as f:
                reader = csv.reader(f, dialect=dialect)
                header = next(reader)
            
            if not header:
                self.logger.error(
                    'Empty CSV header',
                    extra={
                        'event': 'csv_validation_error',
                        'path': str(file_path),
                        'error': 'Empty header'
                    }
                )
                return None, None
            
            return dialect, header
            
        except Exception as e:
            self.logger.error(
                'Error analyzing CSV file',
                extra={
                    'event': 'csv_analysis_error',
                    'path': str(file_path),
                    'error': str(e)
                }
            )
            return None, None

class DirectoryWatcher:
    """Watches a directory for changes and processes events."""
    
    def __init__(self, path: str, logger: logging.Logger):
        """Initialize the directory watcher.
        
        Args:
            path: Directory path to watch
            logger: Logger instance for event logging
        """
        self.path = Path(path)
        self.logger = logger
        self.observer = Observer()
        self.event_handler = StorageEventHandler(logger)
        
    def start(self):
        """Start watching the directory."""
        self.observer.schedule(self.event_handler, str(self.path), recursive=True)
        self.observer.start()
        
        self.logger.info(
            'Directory watcher started',
            extra={
                'event': 'watcher_started',
                'path': str(self.path)
            }
        )
    
    def stop(self):
        """Stop watching the directory."""
        self.observer.stop()
        self.observer.join()
        
        self.logger.info(
            'Directory watcher stopped',
            extra={
                'event': 'watcher_stopped',
                'path': str(self.path)
            }
        )
