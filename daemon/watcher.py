"""Directory monitoring for the Keboola Storage Daemon.

This module handles filesystem events using watchdog and processes
directory/file changes accordingly.
"""

import os
import csv
import gzip
import tempfile
import threading
from pathlib import Path
from typing import Optional, Tuple, Dict, Set
import logging
from watchdog.observers import Observer
from watchdog.events import (
    FileSystemEventHandler,
    FileCreatedEvent,
    FileModifiedEvent,
    DirCreatedEvent,
    FileSystemEvent
)

from .utils import (
    format_bytes,
    get_file_encoding,
    compress_file,
    get_compressed_reader
)
from .storage_client import StorageClient, StorageError

class StorageEventHandler(FileSystemEventHandler):
    """Handles filesystem events and processes them for Keboola Storage."""

    def __init__(
        self,
        storage_client: StorageClient,
        logger: Optional[logging.Logger] = None,
        compression_threshold_mb: float = 50.0
    ):
        """Initialize the event handler.
        
        Args:
            storage_client: Keboola Storage client instance
            logger: Optional logger instance
            compression_threshold_mb: File size threshold for compression in MB
        """
        self.storage = storage_client
        self.logger = logger or logging.getLogger(__name__)
        self.compression_threshold = compression_threshold_mb * 1024 * 1024  # Convert to bytes
        self._processing = set()  # Track files being processed
        self._temp_files = set()  # Track temporary compressed files
        self._processing_lock = threading.Lock()  # Lock for thread safety
        self._stored_headers: Dict[str, list[str]] = {}  # Cache for table headers
        self.selected_bucket_id = os.getenv("SELECTED_BUCKET_ID")
        # Track processed lines for each file
        self._processed_lines: Dict[str, int] = {}

    def __del__(self):
        """Clean up temporary files on destruction."""
        self._cleanup_temp_files()

    def _cleanup_temp_files(self):
        """Clean up temporary files."""
        for temp_file in self._temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception as e:
                self.logger.warning(
                    f'Failed to remove temporary file: {temp_file}',
                    extra={'error': str(e)}
                )

    def _is_file_ready(self, file_path: Path) -> bool:
        """Check if a file is ready for processing.
        
        This helps avoid processing partially written files.
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if file is ready, False otherwise
        """
        try:
            # Try to open file for reading and writing
            with open(file_path, 'rb+') as f:
                return True
        except (IOError, PermissionError):
            return False

    def _add_to_processing(self, file_path: str) -> bool:
        """Add file to processing set with thread safety.
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if file was added, False if already being processed
        """
        with self._processing_lock:
            if file_path in self._processing:
                return False
            self._processing.add(file_path)
            return True

    def _remove_from_processing(self, file_path: str):
        """Remove file from processing set with thread safety."""
        with self._processing_lock:
            self._processing.discard(file_path)

    def on_created(self, event: FileSystemEvent) -> None:
        """Handle creation events.
        
        Args:
            event: File system event
        """
        try:
            if not event.is_directory:
                self._handle_file_created(event)
        except Exception as e:
            self.logger.error(
                f"Error handling creation event: {e}",
                extra={'path': event.src_path}
            )

    def on_modified(self, event: FileSystemEvent) -> None:
        """Handle modification events.
        
        Args:
            event: File system event
        """
        if not event.is_directory:
            try:
                self._handle_file_modified(event)
            except Exception as e:
                self.logger.error(
                    f"Error handling modification event: {e}",
                    extra={'path': event.src_path}
                )

    def _count_lines(self, file_path: Path) -> int:
        """Count number of lines in a file."""
        with open(file_path, 'r') as f:
            return sum(1 for _ in f)

    def _read_new_lines(self, file_path: Path) -> tuple[list[str], int]:
        """Read only new lines from the file.
        
        Returns:
            Tuple of (new lines list, total line count)
        """
        total_lines = self._count_lines(file_path)
        start_line = self._processed_lines.get(str(file_path), 0)
        
        if start_line >= total_lines:
            return [], total_lines
            
        new_lines = []
        with open(file_path, 'r') as f:
            # Skip already processed lines
            for _ in range(start_line):
                next(f)
            # Read new lines
            for line in f:
                new_lines.append(line)
        
        return new_lines, total_lines

    def _handle_file_created(self, event: FileCreatedEvent) -> None:
        """Handle file creation event.
        
        Creates new table in Keboola Storage and performs initial load.
        """
        if not self._add_to_processing(event.src_path):
            return
        
        try:
            file_path = Path(event.src_path)
            
            # Only process .csv files
            if file_path.suffix.lower() != '.csv':
                self.logger.debug(
                    f"Ignoring non-CSV file: {file_path}",
                    extra={'path': str(file_path)}
                )
                return
            
            # Use selected bucket
            bucket_id = self.selected_bucket_id
            table_id = file_path.stem
            
            # Ensure bucket exists before proceeding
            try:
                if not self.storage.bucket_exists(bucket_id):
                    self.logger.error(
                        'Selected bucket does not exist',
                        extra={
                            'bucket_id': bucket_id
                        }
                    )
                    return
            except StorageError as e:
                self.logger.error(
                    'Failed to verify bucket',
                    extra={
                        'bucket_id': bucket_id,
                        'error': str(e)
                    }
                )
                return

            if not table_id:
                self.logger.error(
                    'Invalid file name',
                    extra={
                        'path': str(file_path),
                        'table_id': table_id
                    }
                )
                return
            
            self.logger.info(
                'CSV file created',
                extra={
                    'event': 'file_created',
                    'path': str(file_path),
                    'size': format_bytes(file_path.stat().st_size),
                    'bucket_id': bucket_id,
                    'table_id': table_id
                }
            )
            
            # Wait for file to be ready
            if not self._is_file_ready(file_path):
                self.logger.debug(
                    'File not ready for processing, will retry on modification',
                    extra={'path': str(file_path)}
                )
                return
            
            # Create table and perform initial load
            self.storage.create_table(
                bucket_id=bucket_id,
                table_id=table_id,
                file_path=file_path
            )
            
            # Record the number of lines processed
            self._processed_lines[str(file_path)] = self._count_lines(file_path)
            
            self.logger.info(
                'Table created and data loaded',
                extra={
                    'bucket_id': bucket_id,
                    'table_id': table_id,
                    'file': str(file_path),
                    'lines_processed': self._processed_lines[str(file_path)]
                }
            )
            
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
            self._remove_from_processing(event.src_path)

    def _handle_file_modified(self, event: FileModifiedEvent) -> None:
        """Handle file modification event.
        
        Validates header consistency and performs full load to existing table.
        """
        if not self._add_to_processing(event.src_path):
            return
            
        try:
            file_path = Path(event.src_path)
            
            # Only process .csv files
            if file_path.suffix.lower() != '.csv':
                self.logger.debug(
                    f"Ignoring non-CSV file: {file_path}",
                    extra={'path': str(file_path)}
                )
                return
            
            # Use selected bucket
            bucket_id = self.selected_bucket_id
            table_id = file_path.stem
            
            # Ensure bucket exists before proceeding
            try:
                if not self.storage.bucket_exists(bucket_id):
                    self.logger.error(
                        'Selected bucket does not exist',
                        extra={
                            'bucket_id': bucket_id
                        }
                    )
                    return
            except StorageError as e:
                self.logger.error(
                    'Failed to verify bucket',
                    extra={
                        'bucket_id': bucket_id,
                        'error': str(e)
                    }
                )
                return

            if not table_id:
                self.logger.error(
                    'Invalid file name',
                    extra={
                        'path': str(file_path),
                        'table_id': table_id
                    }
                )
                return
            
            # Wait for file to be ready
            if not self._is_file_ready(file_path):
                self.logger.debug(
                    'File not ready for processing, will retry later',
                    extra={'path': str(file_path)}
                )
                return
            
            # Read only new lines
            new_lines, total_lines = self._read_new_lines(file_path)
            
            if not new_lines:
                self.logger.debug(
                    'No new lines to process',
                    extra={'path': str(file_path)}
                )
                return
            
            # Create temporary file with new lines
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_f:
                # Write header (first line from original file)
                with open(file_path, 'r') as orig_f:
                    header = next(orig_f)
                    temp_f.write(header)
                
                # Write new lines
                for line in new_lines:
                    temp_f.write(line)
                
                temp_path = Path(temp_f.name)
            
            try:
                # Verify table exists
                if not self.storage.table_exists(bucket_id, table_id):
                    self.logger.warning(
                        'Table does not exist, creating new table',
                        extra={'bucket_id': bucket_id, 'table_id': table_id}
                    )
                    self._handle_file_created(event)
                    return
                
                # Perform incremental load
                self.storage.load_table(
                    bucket_id=bucket_id,
                    table_id=table_id,
                    file_path=temp_path,
                    is_incremental=True  # This needs to be added to the StorageClient class
                )
                
                # Update processed lines count
                self._processed_lines[str(file_path)] = total_lines
                
                self.logger.info(
                    'Table data updated incrementally',
                    extra={
                        'bucket_id': bucket_id,
                        'table_id': table_id,
                        'file': str(file_path),
                        'new_lines': len(new_lines),
                        'total_lines': total_lines
                    }
                )
                
            finally:
                # Clean up temporary file
                try:
                    os.remove(temp_path)
                except Exception as e:
                    self.logger.warning(
                        'Failed to remove temporary file',
                        extra={'file': str(temp_path), 'error': str(e)}
                    )
            
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
            self._remove_from_processing(event.src_path)

class DirectoryWatcher:
    """Watches a directory for changes and processes events."""
    
    def __init__(
        self,
        path: str,
        storage_client: StorageClient,
        logger: Optional[logging.Logger] = None,
        compression_threshold_mb: float = 50.0
    ):
        """Initialize the directory watcher.
        
        Args:
            path: Directory path to watch
            storage_client: Keboola Storage client instance
            logger: Optional logger instance
            compression_threshold_mb: File size threshold for compression in MB
        """
        self.path = Path(path)
        self.logger = logger or logging.getLogger(__name__)
        self.observer = Observer()
        self.event_handler = StorageEventHandler(
            storage_client=storage_client,
            logger=logger,
            compression_threshold_mb=compression_threshold_mb
        )
        
    def start(self):
        """Start watching the directory."""
        self.observer.schedule(self.event_handler, str(self.path), recursive=False)
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
