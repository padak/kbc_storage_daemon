"""Directory monitoring for the Keboola Storage Daemon.

This module handles filesystem events using watchdog and processes
directory/file changes accordingly.
"""

import os
import csv
import gzip
import tempfile
from pathlib import Path
from typing import Optional, Tuple
import logging
from watchdog.observers import Observer
from watchdog.events import (
    FileSystemEventHandler,
    FileCreatedEvent,
    FileModifiedEvent,
    DirCreatedEvent
)

from .utils import (
    format_bytes,
    get_file_encoding,
    sanitize_bucket_name,
    compress_file,
    get_compressed_reader
)
from .storage_client import StorageClient, StorageError

class StorageEventHandler(FileSystemEventHandler):
    """Handles filesystem events and processes them for Keboola Storage."""

    def __init__(
        self,
        storage_client: StorageClient,
        logger: logging.Logger,
        compression_threshold_mb: int = 50
    ):
        """Initialize the event handler.
        
        Args:
            storage_client: Keboola Storage client instance
            logger: Logger instance for event logging
            compression_threshold_mb: Size threshold for compression in MB
        """
        self.storage = storage_client
        self.logger = logger
        self.compression_threshold_mb = compression_threshold_mb
        self._processing = set()  # Track files being processed to avoid duplicates
        self._temp_files = set()  # Track temporary compressed files

    def __del__(self):
        """Clean up temporary files on destruction."""
        for temp_file in self._temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception as e:
                self.logger.warning(
                    f'Failed to remove temporary file: {temp_file}',
                    extra={'error': str(e)}
                )

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
        
        try:
            self.storage.create_bucket(bucket_name)
            self.logger.info(
                'Bucket created for directory',
                extra={
                    'directory': str(dir_path),
                    'bucket': bucket_name
                }
            )
        except StorageError as e:
            self.logger.error(
                'Failed to create bucket for directory',
                extra={
                    'directory': str(dir_path),
                    'bucket': bucket_name,
                    'error': str(e)
                }
            )

    def _handle_file_created(self, event: FileCreatedEvent):
        """Process new CSV file creation.
        
        Creates new table in Keboola Storage and performs initial load.
        """
        if event.src_path in self._processing:
            return
        
        self._processing.add(event.src_path)
        try:
            file_path = Path(event.src_path)
            bucket_name = sanitize_bucket_name(file_path.parent.name)
            table_name = sanitize_bucket_name(file_path.stem)
            
            self.logger.info(
                'CSV file created',
                extra={
                    'event': 'file_created',
                    'path': str(file_path),
                    'size': format_bytes(file_path.stat().st_size),
                    'bucket': bucket_name,
                    'table': table_name
                }
            )
            
            # Validate CSV and get its properties
            dialect, header = self._analyze_csv(file_path)
            if not dialect or not header:
                return
            
            # Prepare file for upload (compress if needed)
            upload_path, is_compressed = self._prepare_file_for_upload(file_path)
            
            try:
                # Create table and perform initial load
                self.storage.create_table(
                    bucket_name=bucket_name,
                    table_name=table_name,
                    file_path=upload_path,
                    is_compressed=is_compressed,
                    delimiter=dialect.delimiter,
                    enclosure=dialect.quotechar
                )
                
                self.logger.info(
                    'Table created and data loaded',
                    extra={
                        'bucket': bucket_name,
                        'table': table_name,
                        'file': str(file_path)
                    }
                )
                
            finally:
                # Clean up temporary file if it was created
                if is_compressed and upload_path != file_path:
                    try:
                        os.remove(upload_path)
                        self._temp_files.remove(str(upload_path))
                    except Exception as e:
                        self.logger.warning(
                            'Failed to remove temporary file',
                            extra={
                                'file': str(upload_path),
                                'error': str(e)
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
            bucket_name = sanitize_bucket_name(file_path.parent.name)
            table_name = sanitize_bucket_name(file_path.stem)
            
            self.logger.info(
                'CSV file modified',
                extra={
                    'event': 'file_modified',
                    'path': str(file_path),
                    'size': format_bytes(file_path.stat().st_size),
                    'bucket': bucket_name,
                    'table': table_name
                }
            )
            
            # Validate CSV and check header consistency
            dialect, header = self._analyze_csv(file_path)
            if not dialect or not header:
                return
            
            # Verify table exists
            if not self.storage.table_exists(bucket_name, table_name):
                self.logger.error(
                    'Table not found for modified file',
                    extra={
                        'bucket': bucket_name,
                        'table': table_name,
                        'file': str(file_path)
                    }
                )
                return
            
            # Get existing table to check header
            table = self.storage.get_table(bucket_name, table_name)
            if not self._verify_header_consistency(header, table):
                return
            
            # Prepare file for upload (compress if needed)
            upload_path, is_compressed = self._prepare_file_for_upload(file_path)
            
            try:
                # Perform full load
                self.storage.load_table(
                    bucket_name=bucket_name,
                    table_name=table_name,
                    file_path=upload_path,
                    is_compressed=is_compressed,
                    delimiter=dialect.delimiter,
                    enclosure=dialect.quotechar
                )
                
                self.logger.info(
                    'Table data updated',
                    extra={
                        'bucket': bucket_name,
                        'table': table_name,
                        'file': str(file_path)
                    }
                )
                
            finally:
                # Clean up temporary file if it was created
                if is_compressed and upload_path != file_path:
                    try:
                        os.remove(upload_path)
                        self._temp_files.remove(str(upload_path))
                    except Exception as e:
                        self.logger.warning(
                            'Failed to remove temporary file',
                            extra={
                                'file': str(upload_path),
                                'error': str(e)
                            }
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
            self._processing.remove(event.src_path)

    def _verify_header_consistency(
        self,
        new_header: list[str],
        table: dict
    ) -> bool:
        """Verify that the new CSV header matches the existing table.
        
        Args:
            new_header: Header from the new CSV file
            table: Table details from Keboola Storage
            
        Returns:
            True if headers match, False otherwise
        """
        try:
            existing_columns = [col['name'] for col in table.get('columns', [])]
            
            if not existing_columns:
                self.logger.error(
                    'No columns found in existing table',
                    extra={
                        'table_id': table.get('id'),
                        'new_header': new_header
                    }
                )
                return False
            
            if new_header != existing_columns:
                self.logger.error(
                    'CSV header does not match existing table',
                    extra={
                        'table_id': table.get('id'),
                        'existing_columns': existing_columns,
                        'new_header': new_header
                    }
                )
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(
                'Error verifying header consistency',
                extra={
                    'table_id': table.get('id'),
                    'new_header': new_header,
                    'error': str(e)
                }
            )
            return False

    def _is_csv_file(self, path: str) -> bool:
        """Check if the file is a CSV file."""
        return path.lower().endswith('.csv')

    def _prepare_file_for_upload(
        self,
        file_path: Path
    ) -> Tuple[Path, bool]:
        """Prepare file for upload, compressing if necessary.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Tuple of (path_to_use, is_compressed)
        """
        try:
            compressed_path = compress_file(
                file_path,
                threshold_mb=self.compression_threshold_mb,
                logger=self.logger
            )
            
            if compressed_path:
                self._temp_files.add(str(compressed_path))
                return compressed_path, True
                
            return file_path, False
            
        except Exception as e:
            self.logger.error(
                'Error preparing file for upload',
                extra={
                    'path': str(file_path),
                    'error': str(e)
                }
            )
            raise

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
            with get_compressed_reader(file_path) as f:
                sample = f.read(sample_size).decode(get_file_encoding(str(file_path)))
            
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
            with get_compressed_reader(file_path) as f:
                # Skip BOM if present
                sample = f.read(3)
                if sample.startswith(b'\xef\xbb\xbf'):
                    header_line = sample[3:] + f.readline()
                else:
                    header_line = sample + f.readline()
                
                header = next(csv.reader([header_line.decode(get_file_encoding(str(file_path)))], dialect=dialect))
            
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
    
    def __init__(
        self,
        path: str,
        storage_client: StorageClient,
        logger: logging.Logger,
        compression_threshold_mb: int = 50
    ):
        """Initialize the directory watcher.
        
        Args:
            path: Directory path to watch
            storage_client: Keboola Storage client instance
            logger: Logger instance for event logging
            compression_threshold_mb: Size threshold for compression in MB
        """
        self.path = Path(path)
        self.logger = logger
        self.observer = Observer()
        self.event_handler = StorageEventHandler(
            storage_client=storage_client,
            logger=logger,
            compression_threshold_mb=compression_threshold_mb
        )
        
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
