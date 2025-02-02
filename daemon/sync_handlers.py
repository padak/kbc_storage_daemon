"""Sync mode handlers for different file types and sync strategies."""

import os
import csv
import gzip
import json
import logging
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests

from .storage_client import StorageClient, StorageError
from .utils import (
    format_bytes,
    get_file_encoding,
    compress_file,
    get_compressed_reader
)

class SyncHandler(ABC):
    """Base class for sync mode handlers."""
    
    def __init__(
        self,
        storage_client: StorageClient,
        logger: Optional[logging.Logger] = None,
        compression_threshold_mb: float = 50.0
    ):
        """Initialize the sync handler.
        
        Args:
            storage_client: Keboola Storage client instance
            logger: Optional logger instance
            compression_threshold_mb: File size threshold for compression in MB
        """
        self.storage = storage_client
        self.logger = logger or logging.getLogger(__name__)
        self.compression_threshold = compression_threshold_mb * 1024 * 1024
        self._temp_files: Set[str] = set()

    def cleanup(self):
        """Clean up any temporary files."""
        for temp_file in self._temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception as e:
                self.logger.warning(
                    f'Failed to remove temporary file: {temp_file}',
                    extra={'error': str(e)}
                )

    @abstractmethod
    def handle_created(
        self,
        file_path: Path,
        bucket_id: str,
        table_id: str,
        options: Dict
    ) -> None:
        """Handle file creation event."""
        pass

    @abstractmethod
    def handle_modified(
        self,
        file_path: Path,
        bucket_id: str,
        table_id: str,
        options: Dict
    ) -> None:
        """Handle file modification event."""
        pass

class FullLoadHandler(SyncHandler):
    """Handler for full load sync mode."""

    def handle_created(
        self,
        file_path: Path,
        bucket_id: str,
        table_id: str,
        options: Dict
    ) -> None:
        """Handle file creation with full load.
        
        Creates a new table and loads the entire file.
        """
        try:
            self.logger.info(
                'Creating table with full load',
                extra={
                    'path': str(file_path),
                    'bucket_id': bucket_id,
                    'table_id': table_id
                }
            )
            
            self.storage.create_table(
                bucket_id=bucket_id,
                table_id=table_id,
                file_path=file_path,
                primary_key=options.get('primary_key', [])
            )
        except StorageError as e:
            self.logger.error(
                'Failed to create table',
                extra={
                    'path': str(file_path),
                    'bucket_id': bucket_id,
                    'table_id': table_id,
                    'error': str(e)
                }
            )
            raise

    def handle_modified(
        self,
        file_path: Path,
        bucket_id: str,
        table_id: str,
        options: Dict
    ) -> None:
        """Handle file modification with full load.
        
        Replaces the entire table contents with the new file.
        """
        try:
            self.logger.info(
                'Replacing table data with full load',
                extra={
                    'path': str(file_path),
                    'bucket_id': bucket_id,
                    'table_id': table_id
                }
            )
            
            self.storage.load_table(
                bucket_id=bucket_id,
                table_id=table_id,
                file_path=file_path,
                is_incremental=False
            )
        except StorageError as e:
            self.logger.error(
                'Failed to replace table data',
                extra={
                    'path': str(file_path),
                    'bucket_id': bucket_id,
                    'table_id': table_id,
                    'error': str(e)
                }
            )
            raise

class IncrementalHandler(SyncHandler):
    """Handler for incremental sync mode."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._processed_lines: Dict[str, int] = {}

    def _count_lines(self, file_path: Path) -> int:
        """Count number of lines in a file."""
        with open(file_path, 'r') as f:
            return sum(1 for _ in f)

    def _read_new_lines(self, file_path: Path) -> Tuple[List[str], int]:
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

    def handle_created(
        self,
        file_path: Path,
        bucket_id: str,
        table_id: str,
        options: Dict
    ) -> None:
        """Handle file creation with incremental load.
        
        Creates a new table with initial data.
        """
        try:
            self.logger.info(
                'Creating table with incremental load',
                extra={
                    'path': str(file_path),
                    'bucket_id': bucket_id,
                    'table_id': table_id
                }
            )
            
            self.storage.create_table(
                bucket_id=bucket_id,
                table_id=table_id,
                file_path=file_path,
                primary_key=options.get('primary_key', [])
            )
            
            # Record processed lines
            self._processed_lines[str(file_path)] = self._count_lines(file_path)
            
        except StorageError as e:
            self.logger.error(
                'Failed to create table',
                extra={
                    'path': str(file_path),
                    'bucket_id': bucket_id,
                    'table_id': table_id,
                    'error': str(e)
                }
            )
            raise

    def handle_modified(
        self,
        file_path: Path,
        bucket_id: str,
        table_id: str,
        options: Dict
    ) -> None:
        """Handle file modification with incremental load.
        
        Appends only new lines to the table.
        """
        try:
            new_lines, total_lines = self._read_new_lines(file_path)
            
            if not new_lines:
                self.logger.debug(
                    'No new lines to process',
                    extra={'path': str(file_path)}
                )
                return
            
            self.logger.info(
                'Appending new lines to table',
                extra={
                    'path': str(file_path),
                    'bucket_id': bucket_id,
                    'table_id': table_id,
                    'new_lines': len(new_lines)
                }
            )
            
            # Create temporary file with new lines
            with tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.csv',
                delete=False
            ) as temp_file:
                self._temp_files.add(temp_file.name)
                writer = csv.writer(temp_file)
                writer.writerows(new_lines)
            
            # Load new lines incrementally
            self.storage.load_table(
                bucket_id=bucket_id,
                table_id=table_id,
                file_path=Path(temp_file.name),
                is_incremental=True
            )
            
            # Update processed lines count
            self._processed_lines[str(file_path)] = total_lines
            
        except StorageError as e:
            self.logger.error(
                'Failed to append new lines',
                extra={
                    'path': str(file_path),
                    'bucket_id': bucket_id,
                    'table_id': table_id,
                    'error': str(e)
                }
            )
            raise
        finally:
            self.cleanup()

class StreamingHandler(SyncHandler):
    """Handler for streaming sync mode."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._batch_sizes: Dict[str, int] = {}
        self._current_batches: Dict[str, List[str]] = {}

    def _get_batch_size(self, options: Dict) -> int:
        """Get batch size from options with default."""
        return int(options.get('batch_size', 1000))

    def _get_endpoint(self, options: Dict) -> str:
        """Get streaming endpoint from options."""
        endpoint = options.get('streaming_endpoint')
        if not endpoint:
            raise ConfigurationError(
                'streaming_endpoint is required for streaming mode'
            )
        return endpoint

    def _send_batch(
        self,
        endpoint: str,
        batch: List[str],
        file_path: Path
    ) -> None:
        """Send a batch of lines to the streaming endpoint."""
        try:
            response = requests.post(
                endpoint,
                data='\n'.join(batch),
                headers={'Content-Type': 'text/plain'}
            )
            response.raise_for_status()
            
            self.logger.debug(
                'Sent batch to streaming endpoint',
                extra={
                    'path': str(file_path),
                    'batch_size': len(batch)
                }
            )
            
        except requests.exceptions.RequestException as e:
            self.logger.error(
                'Failed to send batch to streaming endpoint',
                extra={
                    'path': str(file_path),
                    'error': str(e)
                }
            )
            raise

    def handle_created(
        self,
        file_path: Path,
        bucket_id: str,
        table_id: str,
        options: Dict
    ) -> None:
        """Handle file creation with streaming.
        
        Initializes streaming for the file.
        """
        file_key = str(file_path)
        self._batch_sizes[file_key] = self._get_batch_size(options)
        self._current_batches[file_key] = []
        
        # Process initial content
        self.handle_modified(file_path, bucket_id, table_id, options)

    def handle_modified(
        self,
        file_path: Path,
        bucket_id: str,
        table_id: str,
        options: Dict
    ) -> None:
        """Handle file modification with streaming.
        
        Streams new content in batches.
        """
        file_key = str(file_path)
        
        try:
            endpoint = self._get_endpoint(options)
            batch_size = self._batch_sizes[file_key]
            current_batch = self._current_batches[file_key]
            
            with open(file_path, 'r') as f:
                for line in f:
                    current_batch.append(line.strip())
                    
                    if len(current_batch) >= batch_size:
                        self._send_batch(endpoint, current_batch, file_path)
                        current_batch = []
            
            # Send any remaining lines
            if current_batch:
                self._send_batch(endpoint, current_batch, file_path)
                current_batch = []
            
            self._current_batches[file_key] = current_batch
            
        except Exception as e:
            self.logger.error(
                'Failed to process streaming update',
                extra={
                    'path': str(file_path),
                    'error': str(e)
                }
            )
            raise 