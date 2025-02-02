"""Directory monitoring for the Keboola Storage Daemon.

This module handles filesystem events using watchdog and processes
directory/file changes accordingly.
"""

import os
import threading
from pathlib import Path
from typing import Dict, Optional
import logging
from watchdog.observers import Observer
from watchdog.events import (
    FileSystemEventHandler,
    FileCreatedEvent,
    FileModifiedEvent,
    DirCreatedEvent,
    FileSystemEvent
)

from .storage_client import StorageClient, StorageError
from .sync_handlers import (
    SyncHandler,
    FullLoadHandler,
    IncrementalHandler,
    StreamingHandler
)
from .config import SyncMode, FileMapping

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
        self.compression_threshold = compression_threshold_mb * 1024 * 1024
        self._processing = set()  # Track files being processed
        self._processing_lock = threading.Lock()  # Lock for thread safety
        
        # Initialize sync handlers
        self._handlers = {
            SyncMode.FULL_LOAD: FullLoadHandler(
                storage_client,
                logger,
                compression_threshold_mb
            ),
            SyncMode.INCREMENTAL: IncrementalHandler(
                storage_client,
                logger,
                compression_threshold_mb
            ),
            SyncMode.STREAMING: StreamingHandler(
                storage_client,
                logger,
                compression_threshold_mb
            )
        }

    def __del__(self):
        """Clean up handlers on destruction."""
        for handler in self._handlers.values():
            handler.cleanup()

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

    def _get_handler(self, mapping: FileMapping) -> SyncHandler:
        """Get the appropriate sync handler for a mapping.
        
        Args:
            mapping: File mapping configuration
            
        Returns:
            Sync handler instance
            
        Raises:
            ConfigurationError: If sync mode is invalid
        """
        handler = self._handlers.get(mapping.sync_mode)
        if not handler:
            raise ConfigurationError(f"Invalid sync mode: {mapping.sync_mode}")
        return handler

    def _process_file_event(
        self,
        event: FileSystemEvent,
        mapping: FileMapping,
        is_creation: bool = False
    ) -> None:
        """Process a file event with the appropriate handler.
        
        Args:
            event: File system event
            mapping: File mapping configuration
            is_creation: Whether this is a file creation event
        """
        file_path = Path(event.src_path)
        
        # Wait for file to be ready
        if not self._is_file_ready(file_path):
            self.logger.debug(
                'File not ready for processing, will retry on modification',
                extra={'path': str(file_path)}
            )
            return
        
        try:
            handler = self._get_handler(mapping)
            
            if is_creation:
                handler.handle_created(
                    file_path,
                    mapping.bucket_id,
                    mapping.table_id,
                    mapping.options
                )
            else:
                handler.handle_modified(
                    file_path,
                    mapping.bucket_id,
                    mapping.table_id,
                    mapping.options
                )
                
        except Exception as e:
            self.logger.error(
                'Failed to process file event',
                extra={
                    'path': str(file_path),
                    'error': str(e),
                    'event_type': 'created' if is_creation else 'modified'
                }
            )
            raise
        finally:
            self._remove_from_processing(event.src_path)

    def on_created(self, event: FileSystemEvent) -> None:
        """Handle creation events.
        
        Args:
            event: File system event
        """
        if event.is_directory:
            return
            
        if not self._add_to_processing(event.src_path):
            return
            
        try:
            file_path = Path(event.src_path)
            mapping = self.storage.get_mapping_for_file(str(file_path))
            
            if not mapping or not mapping.enabled:
                self.logger.debug(
                    'Ignoring file with no mapping or disabled mapping',
                    extra={'path': str(file_path)}
                )
                return
                
            self._process_file_event(event, mapping, is_creation=True)
            
        except Exception as e:
            self.logger.error(
                'Error handling creation event',
                extra={
                    'path': event.src_path,
                    'error': str(e)
                }
            )
            raise
        finally:
            self._remove_from_processing(event.src_path)

    def on_modified(self, event: FileSystemEvent) -> None:
        """Handle modification events.
        
        Args:
            event: File system event
        """
        if event.is_directory:
            return
            
        if not self._add_to_processing(event.src_path):
            return
            
        try:
            file_path = Path(event.src_path)
            mapping = self.storage.get_mapping_for_file(str(file_path))
            
            if not mapping or not mapping.enabled:
                self.logger.debug(
                    'Ignoring file with no mapping or disabled mapping',
                    extra={'path': str(file_path)}
                )
                return
                
            self._process_file_event(event, mapping, is_creation=False)
            
        except Exception as e:
            self.logger.error(
                'Error handling modification event',
                extra={
                    'path': event.src_path,
                    'error': str(e)
                }
            )
            raise
        finally:
            self._remove_from_processing(event.src_path)

class DirectoryWatcher:
    """Watches a directory for changes and processes them."""
    
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
        self.path = path
        self.event_handler = StorageEventHandler(
            storage_client,
            logger,
            compression_threshold_mb
        )
        self.observer = Observer()
        self.observer.schedule(self.event_handler, path, recursive=True)

    def start(self):
        """Start watching the directory."""
        self.observer.start()

    def stop(self):
        """Stop watching the directory."""
        self.observer.stop()
        self.observer.join()
