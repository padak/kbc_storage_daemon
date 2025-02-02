"""Daemon process that monitors files and syncs them to Keboola Storage."""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent

from .config import FileMapping, ConfigurationError
from .storage_client import StorageClient
from .sync import sync_file

class FileHandler(FileSystemEventHandler):
    """Handle file system events for monitored files."""
    
    def __init__(
        self,
        mappings: List[Dict],
        storage_client: StorageClient,
        logger: Optional[logging.Logger] = None
    ):
        """Initialize file handler.
        
        Args:
            mappings: List of file mappings
            storage_client: Storage client instance
            logger: Optional logger instance
        """
        self.mappings = mappings
        self.storage_client = storage_client
        self.logger = logger or logging.getLogger(__name__)
        
        # Track last sync times to prevent duplicate events
        self.last_syncs = {}
        
    def on_modified(self, event):
        """Handle file modification events.
        
        Args:
            event: File system event
        """
        if not isinstance(event, FileModifiedEvent):
            return
            
        file_path = event.src_path
        
        # Find matching mappings
        for mapping in self.mappings:
            if not mapping.get('enabled', True):
                continue
                
            if Path(mapping['file_path']) == Path(file_path):
                self._handle_file_change(mapping, file_path)
                
    def _handle_file_change(self, mapping: Dict, file_path: str) -> None:
        """Handle a file change event.
        
        Args:
            mapping: File mapping configuration
            file_path: Path to changed file
        """
        # Debounce - prevent duplicate syncs
        now = time.time()
        last_sync = self.last_syncs.get(file_path, 0)
        if now - last_sync < 1.0:  # 1 second debounce
            return
            
        self.last_syncs[file_path] = now
        
        try:
            sync_file(file_path, mapping, self.storage_client, self.logger)
        except Exception as e:
            self.logger.error(f"Error syncing {file_path}: {e}")

class Daemon:
    """Daemon process that monitors files and syncs them to Keboola Storage."""
    
    def __init__(
        self,
        config_file: str = 'config.json',
        logger: Optional[logging.Logger] = None
    ):
        """Initialize daemon.
        
        Args:
            config_file: Path to configuration file
            logger: Optional logger instance
        """
        self.config_file = config_file
        self.logger = logger or logging.getLogger(__name__)
        self.observer = None
        self.config = None
        self.storage_client = None
        
    def _load_config(self) -> None:
        """Load configuration from file."""
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            raise ConfigurationError(f"Failed to load config: {e}")
            
    def _init_storage_client(self) -> None:
        """Initialize storage client from environment variables."""
        api_token = os.getenv('KEBOOLA_API_TOKEN')
        stack_url = os.getenv('KEBOOLA_STACK_URL')
        if not api_token or not stack_url:
            raise ConfigurationError(
                "KEBOOLA_API_TOKEN and KEBOOLA_STACK_URL must be set in .env"
            )
        self.storage_client = StorageClient(
            api_token=api_token,
            stack_url=stack_url
        )
        
    def start(self) -> None:
        """Start the daemon process."""
        try:
            # Load config and initialize storage client
            self._load_config()
            self._init_storage_client()
            
            if not self.storage_client:
                raise ConfigurationError(
                    "Storage client not initialized. Check your credentials."
                )
            
            # Get watched directory
            watched_dir = self.config.get('default_settings', {}).get('watched_directory')
            if not watched_dir:
                raise ConfigurationError("watched_directory not set in config")
                
            # Create and start file system observer
            handler = FileHandler(
                mappings=self.config.get('mappings', []),
                storage_client=self.storage_client,
                logger=self.logger
            )
            
            self.observer = Observer()
            self.observer.schedule(handler, watched_dir, recursive=False)
            self.observer.start()
            
            self.logger.info(f"Started watching directory: {watched_dir}")
            
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.stop()
                
        except Exception as e:
            self.logger.error(f"Daemon error: {e}")
            self.stop()
            raise
            
    def stop(self) -> None:
        """Stop the daemon process."""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.logger.info("Stopped watching for changes") 