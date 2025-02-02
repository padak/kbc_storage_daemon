"""Main entry point for the Keboola Storage Daemon."""

import os
import signal
import sys
import threading
from pathlib import Path
from typing import Optional

from .config import Config, ConfigurationError
from .utils import setup_logging
from .storage_client import StorageClient, StorageError
from .watcher import DirectoryWatcher

class DaemonContext:
    """Context manager for graceful daemon startup and shutdown."""
    
    def __init__(self, handle_signals: bool = False):
        self.config: Optional[Config] = None
        self.logger = None
        self.storage = None
        self.watcher = None
        self._shutdown_event = threading.Event()
        self._original_sigint = None
        self._original_sigterm = None
        self._handle_signals = handle_signals

    def _validate_watched_directory(self) -> None:
        """Validate and prepare watched directory."""
        watched_dir = Path(self.config['watched_directory'])
        
        # Check if directory exists
        if not watched_dir.exists():
            try:
                watched_dir.mkdir(parents=True)
                self.logger.info(
                    'Created watched directory',
                    extra={'path': str(watched_dir)}
                )
            except Exception as e:
                raise ConfigurationError(
                    f"Cannot create watched directory: {watched_dir}\n{str(e)}"
                )
        
        # Check if directory is writable
        if not os.access(watched_dir, os.W_OK):
            raise ConfigurationError(
                f"Watched directory is not writable: {watched_dir}"
            )

    def _validate_log_directory(self) -> None:
        """Validate and prepare log directory."""
        log_dir = self.config.get('log_dir')
        if log_dir:
            log_path = Path(log_dir)
            if not log_path.exists():
                try:
                    log_path.mkdir(parents=True)
                    self.logger.info(
                        'Created log directory',
                        extra={'path': str(log_path)}
                    )
                except Exception as e:
                    raise ConfigurationError(
                        f"Cannot create log directory: {log_path}\n{str(e)}"
                    )

    def _signal_handler(self, signum: int, frame) -> None:
        """Handle termination signals."""
        signal_name = 'SIGTERM' if signum == signal.SIGTERM else 'SIGINT'
        if self.logger:
            self.logger.info(f'Received {signal_name} signal')
        self._shutdown_event.set()

    def __enter__(self):
        """Initialize daemon components."""
        try:
            # Load configuration
            self.config = Config()
            
            # Set up logging
            self.logger = setup_logging(
                log_file=self.config['log_file'],
                log_level=self.config['log_level'],
                log_dir=self.config.get('log_dir')
            )
            
            # Validate directories
            self._validate_log_directory()
            self._validate_watched_directory()
            
            # Initialize storage client
            self.storage = StorageClient(
                api_token=self.config['keboola_api_token'],
                stack_url=self.config['keboola_stack_url'],
                logger=self.logger,
                max_retries=self.config.get('max_retries', 3),
                initial_retry_delay=self.config.get('initial_retry_delay', 1.0),
                max_retry_delay=self.config.get('max_retry_delay', 30.0),
                retry_backoff=self.config.get('retry_backoff', 2.0)
            )
            
            # Set up signal handlers if requested
            if self._handle_signals:
                self._original_sigint = signal.getsignal(signal.SIGINT)
                self._original_sigterm = signal.getsignal(signal.SIGTERM)
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
            
            # Create and start the watcher
            self.watcher = DirectoryWatcher(
                path=self.config['watched_directory'],
                storage_client=self.storage,
                logger=self.logger,
                compression_threshold_mb=self.config.get('compression_threshold_mb', 50)
            )
            self.watcher.start()
            
            self.logger.info(
                'Daemon started',
                extra={
                    'event': 'daemon_started',
                    'config': {
                        'watched_directory': self.config['watched_directory'],
                        'log_file': self.config['log_file'],
                        'log_level': self.config['log_level'],
                        'compression_threshold_mb': self.config.get('compression_threshold_mb', 50)
                    }
                }
            )
            
            return self
            
        except Exception as e:
            error_msg = f"Failed to initialize daemon: {e}"
            if self.logger:
                self.logger.error(error_msg)
            else:
                print(error_msg, file=sys.stderr)
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up daemon components."""
        try:
            # Stop the watcher
            if self.watcher:
                self.watcher.stop()
            
            # Restore original signal handlers
            if self._original_sigint:
                signal.signal(signal.SIGINT, self._original_sigint)
            if self._original_sigterm:
                signal.signal(signal.SIGTERM, self._original_sigterm)
            
            if self.logger:
                self.logger.info('Daemon stopped')
                
        except Exception as e:
            if self.logger:
                self.logger.error(f'Error during shutdown: {e}')
            else:
                print(f"Error during shutdown: {e}", file=sys.stderr)

    def wait_for_shutdown(self):
        """Wait for shutdown signal."""
        try:
            while not self._shutdown_event.is_set():
                self._shutdown_event.wait(1.0)  # Wake up every second to check
        except KeyboardInterrupt:
            self.logger.info('Received keyboard interrupt')
        except Exception as e:
            self.logger.error(f'Unexpected error: {e}')

def main():
    """Main entry point for the daemon."""
    try:
        with DaemonContext() as daemon:
            daemon.wait_for_shutdown()
    except (ConfigurationError, StorageError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
