"""Main entry point for the Keboola Storage Daemon."""

import signal
import sys
from pathlib import Path

from .config import Config
from .utils import setup_logging
from .watcher import DirectoryWatcher

def signal_handler(signum, frame):
    """Handle termination signals gracefully."""
    print('\nReceived signal to terminate. Shutting down...')
    sys.exit(0)

def main():
    """Main entry point for the daemon."""
    # Load configuration
    try:
        config = Config()
    except Exception as e:
        print(f"Failed to load configuration: {e}")
        sys.exit(1)

    # Set up logging
    try:
        logger = setup_logging(
            log_file=config['log_file'],
            log_level=config['log_level'],
            log_dir=config.get('log_dir')
        )
    except Exception as e:
        print(f"Failed to set up logging: {e}")
        sys.exit(1)

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create and start the watcher
    try:
        watcher = DirectoryWatcher(config['watched_directory'], logger)
        watcher.start()
        
        logger.info(
            'Daemon started',
            extra={
                'event': 'daemon_started',
                'config': {
                    'watched_directory': config['watched_directory'],
                    'log_file': config['log_file'],
                    'log_level': config['log_level']
                }
            }
        )
        
        # Keep the main thread alive
        while True:
            signal.pause()
            
    except KeyboardInterrupt:
        logger.info('Received keyboard interrupt')
    except Exception as e:
        logger.error(f'Unexpected error: {e}')
    finally:
        watcher.stop()
        logger.info('Daemon stopped')

if __name__ == '__main__':
    main()
