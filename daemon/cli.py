"""Command-line interface for managing the Keboola Storage Daemon configuration."""

import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import csv

from dotenv import load_dotenv
from .config import Config, FileMapping, SyncMode, ConfigurationError
from .storage_client import StorageClient
from .daemon import Daemon
from .sync import sync_file

def _handle_interrupt(message: str = "Operation cancelled by user") -> None:
    """Handle keyboard interrupt gracefully.
    
    Args:
        message: Optional message to display
    """
    print(f"\n{message}")
    exit(0)

class CLI:
    """Command-line interface for the daemon."""
    
    def __init__(self, config_file: str = 'config.json'):
        """Initialize CLI with configuration file.
        
        Args:
            config_file: Path to configuration file
        """
        self.config_file = config_file
        self._load_config()
        self._init_storage_client()

    def _init_storage_client(self) -> None:
        """Initialize storage client from environment variables."""
        self.storage_client = None
        if os.getenv('KEBOOLA_API_TOKEN') and os.getenv('KEBOOLA_STACK_URL'):
            self.storage_client = StorageClient(
                api_token=os.getenv('KEBOOLA_API_TOKEN'),
                stack_url=os.getenv('KEBOOLA_STACK_URL')
            )

    def reload(self) -> None:
        """Reload configuration and environment variables."""
        # Clear existing env vars
        for key in ['KEBOOLA_API_TOKEN', 'KEBOOLA_STACK_URL']:
            if key in os.environ:
                del os.environ[key]
        
        # Force clear any cached environment variables
        os.environ.clear()
        
        # Reload .env file with override
        load_dotenv(override=True)
        
        # Print current values for verification
        print("\nCurrent configuration:")
        print(f"Stack URL: {os.getenv('KEBOOLA_STACK_URL')}")
        print(f"API Token: {'*' * 8}{os.getenv('KEBOOLA_API_TOKEN')[-4:] if os.getenv('KEBOOLA_API_TOKEN') else 'Not set'}")
        
        # Force recreation of storage client
        if self.storage_client:
            try:
                self.storage_client.reconnect()
            except:
                # If reconnect fails, create new instance
                self.storage_client = None
                self._init_storage_client()
        
        try:
            # Test connection with new credentials
            client = self._get_storage_client()
            buckets = client.list_buckets()
            print(f"\nConnection successful! Found {len(buckets)} buckets in project.")
        except Exception as e:
            print(f"\nWarning: Failed to connect with new credentials: {e}")
        
        # Reload config file
        self._load_config()
        print("\nReloaded configuration and credentials")

    def _load_config(self) -> None:
        """Load configuration from file."""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
            else:
                self.config = {
                    'mappings': [],
                    'default_settings': {}
                }
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid config file: {e}")

    def _save_config(self) -> None:
        """Save configuration to file."""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=4)

    def _get_storage_client(self) -> StorageClient:
        """Get or create storage client.
        
        Returns:
            StorageClient instance
            
        Raises:
            ConfigurationError: If API credentials are not set
        """
        if not self.storage_client:
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
        return self.storage_client

    def _select_bucket(self) -> str:
        """Interactive bucket selection.
        
        Returns:
            Selected bucket ID
        """
        try:
            client = self._get_storage_client()
            
            # Get all buckets in stage 'in'
            buckets = []
            try:
                # List all buckets in stage 'in'
                buckets = [
                    bucket for bucket in client.list_buckets()
                    if bucket.get('stage') == 'in'
                ]
            except Exception as e:
                raise ConfigurationError(f"Failed to list buckets: {e}")

            if not buckets:
                # If no buckets exist, offer to create one
                print("\nNo input buckets found.")
                bucket_name = input("Enter new bucket name (without 'in.c-' prefix): ").strip()
                if not bucket_name:
                    raise ConfigurationError("Bucket name is required")
                bucket_id = f"in.c-{bucket_name}"
                try:
                    client.create_bucket(bucket_id, 'in', f"Created by KBC daemon")
                    print(f"Created bucket {bucket_id}")
                    return bucket_id
                except Exception as e:
                    raise ConfigurationError(f"Failed to create bucket: {e}")
            
            print("\nAvailable buckets:")
            print("-" * 80)
            for i, bucket in enumerate(buckets, 1):
                print(f"{i}. {bucket['id']} ({bucket.get('description', 'No description')})")
            print(f"{len(buckets) + 1}. [Create new bucket]")
            
            while True:
                try:
                    choice = input("\nSelect bucket (number or ID): ").strip()
                    if choice.isdigit():
                        index = int(choice) - 1
                        if 0 <= index < len(buckets):
                            return buckets[index]['id']
                        elif index == len(buckets):
                            bucket_name = input("Enter new bucket name (without 'in.c-' prefix): ").strip()
                            if not bucket_name:
                                raise ConfigurationError("Bucket name is required")
                            bucket_id = f"in.c-{bucket_name}"
                            try:
                                client.create_bucket(bucket_id, 'in', f"Created by KBC daemon")
                                print(f"Created bucket {bucket_id}")
                                return bucket_id
                            except Exception as e:
                                raise ConfigurationError(f"Failed to create bucket: {e}")
                    else:
                        if any(b['id'] == choice for b in buckets):
                            return choice
                        # Try to create bucket if it looks like a valid ID
                        if choice.startswith('in.c-'):
                            try:
                                client.create_bucket(choice, 'in', f"Created by KBC daemon")
                                print(f"Created bucket {choice}")
                                return choice
                            except Exception as e:
                                print(f"Failed to create bucket: {e}")
                    print("Invalid selection. Please try again.")
                except (ValueError, IndexError):
                    print("Invalid selection. Please try again.")
        except KeyboardInterrupt:
            _handle_interrupt()

    def _select_table(self, bucket_id: str) -> str:
        """Interactive table selection.
        
        Args:
            bucket_id: Bucket ID to list tables from
            
        Returns:
            Selected table ID
        """
        try:
            client = self._get_storage_client()
            
            # Get all tables in the bucket
            tables = []
            try:
                tables = client.list_tables(bucket_id)
            except Exception as e:
                # If bucket doesn't exist or is empty, that's fine
                pass

            print(f"\nAvailable tables in {bucket_id}:")
            print("-" * 80)
            for i, table in enumerate(tables, 1):
                print(f"{i}. {table['id']} ({table.get('name', 'No name')})")
            print(f"{len(tables) + 1}. [Create new table]")
            
            while True:
                try:
                    choice = input("\nSelect table (number or ID): ").strip()
                    if choice.isdigit():
                        index = int(choice) - 1
                        if 0 <= index < len(tables):
                            return tables[index]['id']
                        elif index == len(tables):
                            name = input("Enter new table name: ").strip()
                            if not name:
                                raise ConfigurationError("Table name is required")
                            return name
                    else:
                        if any(t['id'] == choice for t in tables):
                            return choice
                        # Allow any valid table name for new tables
                        if choice:
                            return choice
                    print("Invalid selection. Please try again.")
                except (ValueError, IndexError):
                    print("Invalid selection. Please try again.")
        except KeyboardInterrupt:
            _handle_interrupt()

    def _select_sync_mode(self) -> str:
        """Interactive sync mode selection.
        
        Returns:
            Selected sync mode
        """
        try:
            modes = {
                '1': SyncMode.FULL_LOAD,
                '2': SyncMode.INCREMENTAL,
                '3': SyncMode.STREAMING
            }
            
            print("\nAvailable sync modes:")
            print("-" * 80)
            print("1. Full Load - Replace entire table contents")
            print("2. Incremental - Append new data only")
            print("3. Streaming - Real-time updates (text files)")
            
            while True:
                choice = input("\nSelect sync mode (1-3): ").strip()
                if choice in modes:
                    return modes[choice]
                print("Invalid selection. Please try again.")
        except KeyboardInterrupt:
            _handle_interrupt()

    def _resolve_file_path(self, file_path: str) -> Path:
        """Resolve file path relative to current directory or watched directory.
        
        Args:
            file_path: Input file path
            
        Returns:
            Resolved Path object
            
        Raises:
            ConfigurationError: If file not found
        """
        # Try as absolute path first
        path = Path(file_path)
        if path.is_absolute() and path.exists():
            return path
        
        # Try relative to current directory
        path = Path.cwd() / file_path
        if path.exists():
            return path
        
        # Try relative to watched directory
        watched_dir = self.config.get('default_settings', {}).get('watched_directory')
        if watched_dir:
            path = Path(watched_dir) / file_path
            if path.exists():
                return path
            
        raise ConfigurationError(
            f"File not found: {file_path}\n"
            f"Searched in:\n"
            f"- Current directory: {Path.cwd()}\n"
            f"- Watched directory: {watched_dir or 'Not set'}"
        )

    def _analyze_csv_file(self, file_path: str) -> Tuple[List[str], Optional[List[str]]]:
        """Analyze CSV file to get headers and suggest primary key columns.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Tuple of (headers, suggested_primary_key)
        """
        try:
            # Resolve file path
            path = self._resolve_file_path(file_path)
            print(f"\nAnalyzing file: {path}")
            
            with open(path, 'r') as f:
                # Try to read first few lines to analyze
                reader = csv.reader(f)
                headers = next(reader)  # Get headers
                
                # Try to read first row to check for unique identifiers
                try:
                    first_row = next(reader)
                    # Look for columns that might be IDs (contain 'id' or are unique)
                    potential_keys = [
                        header for header in headers
                        if 'id' in header.lower() or 
                        'key' in header.lower() or
                        'code' in header.lower()
                    ]
                    return headers, potential_keys
                except StopIteration:
                    # File only has headers
                    return headers, None
                
        except Exception as e:
            print(f"\nWarning: Could not analyze file {file_path}: {e}")
            return [], None

    def add_mapping_interactive(self) -> None:
        """Add a new mapping interactively."""
        try:
            print("\nAdding new file mapping")
            print("=" * 80)
            
            # Show watched directory
            watched_dir = self.config.get('default_settings', {}).get('watched_directory')
            if watched_dir:
                print(f"Watched directory: {watched_dir}")
            
            # Get file path
            file_path = input("Enter file path to monitor: ").strip()
            if not file_path:
                raise ConfigurationError("File path is required")
            
            # Analyze CSV file
            headers, suggested_keys = self._analyze_csv_file(file_path)
            if headers:
                print("\nDetected columns:")
                print("-" * 80)
                for i, header in enumerate(headers, 1):
                    print(f"{i}. {header}")
            
            # Select bucket and table
            bucket_id = self._select_bucket()
            table_id = self._select_table(bucket_id)
            
            # Select sync mode with guidance
            if suggested_keys:
                print("\nNote: Found potential key columns:", ", ".join(suggested_keys))
                print("Incremental load recommended for data with unique identifiers.")
            
            sync_mode = self._select_sync_mode()
            
            # Configure options with smart defaults
            options = self._configure_options(sync_mode, headers, suggested_keys)
            
            # Add mapping
            self.add_mapping(
                file_path=str(self._resolve_file_path(file_path)),  # Use resolved path
                bucket_id=bucket_id,
                table_id=table_id,
                sync_mode=sync_mode,
                **options
            )
        except KeyboardInterrupt:
            _handle_interrupt()

    def init(self, watched_directory: str) -> None:
        """Initialize configuration with default settings.
        
        Args:
            watched_directory: Directory to watch for changes
        """
        self.config = {
            'mappings': [],
            'default_settings': {
                'watched_directory': watched_directory,
                'log_level': 'INFO',
                'log_file': 'daemon.log',
                'log_dir': 'logs',
                'compression_threshold_mb': 50,
                'max_retries': 3,
                'initial_retry_delay': 1.0,
                'max_retry_delay': 30.0,
                'retry_backoff': 2.0
            }
        }
        self._save_config()
        print(f"Initialized configuration in {self.config_file}")

    def list_mappings(self) -> None:
        """List all file mappings."""
        mappings = self.config.get('mappings', [])
        if not mappings:
            print("No mappings configured.")
            return

        print("\nFile Mappings:")
        print("-" * 80)
        for i, mapping in enumerate(mappings, 1):
            status = "enabled" if mapping.get('enabled', True) else "disabled"
            print(f"{i}. {mapping['file_path']} -> {mapping['bucket_id']}.{mapping['table_id']}")
            print(f"   Mode: {mapping['sync_mode']}, Status: {status}")
            if mapping.get('options'):
                print(f"   Options: {mapping['options']}")
            print()

    def add_mapping(
        self,
        file_path: str,
        bucket_id: str,
        table_id: str,
        sync_mode: str,
        enabled: bool = True,
        **options
    ) -> None:
        """Add a new file mapping.
        
        Args:
            file_path: Path to the file to monitor
            bucket_id: Target bucket ID
            table_id: Target table ID
            sync_mode: Sync mode (full_load, incremental, streaming)
            enabled: Whether the mapping is enabled
            **options: Additional options for the mapping
        """
        # Validate sync mode
        if not SyncMode.is_valid(sync_mode):
            valid_modes = [SyncMode.FULL_LOAD, SyncMode.INCREMENTAL, SyncMode.STREAMING]
            raise ConfigurationError(
                f"Invalid sync mode: {sync_mode}. Must be one of: {', '.join(valid_modes)}"
            )

        # Create new mapping
        mapping = {
            'file_path': file_path,
            'bucket_id': bucket_id,
            'table_id': table_id,
            'sync_mode': sync_mode,
            'enabled': enabled,
            'options': options
        }

        # Validate mapping
        FileMapping(mapping)  # This will raise ConfigurationError if invalid

        # Add to config
        self.config.setdefault('mappings', []).append(mapping)
        self._save_config()
        
        print(f"Added mapping for {file_path}")

    def edit_mapping(
        self,
        index: int,
        **kwargs
    ) -> None:
        """Edit an existing mapping.
        
        Args:
            index: 1-based index of mapping to edit
            **kwargs: Fields to update
        """
        if not self.config.get('mappings'):
            raise ConfigurationError("No mappings exist")

        try:
            mapping = self.config['mappings'][index - 1]
        except IndexError:
            raise ConfigurationError(f"Invalid mapping index: {index}")

        # Update fields
        mapping.update(kwargs)

        # Validate updated mapping
        FileMapping(mapping)  # This will raise ConfigurationError if invalid

        self._save_config()
        print(f"Updated mapping {index}")

    def delete_mapping(self, index: int) -> None:
        """Delete a mapping.
        
        Args:
            index: 1-based index of mapping to delete
        """
        if not self.config.get('mappings'):
            raise ConfigurationError("No mappings exist")

        try:
            mapping = self.config['mappings'].pop(index - 1)
        except IndexError:
            raise ConfigurationError(f"Invalid mapping index: {index}")

        self._save_config()
        print(f"Deleted mapping for {mapping['file_path']}")

    def update_settings(self, **kwargs) -> None:
        """Update default settings.
        
        Args:
            **kwargs: Settings to update
        """
        self.config.setdefault('default_settings', {}).update(kwargs)
        self._save_config()
        print("Updated default settings")

    def _configure_options(self, sync_mode: str, headers: List[str] = None, suggested_keys: List[str] = None) -> Dict:
        """Interactive option configuration.
        
        Args:
            sync_mode: Selected sync mode
            headers: CSV file headers if available
            suggested_keys: Suggested primary key columns
            
        Returns:
            Option dictionary
        """
        try:
            options = {}
            
            if sync_mode == SyncMode.INCREMENTAL:
                print("\nConfigure incremental load options:")
                
                if suggested_keys:
                    print(f"Suggested primary key(s): {', '.join(suggested_keys)}")
                    use_suggested = input("Use suggested primary key(s)? [Y/n]: ").strip().lower()
                    if not use_suggested or use_suggested == 'y':
                        options['primary_key'] = suggested_keys
                        return options
                
                if headers:
                    print("\nAvailable columns:")
                    for i, header in enumerate(headers, 1):
                        print(f"{i}. {header}")
                    
                    while True:
                        primary_key = input("\nEnter column numbers or names for primary key (comma-separated): ").strip()
                        if not primary_key:
                            break
                            
                        # Parse input - accept both numbers and column names
                        try:
                            keys = []
                            for key in primary_key.split(','):
                                key = key.strip()
                                if key.isdigit() and 1 <= int(key) <= len(headers):
                                    keys.append(headers[int(key) - 1])
                                elif key in headers:
                                    keys.append(key)
                                else:
                                    print(f"Invalid column: {key}")
                                    keys = []
                                    break
                            if keys:
                                options['primary_key'] = keys
                                break
                        except (ValueError, IndexError):
                            print("Invalid input. Please try again.")
                
                if not options.get('primary_key'):
                    primary_key = input("\nEnter primary key columns (comma-separated): ").strip()
                    if primary_key:
                        options['primary_key'] = [col.strip() for col in primary_key.split(',')]
            
            elif sync_mode == SyncMode.STREAMING:
                print("\nConfigure streaming options:")
                batch_size = input("Enter batch size [1000]: ").strip() or "1000"
                endpoint = input("Enter streaming endpoint URL: ").strip()
                
                options['batch_size'] = int(batch_size)
                if endpoint:
                    options['streaming_endpoint'] = endpoint
            
            return options
        except KeyboardInterrupt:
            _handle_interrupt()

    def sync_mapping(self, index: Optional[int] = None) -> None:
        """Manually sync one or all mappings.
        
        Args:
            index: Optional 1-based index of mapping to sync. If None, sync all.
        """
        mappings = self.config.get('mappings', [])
        if not mappings:
            print("No mappings configured.")
            return

        if index is not None:
            try:
                mapping = mappings[index - 1]
                self._sync_single_mapping(mapping)
            except IndexError:
                raise ConfigurationError(f"Invalid mapping index: {index}")
        else:
            print("\nSyncing all mappings:")
            print("-" * 80)
            for mapping in mappings:
                if mapping.get('enabled', True):
                    self._sync_single_mapping(mapping)

    def _sync_single_mapping(self, mapping: Dict) -> None:
        """Sync a single mapping.
        
        Args:
            mapping: Mapping configuration
        """
        try:
            client = self._get_storage_client()
            sync_file(mapping['file_path'], mapping, client)
        except Exception as e:
            print(f"Error syncing {mapping['file_path']}: {e}")

    def start_daemon(self) -> None:
        """Start the daemon process."""
        daemon = Daemon(self.config_file)
        daemon.start()

def main():
    """Main entry point for CLI."""
    try:
        parser = argparse.ArgumentParser(
            description="Manage Keboola Storage Daemon configuration"
        )
        subparsers = parser.add_subparsers(dest='command', help='Command to execute')

        # Init command
        init_parser = subparsers.add_parser('init', help='Initialize configuration')
        init_parser.add_argument(
            '--watched-directory',
            required=True,
            help='Directory to watch for changes'
        )

        # List command
        subparsers.add_parser('list', help='List all mappings')

        # Reload command
        subparsers.add_parser('reload', help='Reload configuration and credentials')

        # Add mapping command
        add_parser = subparsers.add_parser('add', help='Add a new mapping')
        add_parser.add_argument(
            '-i', '--interactive',
            action='store_true',
            help='Use interactive mode'
        )
        add_parser.add_argument('--file-path', help='File path to monitor')
        add_parser.add_argument('--bucket-id', help='Target bucket ID')
        add_parser.add_argument('--table-id', help='Target table ID')
        add_parser.add_argument(
            '--sync-mode',
            choices=[SyncMode.FULL_LOAD, SyncMode.INCREMENTAL, SyncMode.STREAMING],
            help='Sync mode'
        )
        add_parser.add_argument(
            '--disabled',
            action='store_true',
            help='Disable the mapping'
        )
        add_parser.add_argument(
            '--options',
            type=json.loads,
            default={},
            help='Additional options as JSON'
        )

        # Edit mapping command
        edit_parser = subparsers.add_parser('edit', help='Edit a mapping')
        edit_parser.add_argument('index', type=int, help='Mapping index (1-based)')
        edit_parser.add_argument('--file-path', help='New file path')
        edit_parser.add_argument('--bucket-id', help='New bucket ID')
        edit_parser.add_argument('--table-id', help='New table ID')
        edit_parser.add_argument(
            '--sync-mode',
            choices=[SyncMode.FULL_LOAD, SyncMode.INCREMENTAL, SyncMode.STREAMING],
            help='New sync mode'
        )
        edit_parser.add_argument(
            '--enable',
            action='store_true',
            help='Enable the mapping'
        )
        edit_parser.add_argument(
            '--disable',
            action='store_true',
            help='Disable the mapping'
        )
        edit_parser.add_argument(
            '--options',
            type=json.loads,
            help='New options as JSON'
        )

        # Delete mapping command
        delete_parser = subparsers.add_parser('delete', help='Delete a mapping')
        delete_parser.add_argument('index', type=int, help='Mapping index (1-based)')

        # Settings command
        settings_parser = subparsers.add_parser('settings', help='Update default settings')
        settings_parser.add_argument('--log-level', help='Log level')
        settings_parser.add_argument('--log-file', help='Log file path')
        settings_parser.add_argument('--log-dir', help='Log directory')
        settings_parser.add_argument(
            '--compression-threshold-mb',
            type=float,
            help='Compression threshold in MB'
        )
        settings_parser.add_argument(
            '--max-retries',
            type=int,
            help='Maximum number of retries'
        )
        settings_parser.add_argument(
            '--initial-retry-delay',
            type=float,
            help='Initial retry delay in seconds'
        )
        settings_parser.add_argument(
            '--max-retry-delay',
            type=float,
            help='Maximum retry delay in seconds'
        )
        settings_parser.add_argument(
            '--retry-backoff',
            type=float,
            help='Retry backoff factor'
        )

        # Sync command
        sync_parser = subparsers.add_parser('sync', help='Manually sync file(s) to Keboola')
        sync_parser.add_argument(
            'index',
            nargs='?',
            type=int,
            help='Optional mapping index (1-based) to sync. If not provided, sync all mappings.'
        )

        # Daemon command
        subparsers.add_parser('start', help='Start the daemon process')

        args = parser.parse_args()
        cli = CLI()

        try:
            if args.command == 'init':
                cli.init(args.watched_directory)
            elif args.command == 'list':
                cli.list_mappings()
            elif args.command == 'reload':
                cli.reload()
            elif args.command == 'add':
                if args.interactive:
                    cli.add_mapping_interactive()
                else:
                    if not all([args.file_path, args.bucket_id, args.table_id, args.sync_mode]):
                        parser.error("All parameters required in non-interactive mode")
                    cli.add_mapping(
                        args.file_path,
                        args.bucket_id,
                        args.table_id,
                        args.sync_mode,
                        not args.disabled,
                        **args.options
                    )
            elif args.command == 'edit':
                updates = {}
                if args.file_path:
                    updates['file_path'] = args.file_path
                if args.bucket_id:
                    updates['bucket_id'] = args.bucket_id
                if args.table_id:
                    updates['table_id'] = args.table_id
                if args.sync_mode:
                    updates['sync_mode'] = args.sync_mode
                if args.enable:
                    updates['enabled'] = True
                if args.disable:
                    updates['enabled'] = False
                if args.options:
                    updates['options'] = args.options
                cli.edit_mapping(args.index, **updates)
            elif args.command == 'delete':
                cli.delete_mapping(args.index)
            elif args.command == 'settings':
                updates = {
                    k: v for k, v in vars(args).items()
                    if k not in ('command',) and v is not None
                }
                cli.update_settings(**updates)
            elif args.command == 'sync':
                cli.sync_mapping(args.index)
            elif args.command == 'start':
                cli.start_daemon()
            else:
                parser.print_help()

        except ConfigurationError as e:
            print(f"Error: {e}")
            exit(1)
            
    except KeyboardInterrupt:
        _handle_interrupt()

if __name__ == '__main__':
    main() 