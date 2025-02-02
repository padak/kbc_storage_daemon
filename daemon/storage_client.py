"""Keboola Storage client wrapper for the daemon.

This module handles all interactions with Keboola Storage API using the
official Python SDK.
"""

import csv
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from kbcstorage.client import Client
from kbcstorage.tables import Tables

from .utils import with_retries

class StorageError(Exception):
    """Base exception for storage operations."""
    pass

class StorageClient:
    """Wrapper for Keboola Storage API client."""
    
    def __init__(
        self,
        api_token: str,
        stack_url: str,
        logger: Optional[logging.Logger] = None,
        max_retries: int = 3,
        initial_retry_delay: float = 1.0,
        max_retry_delay: float = 30.0,
        retry_backoff: float = 2.0
    ):
        """Initialize the storage client.
        
        Args:
            api_token: Keboola Storage API token
            stack_url: Keboola Stack URL
            logger: Optional logger instance
            max_retries: Maximum number of retry attempts
            initial_retry_delay: Initial delay between retries in seconds
            max_retry_delay: Maximum delay between retries in seconds
            retry_backoff: Multiplier for exponential backoff
        """
        self.logger = logger or logging.getLogger(__name__)
        self._max_retries = max_retries
        self._initial_retry_delay = initial_retry_delay
        self._max_retry_delay = max_retry_delay
        self._retry_backoff = retry_backoff
        self._api_token = api_token
        self._stack_url = stack_url
        self._client = None
        self._buckets_cache = None
        
        # Initialize connection
        self._connect()
    
    def _connect(self) -> None:
        """Initialize or reinitialize the connection."""
        try:
            # Initialize client according to official SDK docs
            self._client = Client(self._stack_url, self._api_token)
            # Test connection but don't cache buckets yet
            self._client.buckets.list()
            self.logger.info("Successfully connected to Keboola Storage API")
        except Exception as e:
            self._client = None
            self._buckets_cache = None
            raise StorageError(f"Failed to connect to Keboola Storage API: {e}")

    def _ensure_connected(self) -> None:
        """Ensure we have a valid connection."""
        if not self._client:
            self._connect()

    @property
    def _buckets(self) -> List[Dict]:
        """Get list of buckets with caching."""
        if self._buckets_cache is None:
            self._ensure_connected()
            # Get ALL buckets without filtering
            self._buckets_cache = self._client.buckets.list()
        return self._buckets_cache
    
    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._buckets_cache = None

    def reconnect(self) -> None:
        """Force reconnection to the API."""
        self._client = None
        self._buckets_cache = None
        self._connect()
    
    def bucket_exists(self, bucket_id: str) -> bool:
        """Check if a bucket exists."""
        try:
            return any(b['id'] == bucket_id for b in self._buckets)
        except Exception as e:
            raise StorageError(f"Failed to check bucket existence: {e}")
    
    def create_bucket(
        self,
        bucket_id: str,
        bucket_name: str,
        stage: str = 'in'
    ) -> Dict:
        """Create a new bucket if it doesn't exist."""
        try:
            if not self.bucket_exists(bucket_id):
                self.logger.info(f"Creating bucket: {bucket_id}")
                return self._client.buckets.create(
                    name=bucket_name,
                    stage=stage,
                    description=f"Created by Storage Daemon"
                )
            return self.get_bucket(bucket_id)
        except Exception as e:
            raise StorageError(f"Failed to create bucket: {e}")
    
    def get_bucket(self, bucket_id: str) -> Dict:
        """Get bucket details."""
        try:
            return self._client.buckets.detail(bucket_id)
        except Exception as e:
            raise StorageError(f"Failed to get bucket details: {e}")
    
    def table_exists(self, bucket_id: str, table_id: str) -> bool:
        """Check if a table exists in the bucket."""
        try:
            tables = self._client.tables.list()
            return any(
                t['id'] == f"{bucket_id}.{table_id}"
                for t in tables
            )
        except Exception as e:
            raise StorageError(f"Failed to check table existence: {e}")
    
    def create_table(
        self,
        bucket_id: str,
        table_id: str,
        file_path: Path,
        primary_key: Optional[List[str]] = None
    ) -> Dict:
        """Create a new table from a CSV file."""
        try:
            if not file_path.exists():
                raise StorageError(f"File not found: {file_path}")
            
            self.logger.info(
                f"Creating table {table_id} in bucket {bucket_id}"
            )
            
            return self._client.tables.create(
                name=table_id,
                bucket_id=bucket_id,
                file_path=str(file_path),
                primary_key=primary_key
            )
        except Exception as e:
            raise StorageError(f"Failed to create table: {e}")
    
    def load_table(
        self,
        bucket_id: str,
        table_id: str,
        file_path: Union[str, Path],
        is_incremental: bool = False
    ) -> None:
        """Load data into an existing table.
        
        Args:
            bucket_id: Bucket ID
            table_id: Table ID
            file_path: Path to the CSV file
            is_incremental: Whether to perform incremental load (append) or full load (replace)
        """
        try:
            # Get table client using both token and root_url from existing client
            tables = Tables(token=self._client.token, root_url=self._client.root_url)
            
            # Load data - pass the file path string directly
            tables.load(
                table_id=f"{bucket_id}.{table_id}",
                file_path=str(file_path),  # Convert Path to string
                is_incremental=is_incremental
            )
                
            self.logger.info(
                f"{'Incrementally updated' if is_incremental else 'Loaded'} table data",
                extra={
                    'bucket_id': bucket_id,
                    'table_id': table_id,
                    'file': str(file_path),
                    'mode': 'incremental' if is_incremental else 'full'
                }
            )
            
        except Exception as e:
            raise StorageError(
                f"Failed to {'incrementally update' if is_incremental else 'load'} "
                f"table {bucket_id}.{table_id}: {e}"
            )
    
    def get_table(self, bucket_id: str, table_id: str) -> Dict:
        """Get table details."""
        try:
            return self._client.tables.detail(f"{bucket_id}.{table_id}")
        except Exception as e:
            raise StorageError(f"Failed to get table details: {e}")

    def list_buckets(self) -> List[Dict]:
        """List all buckets.
        
        Returns:
            List of bucket dictionaries
        """
        try:
            self._ensure_connected()
            # Get fresh list of ALL buckets without any filtering
            buckets = self._client.buckets.list()
            self.logger.debug(f"Retrieved {len(buckets)} buckets from Storage API")
            return buckets
        except Exception as e:
            raise StorageError(f"Failed to list buckets: {e}")

    def create_bucket(self, bucket_id: str, stage: str, description: str = None) -> Dict:
        """Create a new bucket.
        
        Args:
            bucket_id: Bucket ID (e.g. 'in.c-sales')
            stage: Bucket stage ('in' or 'out')
            description: Optional bucket description
            
        Returns:
            Created bucket dictionary
        """
        try:
            return self._client.buckets.create(
                name=bucket_id.split('.')[-1],
                stage=stage,
                description=description
            )
        except Exception as e:
            raise StorageError(f"Failed to create bucket: {e}")

    def list_tables(self, bucket_id: str) -> List[Dict]:
        """List all tables in a bucket.
        
        Args:
            bucket_id: Bucket ID
            
        Returns:
            List of table dictionaries
        """
        try:
            return self._client.tables.list(bucket_id=bucket_id)
        except Exception as e:
            raise StorageError(f"Failed to list tables: {e}")

    def create_table(
        self,
        bucket_id: str,
        table_id: str,
        file_path: Union[str, Path],
        primary_key: List[str] = None
    ) -> Dict:
        """Create a new table from a CSV file.
        
        Args:
            bucket_id: Bucket ID
            table_id: Table ID
            file_path: Path to CSV file
            primary_key: Optional list of primary key columns
            
        Returns:
            Created table dictionary
        """
        try:
            return self._client.tables.create(
                name=table_id,
                bucket_id=bucket_id,
                file_path=str(file_path),
                primary_key=primary_key or []
            )
        except Exception as e:
            raise StorageError(f"Failed to create table: {e}")

    def load_table(
        self,
        bucket_id: str,
        table_id: str,
        file_path: Union[str, Path],
        is_incremental: bool = False
    ) -> None:
        """Load data into a table.
        
        Args:
            bucket_id: Bucket ID
            table_id: Table ID
            file_path: Path to CSV file
            is_incremental: Whether to append data (True) or replace (False)
        """
        try:
            self._client.tables.load(
                table_id=f"{bucket_id}.{table_id}",
                file_path=str(file_path),
                is_incremental=is_incremental
            )
        except Exception as e:
            raise StorageError(f"Failed to load table data: {e}")

    def table_exists(self, bucket_id: str, table_id: str) -> bool:
        """Check if a table exists.
        
        Args:
            bucket_id: Bucket ID
            table_id: Table ID
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            tables = self.list_tables(bucket_id)
            return any(t['id'] == f"{bucket_id}.{table_id}" for t in tables)
        except Exception:
            return False
