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
        
        try:
            # Initialize client according to official SDK docs
            self._client = Client(stack_url, api_token)
            # Test connection by listing buckets
            self._buckets
            self.logger.info("Successfully connected to Keboola Storage API")
        except Exception as e:
            raise StorageError(f"Failed to connect to Keboola Storage API: {e}")
    
    @property
    def _buckets(self):
        """Get list of buckets."""
        try:
            return self._client.buckets.list()
        except Exception as e:
            raise StorageError(f"Failed to list buckets: {e}")
    
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
        file_path: Path,
        is_incremental: bool = False
    ) -> Dict:
        """Load data into an existing table."""
        try:
            if not file_path.exists():
                raise StorageError(f"File not found: {file_path}")
            
            self.logger.info(
                f"Loading data into table {table_id} in bucket {bucket_id}"
            )
            
            return self._client.tables.load(
                table_id=f"{bucket_id}.{table_id}",
                file_path=str(file_path),
                is_incremental=is_incremental
            )
        except Exception as e:
            raise StorageError(f"Failed to load table data: {e}")
    
    def get_table(self, bucket_id: str, table_id: str) -> Dict:
        """Get table details."""
        try:
            return self._client.tables.detail(f"{bucket_id}.{table_id}")
        except Exception as e:
            raise StorageError(f"Failed to get table details: {e}")
