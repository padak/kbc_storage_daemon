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
        logger: logging.Logger,
        max_retries: int = 3,
        initial_retry_delay: float = 1.0,
        max_retry_delay: float = 30.0,
        retry_backoff: float = 2.0
    ):
        """Initialize the storage client.
        
        Args:
            api_token: Keboola Storage API token
            stack_url: Keboola Stack endpoint URL
            logger: Logger instance for operation logging
            max_retries: Maximum number of retry attempts
            initial_retry_delay: Initial delay between retries in seconds
            max_retry_delay: Maximum delay between retries in seconds
            retry_backoff: Multiplier for exponential backoff
        """
        self.logger = logger
        self.retry_decorator = with_retries(
            max_attempts=max_retries,
            initial_delay=initial_retry_delay,
            max_delay=max_retry_delay,
            backoff_factor=retry_backoff,
            logger=logger
        )
        
        try:
            self.client = Client(api_token, stack_url)
            self.tables = Tables(api_token, stack_url)
            
            # Verify connection
            self._verify_connection()
            self.logger.info('Storage client initialized successfully')
            
        except Exception as e:
            self.logger.error(
                'Failed to initialize storage client',
                extra={'error': str(e)}
            )
            raise StorageError(f"Storage client initialization failed: {e}")

    @property
    def _buckets(self):
        """Get buckets API client with retries."""
        return self.retry_decorator(self.client.buckets)()

    def _verify_connection(self):
        """Verify API token and connection."""
        self.retry_decorator(self.client.verify_token)()

    @with_retries(logger=logger)  # Use instance logger
    def bucket_exists(self, bucket_name: str, stage: str = 'in') -> bool:
        """Check if a bucket exists.
        
        Args:
            bucket_name: Name of the bucket
            stage: Storage stage ('in' or 'out')
            
        Returns:
            True if bucket exists, False otherwise
        """
        try:
            bucket_id = f"{stage}.c-{bucket_name}"
            buckets = self._buckets.list()
            return any(b['id'] == bucket_id for b in buckets)
        except Exception as e:
            self.logger.error(
                'Error checking bucket existence',
                extra={
                    'bucket': bucket_name,
                    'stage': stage,
                    'error': str(e)
                }
            )
            raise StorageError(f"Failed to check bucket existence: {e}")

    @with_retries(logger=logger)  # Use instance logger
    def create_bucket(
        self,
        bucket_name: str,
        stage: str = 'in',
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a new bucket if it doesn't exist.
        
        Args:
            bucket_name: Name of the bucket
            stage: Storage stage ('in' or 'out')
            description: Optional bucket description
            
        Returns:
            Bucket details from the API
        """
        try:
            if self.bucket_exists(bucket_name, stage):
                self.logger.info(
                    'Bucket already exists',
                    extra={
                        'bucket': bucket_name,
                        'stage': stage
                    }
                )
                return self.get_bucket(bucket_name, stage)
            
            bucket = self._buckets.create(
                name=bucket_name,
                stage=stage,
                description=description
            )
            
            self.logger.info(
                'Bucket created successfully',
                extra={
                    'bucket': bucket_name,
                    'stage': stage,
                    'bucket_id': bucket['id']
                }
            )
            
            return bucket
            
        except Exception as e:
            self.logger.error(
                'Error creating bucket',
                extra={
                    'bucket': bucket_name,
                    'stage': stage,
                    'error': str(e)
                }
            )
            raise StorageError(f"Failed to create bucket: {e}")

    @with_retries(logger=logger)  # Use instance logger
    def get_bucket(self, bucket_name: str, stage: str = 'in') -> Dict[str, Any]:
        """Get bucket details.
        
        Args:
            bucket_name: Name of the bucket
            stage: Storage stage ('in' or 'out')
            
        Returns:
            Bucket details from the API
        """
        try:
            bucket_id = f"{stage}.c-{bucket_name}"
            return self._buckets.detail(bucket_id)
        except Exception as e:
            self.logger.error(
                'Error getting bucket details',
                extra={
                    'bucket': bucket_name,
                    'stage': stage,
                    'error': str(e)
                }
            )
            raise StorageError(f"Failed to get bucket details: {e}")

    @with_retries(logger=logger)  # Use instance logger
    def table_exists(
        self,
        bucket_name: str,
        table_name: str,
        stage: str = 'in'
    ) -> bool:
        """Check if a table exists in the bucket.
        
        Args:
            bucket_name: Name of the bucket
            table_name: Name of the table
            stage: Storage stage ('in' or 'out')
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            bucket_id = f"{stage}.c-{bucket_name}"
            tables = self.retry_decorator(self.tables.list)(bucket_id=bucket_id)
            return any(t['id'] == f"{bucket_id}.{table_name}" for t in tables)
        except Exception as e:
            self.logger.error(
                'Error checking table existence',
                extra={
                    'bucket': bucket_name,
                    'table': table_name,
                    'stage': stage,
                    'error': str(e)
                }
            )
            raise StorageError(f"Failed to check table existence: {e}")

    @with_retries(logger=logger)  # Use instance logger
    def create_table(
        self,
        bucket_name: str,
        table_name: str,
        file_path: Union[str, Path],
        primary_key: Optional[List[str]] = None,
        is_compressed: bool = False,
        delimiter: str = ',',
        enclosure: str = '"',
        stage: str = 'in'
    ) -> Dict[str, Any]:
        """Create a new table from a CSV file.
        
        Args:
            bucket_name: Name of the bucket
            table_name: Name of the table
            file_path: Path to the CSV file
            primary_key: Optional list of primary key columns
            is_compressed: Whether the file is gzip compressed
            delimiter: CSV delimiter character
            enclosure: CSV enclosure character
            stage: Storage stage ('in' or 'out')
            
        Returns:
            Table details from the API
        """
        try:
            bucket_id = f"{stage}.c-{bucket_name}"
            file_path = str(file_path)
            
            # Create table definition
            if primary_key is None:
                primary_key = []
            
            table = self.retry_decorator(self.tables.create)(
                name=table_name,
                bucket_id=bucket_id,
                file_path=file_path,
                primary_key=primary_key,
                delimiter=delimiter,
                enclosure=enclosure,
                is_compressed=is_compressed
            )
            
            self.logger.info(
                'Table created successfully',
                extra={
                    'bucket': bucket_name,
                    'table': table_name,
                    'table_id': table['id']
                }
            )
            
            return table
            
        except Exception as e:
            self.logger.error(
                'Error creating table',
                extra={
                    'bucket': bucket_name,
                    'table': table_name,
                    'file': file_path,
                    'error': str(e)
                }
            )
            raise StorageError(f"Failed to create table: {e}")

    @with_retries(logger=logger)  # Use instance logger
    def load_table(
        self,
        bucket_name: str,
        table_name: str,
        file_path: Union[str, Path],
        is_compressed: bool = False,
        delimiter: str = ',',
        enclosure: str = '"',
        stage: str = 'in'
    ) -> Dict[str, Any]:
        """Load data into an existing table (full load).
        
        Args:
            bucket_name: Name of the bucket
            table_name: Name of the table
            file_path: Path to the CSV file
            is_compressed: Whether the file is gzip compressed
            delimiter: CSV delimiter character
            enclosure: CSV enclosure character
            stage: Storage stage ('in' or 'out')
            
        Returns:
            Import details from the API
        """
        try:
            table_id = f"{stage}.c-{bucket_name}.{table_name}"
            file_path = str(file_path)
            
            result = self.retry_decorator(self.tables.load)(
                table_id=table_id,
                file_path=file_path,
                is_incremental=False,
                delimiter=delimiter,
                enclosure=enclosure,
                is_compressed=is_compressed
            )
            
            self.logger.info(
                'Table loaded successfully',
                extra={
                    'bucket': bucket_name,
                    'table': table_name,
                    'import_id': result.get('id')
                }
            )
            
            return result
            
        except Exception as e:
            self.logger.error(
                'Error loading table',
                extra={
                    'bucket': bucket_name,
                    'table': table_name,
                    'file': file_path,
                    'error': str(e)
                }
            )
            raise StorageError(f"Failed to load table: {e}")

    @with_retries(logger=logger)  # Use instance logger
    def get_table(
        self,
        bucket_name: str,
        table_name: str,
        stage: str = 'in'
    ) -> Dict[str, Any]:
        """Get table details.
        
        Args:
            bucket_name: Name of the bucket
            table_name: Name of the table
            stage: Storage stage ('in' or 'out')
            
        Returns:
            Table details from the API
        """
        try:
            table_id = f"{stage}.c-{bucket_name}.{table_name}"
            return self.retry_decorator(self.tables.detail)(table_id)
        except Exception as e:
            self.logger.error(
                'Error getting table details',
                extra={
                    'bucket': bucket_name,
                    'table': table_name,
                    'stage': stage,
                    'error': str(e)
                }
            )
            raise StorageError(f"Failed to get table details: {e}")
