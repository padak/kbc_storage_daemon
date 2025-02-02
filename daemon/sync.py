"""Sync functionality shared between CLI and daemon."""

from pathlib import Path
from typing import Dict
from .storage_client import StorageClient

def sync_file(
    file_path: str,
    mapping: Dict,
    storage_client: StorageClient,
    logger=None
) -> None:
    """Sync a file to Keboola Storage.
    
    Args:
        file_path: Path to the file
        mapping: Mapping configuration
        storage_client: Storage client instance
        logger: Optional logger for output
    """
    bucket_id = mapping['bucket_id']
    table_id = mapping['table_id']
    sync_mode = mapping['sync_mode']
    options = mapping.get('options', {})
    
    path = Path(file_path)
    if not path.exists():
        msg = f"Warning: File not found: {file_path}"
        if logger:
            logger.warning(msg)
        else:
            print(msg)
        return
        
    msg = f"Syncing {file_path} -> {bucket_id}.{table_id}"
    if logger:
        logger.info(msg)
    else:
        print(f"\n{msg}")
        print(f"Mode: {sync_mode}")
    
    try:
        # Check if table exists by trying to get its details
        storage_client.get_table(bucket_id, table_id)
        table_exists = True
    except:
        table_exists = False
    
    try:
        if not table_exists:
            msg = f"Creating table {bucket_id}.{table_id}"
            if logger:
                logger.info(msg)
            else:
                print(msg)
                
            storage_client.create_table(
                bucket_id=bucket_id,
                table_id=table_id,
                file_path=path,
                primary_key=options.get('primary_key')
            )
        else:
            # Load data into existing table
            is_incremental = sync_mode == 'incremental'
            storage_client.load_table(
                bucket_id=bucket_id,
                table_id=table_id,
                file_path=file_path,
                is_incremental=is_incremental
            )
        
        msg = "Sync completed successfully"
        if logger:
            logger.info(msg)
        else:
            print(msg)
            
    except Exception as e:
        msg = f"Error during sync: {str(e)}"
        if logger:
            logger.error(msg)
        else:
            print(msg)
        raise 