# CLI Documentation

## Overview
The Keboola Storage Daemon CLI provides commands to manage file mappings and synchronization with Keboola Storage.

## Commands

### Initialize Configuration
```bash
./scripts/kbc-daemon init --watched-directory <path>
```
Initialize the daemon configuration with a watched directory.

### List Mappings
```bash
./scripts/kbc-daemon list
```
List all configured file mappings.

### Add Mapping
```bash
# Interactive mode (recommended)
./scripts/kbc-daemon add -i

# Non-interactive mode
./scripts/kbc-daemon add \
  --file-path <path> \
  --bucket-id <bucket> \
  --table-id <table> \
  --sync-mode <mode> \
  [--disabled] \
  [--options '{"key": "value"}']
```

Interactive mode features:
- Smart CSV file analysis
- Column detection and listing
- Primary key suggestions
- Easy bucket/table selection

### Edit Mapping
```bash
./scripts/kbc-daemon edit <index> \
  [--file-path <path>] \
  [--bucket-id <bucket>] \
  [--table-id <table>] \
  [--sync-mode <mode>] \
  [--enable | --disable] \
  [--options '{"key": "value"}']
```
Edit an existing mapping by its index (1-based).

### Delete Mapping
```bash
./scripts/kbc-daemon delete <index>
```
Delete a mapping by its index (1-based).

### Sync Files
```bash
# Sync all enabled mappings
./scripts/kbc-daemon sync

# Sync specific mapping
./scripts/kbc-daemon sync <index>
```
Manually trigger synchronization of files to Keboola Storage.

### Reload Configuration
```bash
./scripts/kbc-daemon reload
```
Reload configuration and credentials from `.env` file.

### Start Daemon
```bash
./scripts/kbc-daemon start
```
Start the daemon process to monitor files for changes.

### Update Settings
```bash
./scripts/kbc-daemon settings \
  [--log-level <level>] \
  [--log-file <file>] \
  [--log-dir <dir>] \
  [--compression-threshold-mb <size>] \
  [--max-retries <count>] \
  [--initial-retry-delay <seconds>] \
  [--max-retry-delay <seconds>] \
  [--retry-backoff <factor>]
```
Update daemon settings.

## Configuration

### Environment Variables (.env)
```
KEBOOLA_STACK_URL=https://connection.keboola.com
KEBOOLA_API_TOKEN=your-token
```

### Config File (config.json)
```json
{
  "mappings": [
    {
      "file_path": "/path/to/file",
      "bucket_id": "in.c-bucket",
      "table_id": "my_table",
      "sync_mode": "full_load|incremental|streaming",
      "enabled": true,
      "options": {
        "primary_key": ["id"],
        "streaming_endpoint": "https://...",
        "batch_size": 1000
      }
    }
  ],
  "default_settings": {
    "watched_directory": "/path/to/watch",
    "log_level": "INFO",
    "log_file": "daemon.log",
    "log_dir": "logs",
    "compression_threshold_mb": 50,
    "max_retries": 3,
    "initial_retry_delay": 1.0,
    "max_retry_delay": 30.0,
    "retry_backoff": 2.0
  }
} 