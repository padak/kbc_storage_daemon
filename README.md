# Keboola Storage Daemon

A daemon that monitors a specific folder and automatically syncs CSV files to Keboola Storage.

## Features

- Monitors a specified directory for changes
- Creates Keboola Storage buckets for new subdirectories
- Automatically uploads CSV files to corresponding tables
- Supports CSV dialect detection
- Handles large files with compression
- Configurable logging

## Requirements

- Python 3.x
- Docker (for containerized deployment)

## Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd kbc-daemon
   ```

2. Create and activate virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure environment:
   ```bash
   cp .env.template .env
   # Edit .env with your settings
   ```

## Configuration

The daemon requires the following configuration in your `.env` file:
- `KEBOOLA_STACK_URL`: Your Keboola Stack endpoint URL
- `KEBOOLA_API_TOKEN`: Your Keboola Storage API token
- `WATCHED_DIRECTORY`: Path to the directory to monitor
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Usage

### Running locally

```bash
python -m daemon.main
```

### Running with Docker

There are two ways to run the daemon using Docker:

1. Using Docker Compose (recommended):
   ```bash
   # Create necessary directories
   mkdir -p watched_directory logs

   # Configure environment
   cp .env.template .env
   # Edit .env with your settings

   # Start the daemon
   docker-compose up -d

   # View logs
   docker-compose logs -f
   ```

2. Using Docker directly:
   ```bash
   # Build the image
   docker build -t kbc-daemon .

   # Create necessary directories
   mkdir -p watched_directory logs

   # Run the container
   docker run -d \
     --name kbc-daemon \
     -v "$(pwd)/watched_directory:/watch" \
     -v "$(pwd)/logs:/logs" \
     --env-file .env \
     --restart unless-stopped \
     kbc-daemon
   ```

The daemon will:
- Monitor the `/watch` directory (mapped to `watched_directory` on your host)
- Store logs in the `/logs` directory (mapped to `logs` on your host)
- Automatically restart if it crashes
- Run as a non-root user for security

## Development

The project structure:
- `daemon/`: Main package directory
  - `main.py`: Entry point
  - `config.py`: Configuration management
  - `watcher.py`: Directory monitoring
  - `storage_client.py`: Keboola Storage operations
  - `utils.py`: Helper functions

## License

[Add your license here]
