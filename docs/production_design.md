# Production Design Document: Keboola Storage Daemon

## 1. Overview

This project implements a daemon that monitors a specific folder on the host machine using an event-based watcher. Its purpose is to automatically manage Keboola Storage buckets and tables based on filesystem events:
- **Folder Creation:** When a new sub-folder is created, the daemon checks for an existing bucket with the sub-folder name in the Keboola Storage (in stage). If the bucket does not exist, it creates one.
- **CSV File Management:** When a new CSV file is added or updated within a sub-folder:
  - For the first load, a new table is created in the corresponding bucket.
  - For subsequent updates, a full load is performed (i.e., the existing table data is replaced).
  - The header of the CSV file is verified against the stored header (from the first load) or checked with the remote table to ensure consistency.
- **Non-CSV files** are ignored.

The daemon is designed to be lightwave, easy to maintain, and very well documented. It leverages the Keboola Python SDK and runs within a Docker container.

## 2. Architecture

- **Programming Language:** Python 3.x
- **File Monitoring:** Python [watchdog](https://python-watchdog.readthedocs.io/) (event-based watcher)
- **Keboola Integration:** [Keboola Storage API](https://keboola.docs.apiary.io/#) via the [Keboola Python SDK](https://developers.keboola.com/integrate/storage/python-client/)
- **Deployment:** Docker container
- **Configuration:** A configurable file (JSON, YAML, or another agreed format) that includes:
  - API token for Keboola Storage
  - Keboola stack endpoint URL
  - Configuration for logging (file location, verbosity)
  - Other operational parameters (e.g., retry limits, watchdog settings, etc.)

## 3. Functional Requirements

### 3.1 Folder Monitoring
- Use an event-based approach (e.g., using the watchdog library) to monitor a designated folder.
- Detect events for:
  - New sub-folder creation.
  - New CSV file creation or updates within sub-folders.

### 3.2 Bucket Management
- **On sub-folder creation:**
  - Check if a bucket with the same name exists in Keboola Storage (in stage "in").
  - Create a new bucket in Keboola Storage if it does not exist.

### 3.3 Table Management & CSV File Processing
- **On CSV file creation:**
  - Validate that the file has a CSV extension.
  - Read and store the CSV header on the initial load.
  - Create a new table within the corresponding bucket for the first CSV load.
- **On CSV file update:**
  - Verify that the CSV header matches the stored header (or remote table header).
  - If the header is consistent, perform a full load (replacing existing table data).
  - If the header has changed, log an error and do not proceed with the import.

### 3.4 Error Handling and Logging
- Log all events and errors to a log file.
- Configure log verbosity through the configuration file.
- Utilize error handling with retries (e.g., exponential backoff) in case of API failures.

## 4. Non-Functional Requirements

- **Configurability:** All constants and variables (e.g., API tokens, endpoints, watchdog parameters, logging configurations) must be defined in a configuration file.
- **Maintainability:** Code must be clean, modular, and adhere to PEP8 guidelines.
- **Documentation:** Inline comments and this document should help any developer working on the daemon.
- **Deployment:** The daemon must be easily deployable using Docker.

## 5. Code Structure

Below is a suggested project structure:

- **/daemon**
  - `main.py` – Entry point for the daemon.
  - `config.py` – Module for loading and managing configuration settings.
  - `watcher.py` – Module that utilizes watchdog to capture filesystem events.
  - `storage_client.py` – Module that wraps interactions with Keboola Storage via the Python SDK.
  - `utils.py` – Helper functions and common utilities.
- **Dockerfile** – Setup instructions for building the Docker container.
- **requirements.txt** – List of Python dependencies.
- **docs/production_design.md** – This production design document.

## 6. Detailed Workflow

1. **Startup:**
   - Load configuration (API token, endpoint, logging settings, etc.).
   - Initialize the connection to Keboola Storage using the Python SDK.
   - Start the event-based file watcher on the designated folder.

2. **Sub-folder Creation Event:**
   - When a sub-folder is detected, validate whether the corresponding bucket exists in Keboola Storage.
   - Create the bucket if it does not exist.

3. **CSV File Creation/Update Event:**
   - On detection of a new CSV file:
     - Verify file validity (CSV extension, proper header format).
     - If it is the first load for that CSV:
       - Create a new table in the corresponding bucket.
       - Store the CSV header for subsequent validation.
     - If it is an update:
       - Compare the current CSV header with the stored header (or remote header).
       - If the header is valid, perform a full load (delete existing table data and replace it with the new data).

4. **Error Handling:**
   - In the event of failure (e.g., API errors, header mismatches), log detailed error messages.
   - Implement retry mechanisms for transient errors.

5. **Logging:**
   - Events, status updates, and errors should be logged to a file with a configured verbosity level.

## 7. Docker Considerations

- Include a `Dockerfile` that sets up the Python environment, installs required dependencies, and runs the daemon.
- Ensure that the folder to be monitored is mounted as a volume into the container.
- Expose necessary ports or settings for logging and configuration.

## 8. Additional Recommendations

- Create unit tests for each module, especially the event processing, CSV handling, and API interactions.
- Document any assumptions made during development.
- Maintain clear separation between configuration, business logic, and third-party API integrations.
- Consider enhancing the system design in the future to support optional incremental loads.

## 9. References

- [Keboola Storage API Documentation](https://keboola.docs.apiary.io/#)
- [Keboola Python SDK Documentation](https://developers.keboola.com/integrate/storage/python-client/)
- [Keboola Python SDK GitHub Repository](https://github.com/keboola/sapi-python-client/)
- [watchdog Documentation](https://python-watchdog.readthedocs.io/)

---

This document is intended to guide developers in implementing and maintaining the daemon. Please follow these instructions and refer to the provided references for detailed API usage and best practices.
