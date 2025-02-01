# Keboola Storage Daemon - Development Plan

## Phase 1: Project Setup and Basic Infrastructure
1. [x] Initialize project structure
   - [x] Create basic folder structure
   - [x] Set up Python virtual environment
   - [x] Create initial requirements.txt with base dependencies
   - [x] Set up .gitignore

2. [x] Configuration Management
   - [x] Implement config loader (supporting both JSON and YAML)
   - [x] Set up .env handling for API tokens
   - [x] Create sample configuration files

3. [x] Logging System
   - [x] Implement file-based logging with rotation (100MB limit)
   - [x] Set up log formatting
   - [x] Add basic logging utilities

4. [x] Docker Setup
   - [x] Create Dockerfile using lightweight base image
   - [x] Set up volume mounting for watched directory
   - [x] Configure logging volume

## Phase 2: Core Functionality - File System Monitoring
1. [x] Implement Directory Watcher
   - [x] Set up watchdog observer
   - [x] Implement event handlers for:
     - [x] Directory creation
     - [x] File creation
     - [x] File modification

2. [x] CSV File Processing
   - [x] Implement CSV dialect detection
   - [x] Add UTF-8 validation
   - [x] Create header validation logic
   - [x] Implement gzip compression for files >50MB

## Phase 3: Keboola Integration
1. [x] Storage Client Implementation
   - [x] Set up Keboola SDK integration
   - [x] Implement bucket management
   - [x] Create table operations wrapper

2. [x] Data Upload Logic
   - [x] Implement initial table creation
   - [x] Add full load table update
   - [x] Handle compressed file uploads
   - [x] Implement CSV import with proper dialect settings

## Phase 4: Error Handling and Robustness
1. [ ] Implement Retry Mechanism
   - Add exponential backoff for API calls
   - Implement proper error catching
   - Add detailed error logging

2. [ ] Edge Cases Handling
   - Handle invalid CSV files
   - Process header changes
   - Manage concurrent file operations

## Phase 5: Final Integration and Testing
1. [ ] System Integration
   - Connect all components
   - Implement graceful shutdown
   - Add startup validation

2. [ ] Manual Testing
   - Test with various CSV formats
   - Verify bucket/table creation
   - Test error scenarios
   - Validate logging system

## TODO (Future Enhancements)

### Monitoring & Metrics
- [ ] Add Prometheus metrics export
- [ ] Implement health check endpoints
- [ ] Add operational metrics (processed files, errors, etc.)
- [ ] Create dashboard templates

### Testing
- [ ] Unit tests for core components
- [ ] Integration tests with Keboola API
- [ ] Mock filesystem events for testing
- [ ] Add CI/CD pipeline

### Security & Performance
- [ ] Add file permission checks
- [ ] Implement rate limiting
- [ ] Add API token rotation capability
- [ ] Performance optimization for large files

### Documentation
- [ ] API documentation
- [ ] Deployment guide
- [ ] Troubleshooting guide
- [ ] Configuration reference
