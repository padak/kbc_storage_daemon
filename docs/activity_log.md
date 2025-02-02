# Activity Log

## Version 1 Development (Completed)
- [x] Basic daemon functionality
  - [x] File system monitoring with watchdog
  - [x] CSV file detection and processing
  - [x] Basic Keboola Storage integration

- [x] Core Features
  - [x] Environment configuration (.env)
  - [x] Bucket/table management
  - [x] Full load implementation
  - [x] Incremental load support
  - [x] Status bar app integration

## Version 2 Development (In Progress)

### Phase 1: Core Functionality
#### Configuration Management
- [x] Split configuration
  - [x] Move sensitive data to .env
  - [x] Create config.json structure
  - [x] Implement config file handling
- [ ] CLI Implementation
  - [ ] Basic config commands (init, list)
  - [ ] Mapping management (add, edit, delete)
  - [ ] Configuration validation

#### Storage API Integration
- [x] Enhanced API Support
  - [x] Fetch and list all buckets
  - [x] Fetch and list all tables
  - [x] Manage streaming endpoints
- [x] Table Management
  - [x] File-to-table mapping
  - [x] Sync mode configuration
  - [x] Streaming endpoint setup

#### Monitoring Modes
- [x] Full Load Mode
  - [x] Monitor specific files
  - [x] Header validation
  - [x] Full table updates
- [x] Incremental Mode
  - [x] Append detection
  - [x] Line tracking
  - [x] Deletion handling
- [x] Streaming Mode
  - [x] TXT file support
  - [x] HTTP POST implementation
  - [x] Basic error handling

### Phase 2: UI Integration
- [ ] Status Bar Updates
  - [ ] Configuration menu
  - [ ] Mapping management
  - [ ] Status indicators
- [ ] Configuration Interface
  - [ ] Table/bucket selection
  - [ ] File mapping setup
  - [ ] Mode selection UI

## Testing & Documentation
- [ ] Unit Tests
  - [ ] Configuration management
  - [ ] API integration
  - [ ] Streaming functionality
- [ ] Documentation
  - [ ] CLI command reference
  - [ ] Configuration guide
  - [ ] API integration details

## Current Focus
- Implementing CLI commands for configuration management
- Adding unit tests for sync handlers
- Preparing for UI integration

## Next Steps
1. Create CLI module for configuration management
2. Add unit tests for sync handlers and configuration
3. Begin status bar UI integration

## Recent Changes
- Implemented sync mode handlers (FullLoadHandler, IncrementalHandler, StreamingHandler)
- Updated watcher module to use new sync handlers
- Added streaming support with batching and error handling
- Added requests package for streaming functionality
- Refactored configuration to support file mappings and sync modes

## Issues & Challenges
- Need to handle file deletion detection efficiently
- Ensure proper error handling for streaming mode
- Design user-friendly CLI interface
- Add comprehensive test coverage for new features

## Notes
- Starting with CLI implementation before UI
- Focusing on TXT file streaming initially
- Planning to add more file format support later

## 2024-02-02

### Implemented Core Functionality
1. Fixed environment variable handling
   - Added proper clearing of environment variables in reload command
   - Improved handling of `.env` file loading
   - Fixed issue with venv activation overriding variables

2. Enhanced Interactive Mode
   - Added CSV file analysis for better configuration
   - Implemented smart primary key detection
   - Added file path resolution (current dir and watched dir)
   - Improved UX with column listing and suggestions

3. Added Sync Command
   - Implemented manual sync functionality
   - Added support for both single mapping and all mappings sync
   - Fixed table existence checking and creation
   - Proper handling of incremental vs full load modes

4. Started Daemon Implementation
   - Created basic daemon structure with watchdog
   - Implemented file change monitoring
   - Added debouncing for file events
   - Proper logging setup

### Code Organization
1. Improved code structure:
   - Split sync functionality into separate module
   - Fixed circular imports
   - Better error handling and logging
   - Shared code between CLI and daemon

### Next Steps
1. Test daemon functionality with real-time file changes
2. Add more robust error handling for file operations
3. Implement streaming mode for text files
4. Add configuration validation
5. Consider adding status notifications 