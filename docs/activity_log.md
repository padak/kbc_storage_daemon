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
- [ ] Split configuration
  - [ ] Move sensitive data to .env
  - [ ] Create config.json structure
  - [ ] Implement config file handling
- [ ] CLI Implementation
  - [ ] Basic config commands (init, list)
  - [ ] Mapping management (add, edit, delete)
  - [ ] Configuration validation

#### Storage API Integration
- [ ] Enhanced API Support
  - [ ] Fetch and list all buckets
  - [ ] Fetch and list all tables
  - [ ] Manage streaming endpoints
- [ ] Table Management
  - [ ] File-to-table mapping
  - [ ] Sync mode configuration
  - [ ] Streaming endpoint setup

#### Monitoring Modes
- [ ] Full Load Mode
  - [ ] Monitor specific files
  - [ ] Header validation
  - [ ] Full table updates
- [ ] Incremental Mode
  - [ ] Append detection
  - [ ] Line tracking
  - [ ] Deletion handling
- [ ] Streaming Mode
  - [ ] TXT file support
  - [ ] HTTP POST implementation
  - [ ] Basic error handling

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
- Implementing configuration split (.env + config.json)
- Developing CLI commands for configuration management
- Adding support for streaming mode with TXT files

## Next Steps
1. Complete Phase 1 core functionality
2. Test and validate all sync modes
3. Begin UI integration in Phase 2

## Issues & Challenges
- Need to handle file deletion detection efficiently
- Ensure proper error handling for streaming mode
- Design user-friendly CLI interface

## Notes
- Starting with CLI implementation before UI
- Focusing on TXT file streaming initially
- Planning to add more file format support later 