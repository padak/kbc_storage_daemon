# Manual Test Plan

## 1. CSV Format Testing
### Test Case 1.1: Basic CSV Files
- **Input**: Simple CSV with standard format
- **Expected**: 
  - File is detected and processed
  - Table created in Keboola
  - Data matches source file

### Test Case 1.2: Different Delimiters
- **Input**: Files with various delimiters (comma, semicolon, tab)
- **Expected**: 
  - Correct delimiter detection
  - Proper table creation
  - Data correctly parsed

### Test Case 1.3: Character Encodings
- **Input**: Files with different encodings (UTF-8, UTF-8 with BOM)
- **Expected**:
  - Proper encoding detection
  - Correct character handling
  - No data corruption

### Test Case 1.4: Large Files
- **Input**: CSV file > 50MB
- **Expected**:
  - Automatic compression
  - Successful upload
  - Data integrity maintained

## 2. Bucket/Table Operations
### Test Case 2.1: New Bucket Creation
- **Input**: CSV in new directory
- **Expected**:
  - Bucket automatically created
  - Proper naming convention followed
  - Correct permissions set

### Test Case 2.2: Table Updates
- **Input**: Modified CSV file
- **Expected**:
  - Table update triggered
  - Data properly updated
  - Header consistency maintained

### Test Case 2.3: Concurrent Operations
- **Input**: Multiple files simultaneously
- **Expected**:
  - All files processed
  - No data corruption
  - Proper locking mechanism

## 3. Error Scenarios
### Test Case 3.1: Invalid CSV Files
- **Input**: Malformed CSV
- **Expected**:
  - Error logged
  - No table corruption
  - Proper error message

### Test Case 3.2: Network Issues
- **Input**: Simulate network interruption
- **Expected**:
  - Retry mechanism activated
  - Operation eventually succeeds
  - Proper error logging

### Test Case 3.3: Permission Issues
- **Input**: Invalid API token
- **Expected**:
  - Clear error message
  - Graceful shutdown
  - Proper error logging

## 4. Logging System
### Test Case 4.1: Log Generation
- **Verify**:
  - Log file creation
  - Rotation at 100MB
  - Proper formatting

### Test Case 4.2: Log Content
- **Verify**:
  - All operations logged
  - Error traceability
  - Appropriate log levels

### Test Case 4.3: Log Directory
- **Verify**:
  - Directory creation
  - Write permissions
  - Proper cleanup

## 5. System Integration
### Test Case 5.1: Startup Validation
- **Verify**:
  - Directory validation
  - Configuration validation
  - Storage connection check

### Test Case 5.2: Graceful Shutdown
- **Verify**:
  - SIGTERM handling
  - SIGINT handling
  - Resource cleanup

### Test Case 5.3: Resource Management
- **Verify**:
  - Memory usage
  - CPU usage
  - File handle cleanup

## Test Environment Setup
1. Create test directory structure
2. Prepare sample CSV files
3. Set up test Keboola project
4. Configure test environment variables

## Test Data Requirements
1. Sample CSV files of various sizes
2. Files with different delimiters
3. Files with different encodings
4. Malformed files for error testing

## Test Execution Checklist
- [ ] Set up test environment
- [ ] Execute all test cases
- [ ] Document results
- [ ] Track any issues found
- [ ] Verify fixes for issues 