# OpenDAL C Binding Test Framework

This directory contains a comprehensive test framework for the OpenDAL C binding, modeled after the Python and JavaScript binding test suites.

## Quick Start

The test framework follows the standard OpenDAL testing pattern:

```bash
# Test with memory service (default)
OPENDAL_TEST=memory make behavior_test

# Test with filesystem service  
OPENDAL_TEST=fs OPENDAL_FS_ROOT=/tmp/test make behavior_test

# Test with S3 service
OPENDAL_TEST=s3 OPENDAL_S3_BUCKET=test-bucket OPENDAL_S3_REGION=us-east-1 make behavior_test
```

## Framework Features

The test framework implements the core requirements:

- ✅ **Environment Variable Support**: Reads `OPENDAL_TEST` as the scheme to test
- ✅ **Configuration Mapping**: Reads all `OPENDAL_{scheme}_{key}={value}` environment variables to construct the operator
- ✅ **Comprehensive Testing**: Performs test logic on operators (write files, read back, verify checksums, etc.)
- ✅ **Capability Support**: Automatically skips tests when backend doesn't support required capabilities
- ✅ **Random Root Support**: Generates random roots for test isolation (can be disabled with `OPENDAL_DISABLE_RANDOM_ROOT=true`)

## Overview

The test framework provides:

- **Structured Test Organization**: Tests are organized into logical suites (Basic Operations, List Operations, Reader/Writer Operations)
- **Capability-Based Testing**: Tests automatically skip when the backend doesn't support required capabilities
- **Environment-Driven Configuration**: Service and configuration are specified via environment variables
- **Flexible Test Execution**: Run all tests, specific suites, or individual test cases
- **Random Root Generation**: Automatic isolation of test runs with random root paths
- **Comprehensive Assertions**: Rich assertion macros with detailed error reporting

## Architecture

### Core Components

- **`test_framework.h/cpp`**: Core framework with assertion macros, configuration management, and test runner
- **`test_suites_basic.cpp`**: Basic CRUD operations (check, write, read, exists, stat, delete, create_dir)
- **`test_suites_list.cpp`**: Directory listing and traversal operations
- **`test_suites_reader_writer.cpp`**: Streaming read/write operations with seek functionality
- **`test_runner.cpp`**: Main executable with command-line interface

### Test Structure

Each test case specifies:
- **Name**: Human-readable test identifier
- **Function**: Test implementation
- **Capabilities**: Required backend capabilities (automatically checked)

```cpp
opendal_test_case basic_tests[] = {
    {"write_read", test_write_read, CAPABILITY(.read = true, .write = true)},
    {"exists", test_exists, CAPABILITY(.write = true)},
    // ...
};
```

## Building

### Prerequisites

- C++ compiler with C++17 support
- OpenDAL C binding library
- libuuid development headers

### Build Commands

```bash
# Build the test runner
make all

# Or use the existing build system
cd ..
make  # This should build the C binding
cd tests
make
```

## Running Tests

### Basic Usage

```bash
# Run all tests (default: memory service)
./opendal_test_runner

# List available test suites
./opendal_test_runner --list-suites

# Run specific test suite
./opendal_test_runner --suite "Basic Operations"

# Run specific test case
./opendal_test_runner --test "Basic Operations::write_read"

# Get help
./opendal_test_runner --help
```

### Using Different Services

Configure the service via environment variables:

```bash
# Test with memory service (default)
OPENDAL_TEST=memory ./opendal_test_runner

# Test with filesystem service
OPENDAL_TEST=fs OPENDAL_FS_ROOT=/tmp/opendal_test ./opendal_test_runner

# Test with S3 service
OPENDAL_TEST=s3 \
OPENDAL_S3_BUCKET=my-bucket \
OPENDAL_S3_REGION=us-west-2 \
OPENDAL_S3_ACCESS_KEY_ID=your-key \
OPENDAL_S3_SECRET_ACCESS_KEY=your-secret \
./opendal_test_runner

# Disable random root generation
OPENDAL_DISABLE_RANDOM_ROOT=true ./opendal_test_runner
```

### Convenient Make Targets

```bash
make test          # Run all tests with memory service
make test-memory   # Run tests with memory service
make test-fs       # Run tests with filesystem service
make list-suites   # List all available test suites
make help          # Show make help
```

## Configuration

### Environment Variables

The test framework follows the same configuration pattern as Python/JavaScript bindings:

- **`OPENDAL_TEST`**: Service to test (memory, fs, s3, gcs, azblob, etc.)
- **`OPENDAL_DISABLE_RANDOM_ROOT`**: Set to 'true' to disable random root generation
- **`OPENDAL_<SERVICE>_<CONFIG>`**: Service-specific configuration keys

### Service Configuration Examples

**Filesystem:**
```bash
OPENDAL_TEST=fs
OPENDAL_FS_ROOT=/path/to/test/directory
```

**S3:**
```bash
OPENDAL_TEST=s3
OPENDAL_S3_BUCKET=test-bucket
OPENDAL_S3_REGION=us-east-1
OPENDAL_S3_ACCESS_KEY_ID=your-access-key
OPENDAL_S3_SECRET_ACCESS_KEY=your-secret-key
```

**Azure Blob:**
```bash
OPENDAL_TEST=azblob
OPENDAL_AZBLOB_CONTAINER=test-container
OPENDAL_AZBLOB_ACCOUNT_NAME=your-account
OPENDAL_AZBLOB_ACCOUNT_KEY=your-key
```

## Test Suites

### Basic Operations
Tests fundamental CRUD operations:
- **check**: Verify operator functionality
- **write_read**: Write data and read it back
- **exists**: Check file existence
- **stat**: Get file metadata
- **delete**: Remove files (with idempotency check)
- **create_dir**: Create directories

### List Operations
Tests directory listing and traversal:
- **list_basic**: List directory contents
- **list_empty_dir**: List empty directory
- **list_nested**: List nested directory structure
- **entry_name_path**: Verify entry name vs path properties

### Reader and Writer Operations
Tests streaming I/O operations:
- **reader_basic**: Basic file reading
- **reader_seek**: Seek operations (SET, CUR, END)
- **writer_basic**: Basic file writing
- **writer_large_data**: Write large amounts of data
- **reader_partial_read**: Read files in chunks

## Assertion Macros

The framework provides comprehensive assertion macros:

```cpp
OPENDAL_ASSERT(condition, message)              // General condition check
OPENDAL_ASSERT_EQ(expected, actual, message)    // Equality check
OPENDAL_ASSERT_STR_EQ(expected, actual, message) // String equality
OPENDAL_ASSERT_NULL(ptr, message)               // Null pointer check
OPENDAL_ASSERT_NOT_NULL(ptr, message)           // Non-null pointer check
OPENDAL_ASSERT_NO_ERROR(error, message)         // OpenDAL error check
```

## Adding New Tests

### 1. Create Test Function
```cpp
void test_my_feature(opendal_test_context* ctx) {
    // Your test implementation
    const char* path = "test_file.txt";
    
    // Use assertions
    OPENDAL_ASSERT_NOT_NULL(ctx->config->operator_instance, "Operator should be available");
    
    // Test your feature...
    
    // Cleanup
}
```

### 2. Add to Test Suite
```cpp
opendal_test_case my_tests[] = {
    {"my_feature", test_my_feature, CAPABILITY(.write = true, .read = true)},
    // ... other tests
};

opendal_test_suite my_suite = {
    .name = "My Test Suite",
    .tests = my_tests,
    .test_count = sizeof(my_tests) / sizeof(my_tests[0])
};
```

### 3. Register in Runner
Add your suite to `test_runner.cpp`:
```cpp
extern opendal_test_suite my_suite;

static opendal_test_suite* all_suites[] = {
    &basic_suite,
    &list_suite,
    &reader_writer_suite,
    &my_suite,  // Add your suite here
};
```

## Design Philosophy

This test framework mirrors the design patterns from Python and JavaScript bindings:

1. **Environment-driven configuration**: Same environment variable patterns
2. **Capability-based testing**: Tests skip automatically when capabilities are missing
3. **Random root isolation**: Each test run uses a unique root path
4. **Comprehensive coverage**: Tests cover all major API surface areas
5. **Easy extensibility**: Simple to add new test suites and cases

## Integration with CI/CD

The test framework is designed for integration with automated testing:

- **Exit codes**: Returns 0 for success, 1 for failure
- **Structured output**: Clear test results and summaries
- **Environment flexibility**: Easy to configure for different services
- **Parallel safety**: Random roots prevent test interference

Example CI usage:
```bash
# Test with multiple services
for service in memory fs; do
    echo "Testing with $service"
    OPENDAL_TEST=$service make test || exit 1
done
``` 