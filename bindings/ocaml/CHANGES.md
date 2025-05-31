# Changelog

All notable changes to the OpenDAL OCaml bindings will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.48.0] - 2024-06-25

### Added
- Initial release of OpenDAL OCaml bindings
- Support for basic file operations (read, write, delete, copy, rename)
- Support for multiple storage backends (filesystem, S3, etc.)
- Blocking API implementation
- Directory operations (create_dir, list)
- Metadata operations (stat, exists)
- Reader and Writer interfaces
- Comprehensive test suite

### Features
- **Storage Backends**: Support for local filesystem, S3, and other cloud storage services
- **File Operations**: Complete CRUD operations for files and directories
- **Metadata**: Access to file metadata including size, modification time, content type
- **Error Handling**: Robust error handling with detailed error messages
- **Type Safety**: Full OCaml type safety with no unsafe operations exposed

### Dependencies
- OCaml >= 4.10.0 and < 5.0.0
- Dune >= 2.1
- conf-rust (for building the Rust backend)
- ounit2 >= 2.2.6 (for testing)

### Supported Platforms
- Linux (x86_64, ARM64)
- macOS (x86_64, ARM64)
- Windows (via WSL)

### Documentation
- Complete API documentation
- Usage examples
- Build and installation instructions
