# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

While working on OpenDAL, please remember:

- Always use English in code and comments.
- Only add meaningful comments when the code's behavior is difficult to understand.
- Only add meaningful tests when they actually verify internal behaviors; otherwise, don't create them unless requested.

## Build and Development Commands

### Core Rust Development

- All cargo commands must be executed within the `core` directory.
- All changes to `core` must pass the clippy check and behavior tests.
- Don't change public API or behavior tests unless requested.

```bash
# Check code
cargo check

# Build
cargo build

# Run linter for all services.
cargo clippy --all-targets --all-features -- -D warnings

# Run linter for specific services.
cargo clippy --all-targets --features=services-s3 -- -D warnings

# Run tests (requires test features)
cargo test --features tests

# Run behavior tests
OPENDAL_TEST=s3 cargo test --features services-s3,tests behavior

# Run specific test
cargo test tests::it::services::fs

# Format code
cargo fmt

# Check formatting
cargo fmt --check

# Build documentation
cargo doc --lib --no-deps --all-features

# Run benchmarks
cargo bench
```

### Multi-package Operations
```bash
# Run command across all packages
./scripts/workspace.py cargo check
./scripts/workspace.py cargo fmt -- --check
```

### Code Generation and Release
```bash
# Generate bindings code
just generate python
just generate java

# Update version across project
just update-version

# Release process
just release
```

### Testing Setup
```bash
# Copy environment template for tests
cp .env.example .env
# Edit .env with your service credentials
```

## Architecture Overview

OpenDAL is a unified data access layer with a modular architecture:

### Core Structure
- **core/**: Main Rust library implementing the storage abstraction
  - `src/services/`: 50+ storage service implementations (S3, Azure, GCS, etc.)
  - `src/layers/`: Middleware for cross-cutting concerns (retry, metrics, tracing)
  - `src/raw/`: Low-level traits and types for implementing backends
  - `src/types/`: Core types and traits

### Service Implementation Pattern
Each service follows a consistent structure:
- `backend.rs`: Main service implementation with `Accessor` trait
- `config.rs`: Configuration and builder pattern
- `core.rs`: Core logic and HTTP client setup
- `writer.rs`, `reader.rs`, `lister.rs`: Operation implementations
- `error.rs`: Service-specific error handling

### Language Bindings
- **bindings/**: Language-specific bindings (Python, Java, Go, Node.js, etc.)
  - Each binding has its own build system and contributing guide
  - Generated code uses the `dev` crate for consistency

### Applications
- **bin/oli**: CLI tool for data access (like s3cmd)
- **bin/oay**: API gateway for HTTP access
- **bin/ofs**: POSIX filesystem interface

### Integrations
- **integrations/**: Ecosystem integrations (FUSE, WebDAV, object_store)

## Key Development Patterns

### Feature Flags
Services are feature-gated to reduce binary size:
```rust
// Enable specific services
[features]
services-s3 = []
services-azblob = []
```

### Layer System
Layers provide composable middleware:
```rust
op.layer(RetryLayer::new())
  .layer(MetricsLayer::new())
  .layer(LoggingLayer::new())
```

### Async-First Design
All core operations are async with blocking wrappers available:
```rust
// Async API
let data = op.read("path").await?;

// Blocking API
let data = op.blocking().read("path")?;
```

## Commit Message Format
Use conventional commits:
```
feat(services/gcs): Add start-after support for list
fix(services/s3): Ignore prefix if it's empty
docs: Add troubleshooting guide
ci: Update GitHub Actions workflow
refactor(core): Simplify error handling
```

## Testing Approach
- Unit tests in `src/tests/`
- Behavior tests validate service implementations
- Integration tests require service credentials in `.env`
- CI runs tests across multiple platforms and Rust versions

## Common Tasks

### Adding a New Service
1. Create new module in `core/src/services/`
2. Implement `Accessor` trait
3. Add service feature flag in `Cargo.toml`
4. Add service tests in behavior test suite
5. Update documentation

### Debugging Service Issues
1. Enable debug logging: `RUST_LOG=debug`
2. Use tracing layer for detailed operation tracking
3. Check service-specific error types
4. Verify credentials and endpoint configuration

## Important Notes
- Minimum Rust version: 1.82 (MSRV)
- All services implement the same `Accessor` trait
- Use `just` for common development tasks
- Check CI workflows for platform-specific requirements
- Services are tested against real backends (credentials required)
