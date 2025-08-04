- Proposal Name: `operator_from_uri`
- Start Date: 2024-12-23
- RFC PR: [apache/opendal#5444](https://github.com/apache/opendal/pull/5444)
- Tracking Issue: [apache/opendal#5445](https://github.com/apache/opendal/issues/5445)

# Summary

This RFC proposes adding URI-based configuration support to OpenDAL, allowing users to create operators directly from URIs. The proposal introduces a new `from_uri` API in both the `Operator` and `Configurator` traits, along with an `OperatorRegistry` to manage operator factories. As part of this change, we will also transition from the `Scheme` enum to string-based scheme identifiers, enabling better modularity and support for service crate splitting.

# Motivation

Currently, creating an operator in OpenDAL requires explicit configuration through builder patterns. While this approach provides type safety and clear documentation, it can be verbose and inflexible for simple use cases. Many storage systems are naturally identified by URIs (e.g., `s3://bucket/path`, `fs:///path/to/dir`).

Adding URI-based configuration would:

- Simplify operator creation for common use cases
- Enable configuration via connection strings (common in many applications)
- Make OpenDAL more approachable for new users
- Allow dynamic operator creation based on runtime configuration

# Guide-level explanation

The new API allows creating operators directly from URIs:

```rust
// Create an operator using URI
let op = Operator::from_uri("s3://my-bucket/path", vec![
    ("endpoint".to_string(), "http://localhost:8080"to_string()),
])?;

// Users can pass options through the URI along with additional key-value pairs
// The extra options will override identical options specified in the URI
let op = Operator::from_uri("s3://my-bucket/path?region=us-east-1", vec![
    ("endpoint".to_string(), "http://localhost:8080"to_string()),
])?;

// Create a file system operator
let op = Operator::from_uri("fs:///tmp/test", vec![])?;
```

OpenDAL will, by default, register services enabled by features in a global `OperatorRegistry`. Users can also create custom operator registries to support their own schemes or additional options.

```rust
// Using with custom registry
let registry = OperatorRegistry::new();
registry.register("custom", my_factory);
let op = registry.parse("custom://endpoint", options)?;

// The same service implementation can be registered under multiple schemes
registry.register("s3", s3_factory);
registry.register("minio", s3_factory);  // MinIO is S3-compatible
registry.register("r2", s3_factory);     // Cloudflare R2 is S3-compatible

// Users can define their own scheme names for internal use
registry.register("company-storage", s3_factory);
registry.register("backup-storage", azblob_factory);
```

# Reference-level explanation

The implementation consists of three main components:

1. The `OperatorFactory` and `OperatorRegistry`:

`OperatorFactory` is a function type that takes a URI and a map of options and returns an `Operator`. `OperatorRegistry` manages operator factories for different schemes.

```rust
type OperatorFactory = fn(http::Uri, HashMap<String, String>) -> Result<Operator>;

pub struct OperatorRegistry { ... }

impl OperatorRegistry {
    fn register(&self, scheme: &str, factory: OperatorFactory) {
        ...
    }

    fn parse(&self, uri: &str, options: impl IntoIterator<Item = (String, String)>) -> Result<Operator> {
        ...
    }
}
```

2. The `Configurator` trait extension:

`Configurator` will add a new API to create a configuration from a URI and options. Services should only parse the URI components relevant to their configuration (host, path, query parameters) without concerning themselves with the scheme portion.

```rust
impl Configurator for S3Config {
    fn from_uri(uri: &str, options: impl IntoIterator<Item = (String, String)>) -> Result<Self> {
        ...
    }
}
```

This design allows the same S3 implementation to work whether accessed via `s3://`, `minio://`, or any other user-defined scheme.

3. The `Operator` `from_uri` method:

The `Operator` trait will add a new `from_uri` method to create an operator from a URI and options. This method will use the global `OperatorRegistry` to find the appropriate factory for the scheme.

```rust
impl Operator {
    pub fn from_uri(
        uri: &str,
        options: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self> {
        ...
    }
}
```

## Scheme Enum Removal

As part of this RFC, we will transition from the `Scheme` enum to string-based identifiers (`&'static str`). This change is necessary because:

1. **Modularity**: Services in separate crates cannot add variants to a core enum
2. **Extensibility**: Users and third-party crates can define custom schemes without modifying OpenDAL
3. **Simplicity**: Services don't need to know their scheme identifier

# Drawbacks

- Increases API surface area
- Less type safety compared to builder patterns
- Potential for confusing error messages with invalid URIs
- Need to maintain backwards compatibility

# Rationale and alternatives

Alternatives considered:

1. Connection string format instead of URIs
2. Builder pattern with URI parsing
3. Macro-based configuration

URI-based configuration was chosen because:

- URIs are widely understood
- Natural fit for storage locations
- Extensible through custom schemes
- Common in similar tools

# Prior art

Similar patterns exist in:

- Database connection strings (PostgreSQL, MongoDB)
- [`object_store::parse_url`](https://docs.rs/object_store/latest/object_store/fn.parse_url.html)

# Unresolved questions

None

# Future possibilities

- Support for connection string format
- Configuration presets like `r2` and `s3` with directory bucket enabled
- Service crate splitting: Each service can live in its own crate and register itself with the core
- Plugin system: Allow dynamic loading of service implementations at runtime
- Service discovery: Automatically register available services based on feature flags or runtime detection
- Scheme validation and conventions: Provide utilities to validate scheme naming conventions
