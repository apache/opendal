- Proposal Name: `operator_from_uri`
- Start Date: 2024-12-23
- RFC PR: [apache/opendal#5444](https://github.com/apache/opendal/pull/5444)
- Tracking Issue: [apache/opendal#5445](https://github.com/apache/opendal/issues/5445)

# Summary

This RFC proposes adding URI-based configuration support to OpenDAL, allowing users to create operators directly from URIs. The proposal introduces a new `from_uri` API in both the `Operator` and `Configurator` traits, along with an `OperatorRegistry` to manage operator factories.

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

```
// Using with custom registry
let registry = OperatorRegistry::new();
registry.register("custom", my_factory);
let op = registry.parse("custom://endpoint", options)?;
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

`Configurator` will add a new API to create a configuration from a URI and options. OpenDAL will provide default implementations for common configurations. But services can override this method to support their own special needs.

For example, S3 might need to extract the `bucket` and `region` from the URI when possible.

```rust
impl Configurator for S3Config {
    fn from_uri(uri: &str, options: impl IntoIterator<Item = (String, String)>) -> Result<Self> {
        ...
    }
}
```

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

We are intentionally using `&str` instead of `Scheme` here to simplify working with external components outside this crate. Additionally, we plan to remove `Scheme` from our public API soon to enable splitting OpenDAL into multiple crates.

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

- Support for connection string format.
- Configuration presets like `r2` and `s3` with directory bucket enabled.
