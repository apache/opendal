- Proposal Name: `operator_from_uri`
- Start Date: 2024-12-23
- RFC PR: [apache/opendal#0000](https://github.com/apache/opendal/pull/0000)
- Tracking Issue: [apache/opendal#0000](https://github.com/apache/opendal/issues/0000)

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
    ("access_key_id".to_string(), "xxx".to_string()),
    ("secret_key_key".to_string(), "yyy".to_string()),
])?;

// Create a file system operator
let op = Operator::from_uri("fs:///tmp/test", vec![])?;

// Using with custom registry
let registry = OperatorRegistry::new();
registry.register("custom", my_factory);
let op = registry.parse("custom://endpoint", options)?;
```

# Reference-level explanation

The implementation consists of three main components:

1. The `OperatorRegistry`:

```rust
type OperatorFactory = fn(http::Uri, HashMap<String, String>) -> Result<Operator>;

pub struct OperatorRegistry {
    register: Arc<Mutex<HashMap<String, OperatorFactory>>>,
}

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

```rust
impl Configurator for S3Config {
    fn from_uri(uri: &str, options: impl IntoIterator<Item = (String, String)>) -> Result<Self> {
        ...
    }
}
```

3. The `Operator` factory method:

```rust
impl Operator {
    pub fn from_uri(
        uri: &str,
        options: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self> {
        static REGISTRY: Lazy<OperatorRegistry> = Lazy::new(|| {
            let registry = OperatorRegistry::new();
            // Register built-in operators
            registry.register("s3", s3_factory);
            registry.register("fs", fs_factory);
            // ...
            registry
        });
      
        REGISTRY.parse(uri, options)
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

- Rust's `url` crate
- Database connection strings (PostgreSQL, MongoDB)
- AWS SDK endpoint configuration
- Python's `urllib`

# Unresolved questions

- Should we support custom URI parsing per operator?
- How to handle scheme conflicts?
- Should we support URI validation?
- How to handle complex configurations that don't map well to URIs?

# Future possibilities

- Support for connection string format
- URI templates for batch operations
- Custom scheme handlers
- Configuration presets
- URI validation middleware
- Dynamic operator loading based on URI schemes
