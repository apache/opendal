# Apache OpenDAL™ unftp Integration

`unftp-sbe-opendal` is an [unftp](https://crates.io/crates/unftp) `StorageBackend` implementation using opendal.

This crate can help you to access ANY storage services with the same ftp API.

## Examples

```rust
use anyhow::Result;
use opendal::Operator;
use unftp_sbe_opendal::OpendalStorage;

#[tokio::main]
async fn main() -> Result<()> {
    // Create any service desired
    let service = opendal::services::S3::from_map(
        [
            ("access_key".to_string(), "my_access_key".to_string()),
            ("secret_key".to_string(), "my_secret_key".to_string()),
            ("endpoint".to_string(), "my_endpoint".to_string()),
            ("region".to_string(), "my_region".to_string()),
        ]
        .into_iter()
        .collect(),
    );

    // Init an operator with the service created
    let op = Operator::new(service)?.finish();

    // Wrap the operator with `OpendalStorage`
    let backend = OpendalStorage::new(op);

    // Build the actual unftp server
    let server = libunftp::ServerBuilder::new(Box::new(move || backend.clone())).build()?;

    // Start the server
    server.listen("0.0.0.0:0").await?;

    Ok(())
}
```

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
