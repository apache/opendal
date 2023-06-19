# This is an example of how to use Open DAL to read and write test "Hello World" using multiple schemas/services: 

* Scheme::S3
* Scheme::Sled
* Scheme::Redis
* Scheme::Memory & Scheme::Dashmap 
with different settings when creating an operator for each Scheme

## S3 using init via map
```rust
fn init_operator_via_map() -> Result<Operator> {
    // setting up the credentials
let access_key_id = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID is set and a valid String");
let secret_access_key = env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_ACCESS_KEY_ID is set and a valid String");

    let mut map = HashMap::default();
    map.insert("bucket".to_string(), "test".to_string());
    map.insert("region".to_string(), "us-east-1".to_string());
    map.insert("endpoint".to_string(), "http://rpi4endpoint".to_string());
    map.insert("access_key_id".to_string(), access_key_id.to_string());
    map.insert(
        "secret_access_key".to_string(),
        secret_access_key.to_string(),
    );

    let op = Operator::via_map(Scheme::S3, map)?;
    Ok(op)
}
```
from the previous [example](https://github.com/apache/incubator-opendal/blob/main/examples/rust/01-init-operator/src/main.rs). 

# Sled 
```rust
            let mut builder = services::Sled::default();
            builder.datadir("/tmp/opendal/sled");
            // Init an operator
            let op = Operator::new(builder)?
                // Init with logging layer enabled.
                .layer(LoggingLayer::default())
                .finish();
```

# Redis
```rust
            let mut builder = services::Redis::default();
            builder.endpoint("redis://localhost:6379");
            // Init an operator
            let op = Operator::new(builder)?
                // Init with logging layer enabled.
                .layer(LoggingLayer::default())
                .finish();
```

# Dashmap and Memory 
Self-explanatory and doesn't need to take extra parameters

# Writing and reading the data is the same for all the schemes
```rust
    // Write data into object test.
    let test_string = format!("Hello, World! {scheme}");
    op.write("test", test_string).await?;

    // Read data from object.
    let bs = op.read("test").await?;
    info!("content: {}", String::from_utf8_lossy(&bs));

    // Get object metadata.
    let meta = op.stat("test").await?;
    info!("meta: {:?}", meta);
```
