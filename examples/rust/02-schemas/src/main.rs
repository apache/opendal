use log::debug;
use log::info;
use opendal::layers::LoggingLayer;
use opendal::services;
use opendal::Operator;
use opendal::Result;
use opendal::Scheme;
use std::collections::HashMap;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let schemes = [
        Scheme::S3,
        Scheme::Memory,
        Scheme::Dashmap,
        Scheme::Sled,
        Scheme::Redis,
    ];

    for scheme in schemes.iter() {
        info!("scheme: {:?}", scheme);
        read_and_write(*scheme).await?;
    }

    Ok(())
}

async fn read_and_write(scheme: Scheme) -> Result<()> {
    // Write data into object test and read it back
    let op = match scheme {
        Scheme::S3 => {
            let op = init_operator_via_map()?;
            debug!("operator: {op:?}");
            op
        }
        Scheme::Dashmap => {
            let builder = services::Dashmap::default();
            // Init an operator
            let op = Operator::new(builder)?
                // Init with logging layer enabled.
                .layer(LoggingLayer::default())
                .finish();
            debug!("operator: {op:?}");
            op
        }
        Scheme::Sled => {
            let mut builder = services::Sled::default();
            builder.datadir("/tmp/opendal/sled");
            // Init an operator
            let op = Operator::new(builder)?
                // Init with logging layer enabled.
                .layer(LoggingLayer::default())
                .finish();
            debug!("operator: {op:?}");
            op
        }
        Scheme::Redis => {
            let mut builder = services::Redis::default();
            builder.endpoint("redis://localhost:6379");
            // Init an operator
            let op = Operator::new(builder)?
                // Init with logging layer enabled.
                .layer(LoggingLayer::default())
                .finish();
            debug!("operator: {op:?}");
            op
        }
        _ => {
            let builder = services::Memory::default();
            // Init an operator
            let op = Operator::new(builder)?
                // Init with logging layer enabled.
                .layer(LoggingLayer::default())
                .finish();
            debug!("operator: {op:?}");
            op
        }
    };
    // Write data into object test.
    let test_string = format!("Hello, World! {scheme}");
    op.write("test", test_string).await?;

    // Read data from object.
    let bs = op.read("test").await?;
    info!("content: {}", String::from_utf8_lossy(&bs));

    // Get object metadata.
    let meta = op.stat("test").await?;
    info!("meta: {:?}", meta);

    Ok(())
}

fn init_operator_via_map() -> Result<Operator> {
    // setting up the credentials
    let access_key_id =
        env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID is set and a valid String");
    let secret_access_key =
        env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_ACCESS_KEY_ID is set and a valid String");

    let mut map = HashMap::default();
    map.insert("bucket".to_string(), "test".to_string());
    map.insert("region".to_string(), "us-east-1".to_string());
    map.insert("endpoint".to_string(), "http://rpi4node3:8333".to_string());
    map.insert("access_key_id".to_string(), access_key_id);
    map.insert("secret_access_key".to_string(), secret_access_key);

    let op = Operator::via_map(Scheme::S3, map)?;
    Ok(op)
}
