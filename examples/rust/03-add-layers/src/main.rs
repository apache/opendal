use std::time::Duration;

use env_logger;
use opendal::layers::LoggingLayer;
use opendal::layers::TimeoutLayer;
use opendal::services::Memory;
use opendal::Operator;
use opendal::Result;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let op = Operator::new(Memory::default())
        .expect("must init")
        .layer(LoggingLayer::default())
        .layer(
            TimeoutLayer::default()
                .with_timeout(Duration::from_secs(5))
                .with_io_timeout(Duration::from_secs(3)),
        )
        .finish();

    write_data(op.clone()).await?;
    read_data(op.clone()).await?;
    Ok(())
}

async fn write_data(op: Operator) -> Result<()> {
    op.write("test", "Hello, World!").await?;

    Ok(())
}

async fn read_data(op: Operator) -> Result<()> {
    let bs = op.read("test").await?;
    println!("data: {}", String::from_utf8_lossy(&bs));

    Ok(())
}
