use opendal::services::Memory;
use opendal::Operator;
use opendal::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let op = Operator::new(Memory::default())?.finish();

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
