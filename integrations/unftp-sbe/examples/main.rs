use std::error::Error;

use opendal::Operator;
use unftp_sbe_opendal::OpendalStorage;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Create any service desired
    let service = opendal::services::Memory::default();

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
