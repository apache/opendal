use std::env;
use std::sync::Arc;

use opendal::credential::Credential;
use opendal::error::Result;
use opendal::services::azblob;
use opendal::Accessor;

/// In order to test azblob service, please set the following environment variables:
/// - `OPENDAL_AZBLOB_TEST=on`: set to `on` to enable the test.
/// - `OPENDAL_AZBLOB_ROOT=/path/to/dir`: set the root dir.
/// - `OPENDAL_AZBLOB_BUCKET=<bucket>`: set the bucket name.
/// - `OPENDAL_AZBLOB_ENDPOINT=<endpoint>`: set the endpoint of the azblob service.
/// - `OPENDAL_AZBLOB_ACCESS_NAME=<access_name>`: set the access_name.
/// - `OPENDAL_AZBLOB_SHARED_KEY=<shared_key>`: set the shared_key.
pub async fn new() -> Result<Option<Arc<dyn Accessor>>> {
    dotenv::from_filename(".env").ok();

    let root =
        &env::var("OPENDAL_AZBLOB_ROOT").unwrap_or_else(|_| format!("/{}", uuid::Uuid::new_v4()));

    let mut builder = azblob::Backend::build();

    builder
        .root(root)
        .bucket(&env::var("OPENDAL_AZBLOB_BUCKET").expect("OPENDAL_AZBLOB_BUCKET must set"))
        .endpoint(&env::var("OPENDAL_AZBLOB_ENDPOINT").unwrap_or_default())
        .credential(Credential::hmac(
            &env::var("OPENDAL_AZBLOB_ACCESS_NAME").unwrap_or_default(),
            &env::var("OPENDAL_AZBLOB_SHARED_KEY").unwrap_or_default(),
        ));

    Ok(Some(builder.finish().await?))
}
