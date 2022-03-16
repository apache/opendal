// use std::env;
// use std::sync::Arc;

// use opendal::error::Result;
// use opendal::services::azblob;
// use opendal::Accessor;

// pub async fn new() -> Result<Option<Arc<dyn Accessor>>> {
//     dotenv::from_filename(".env").ok();

//     if env::var("OPENDAL_AZBLOB_TEST").is_err() || env::var("OPENDAL_AZBLOB_TEST").unwrap() != "on" {
//         return Ok(None);
//     }

//     let root =
//         &env::var("OPENDAL_S3_ROOT").unwrap_or_else(|_| format!("/{}", uuid::Uuid::new_v4()));

//     let mut builder = azblob::Backend::build();
//     builder.root(root);
//     builder.bucket(&env::var("OPENDAL_AZBLOB_BUCKET").expect("OPENDAL_AZBLOB_BUCKET must set"));

//     builder.credential(Credential::hmac(
//         &env::var("OPENDAL_AZBLOB_ACCESS_KEY_ID").unwrap_or_default(),
//         &env::var("OPENDAL_AZBLOB_SECRET_ACCESS_KEY").unwrap_or_default(),
//     ));

//     Ok(Some(builder.finish().await?))
// }