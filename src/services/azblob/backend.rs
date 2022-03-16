
// use std::collections::HashMap;



// use std::num::NonZeroU32;

// use anyhow::anyhow;
// use async_trait::async_trait;

// use metrics::increment_counter;

// use super::error::parse_get_object_error;
// use super::error::parse_head_object_error;
// use super::error::parse_unexpect_error;

// use crate::credential::Credential;
// use crate::error::Error;
// use crate::error::Kind;
// use crate::error::Result;
// use crate::object::BoxedObjectStream;
// use crate::object::Metadata;
// use crate::ops::OpDelete;
// use crate::ops::OpList;
// use crate::ops::OpRead;
// use crate::ops::OpStat;
// use crate::ops::OpWrite;
// use log::debug;
// use log::error;
// use log::info;
// use log::warn;
// use crate::Accessor;
// use crate::BoxedAsyncReader;
// use crate::ObjectMode;
// use std::sync::Arc;

// use azure_core::prelude::*;
// use azure_storage::core::prelude::*;
// use azure_storage_blobs::prelude::*;

// #[derive(Default, Debug, Clone)]
// pub struct Builder {
//     root: Option<String>,
//     bucket: String, // in Azure, bucket =  container
//     credential: Option<Credential>,
// }

// impl Builder {
//     pub fn root(&mut self, root: &str) -> &mut Self {
//         self.root = if root.is_empty() {
//             None
//         } else {
//             Some(root.to_string())
//         };

//         self
//     }
//     pub fn bucket(&mut self, bucket: &str) -> &mut Self {
//         self.bucket = bucket.to_string();

//         self
//     }

//     pub fn credential(&mut self, credential: Credential) -> &mut Self {
//         self.credential = Some(credential);

//         self
//     }
//     pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
//         info!("backend build started: {:?}", &self);
//         let root = self.root.unwrap();
//         info!("backend use root {}", root);
//         let bucket = match self.bucket.is_empty() {
//             false => Ok(&self.bucket),
//             true => Err(Error::Backend {
//                 kind: Kind::BackendConfigurationInvalid,
//                 context: HashMap::from([("bucket".to_string(), "".to_string())]),
//                 source: anyhow!("bucket is empty"),
//             }),
//         }?;
//         debug!("backend use bucket {}", &bucket);
//         let mut context: HashMap<String, String> =
//             HashMap::from([("bucket".to_string(), bucket.to_string())]);
//         let mut azure_storage_account = String::new();
//         let mut azure_storage_key = String::new();
//         if let Some(cred) = &self.credential {
//             context.insert("credential".to_string(), "*".to_string());
//             match cred {
//                 Credential::HMAC {
//                     access_key_id,
//                     secret_access_key,
//                 } => {
//                     azure_storage_account = access_key_id.to_string();
//                     azure_storage_key = secret_access_key.to_string();
//                 }
//                 // We don't need to do anything if user tries to read credential from env.
//                 Credential::Plain => {
//                     warn!("backend got empty credential, fallback to read from env.")
//                 }
//                 _ => {
//                     return Err(Error::Backend {
//                         kind: Kind::BackendConfigurationInvalid,
//                         context: context.clone(),
//                         source: anyhow!("credential is invalid"),
//                     });
//                 }
//             }
//         }
//         let http_client = azure_core::new_http_client();
//         let storage_client = StorageAccountClient::new_access_key(
//             http_client.clone(),
//             azure_storage_account,
//             azure_storage_key,
//         ).as_storage_client();
//         info!("backend build finished: {:?}", &self);
//         Ok(Arc::new(Backend {
//             root:root.unwrap().clone(),
//             bucket: self.bucket.clone(),
//             client: storage_client,
//         }))
//     }
// }
// #[derive(Debug, Clone)]
// pub struct Backend {
//     bucket: String,
//     client: Arc<StorageClient>,
//     root: String,
// }

// impl Backend {
//     pub fn build() -> Builder {
//         Builder::default()
//     }

//     pub(crate) fn inner(&self) -> Arc<StorageClient> {
//         self.client.clone()
//     }
//     pub(crate) fn normalize_path(path: &str) -> String {
//         let has_trailing = path.ends_with('/');

//         let mut p = path
//             .split('/')
//             .filter(|v| !v.is_empty())
//             .collect::<Vec<&str>>()
//             .join("/");

//         if has_trailing && !p.eq("/") {
//             p.push('/')
//         }

//         p
//     }
//     pub(crate) fn get_abs_path(&self, path: &str) -> String {
//         let path = Backend::normalize_path(path);
//         // root must be normalized like `/abc/`
//         format!("{}{}", self.root, path)
//             .trim_start_matches('/')
//             .to_string()
//     }
//     pub(crate) fn get_rel_path(&self, path: &str) -> String {
//         let path = format!("/{}", path);

//         match path.strip_prefix(&self.root) {
//             Some(v) => v.to_string(),
//             None => unreachable!(
//                 "invalid path {} that not start with backend root {}",
//                 &path, &self.root
//             ),
//         }
//     }
// }
// #[async_trait]
// impl Accessor for Backend {
//     async fn read(&self, args: &OpRead) -> Result<BoxedAsyncReader> {
//         increment_counter!("opendal_s3_read_requests");

//         let p = self.get_abs_path(&args.path);
//         info!(
//             "object {} read start: offset {:?}, size {:?}",
//             &p, args.offset, args.size
//         );

//         let mut blob_client = self
//             .client
//             .as_container_client(&self.bucket)
//             .as_blob_client(&p);

//         let resp =if  args.offset.is_some() || args.size.is_some() {
//             blob_client
//             .get()
//             .range(Range::new(0, 1024))
//             .execute()
//         .await.map_err(|e| {
//                 let e = parse_get_object_error(e, "read", &p);
//                 error!("object {} get_object: {:?}", &p, e);
//                 e
//         })?
//         } else{
//             blob_client
//             .get()
//             .execute()
//             .await.map_err(|e| {
//                 let e = parse_get_object_error(e, "read", &p);
//                 error!("object {} get_object: {:?}", &p, e);
//                 e
//             })?
//         };

//         info!(
//             "object {} reader created: offset {:?}, size {:?}",
//             &p, args.offset, args.size
//         );
//         // Ok(Box::new(S3ByteStream(resp).into_async_read()))
//         todo!()
//     }
//     async fn write(&self, r: BoxedAsyncReader, args: &OpWrite) -> Result<usize> {
//         increment_counter!("opendal_s3_stat_requests");
//         let p = self.get_abs_path(&args.path);
//         info!("object {} write start: size {}", &p, args.size);
//         let blob_client =self.client
//                 .as_container_client(&self.bucket)
//                 .as_blob_client(&p);
//         blob_client
//         .put_block_blob("tmp_data")
//         .content_type("text/plain")
//         .execute()
//         .await.map_err(|e| {
//             let e = parse_unexpect_error(e, "write", &p);
//             error!("object {} put_object: {:?}", &p, e);
//             e
//         })?;
//         Ok(args.size as usize)
//     }
//     async fn stat(&self, args: &OpStat) -> Result<Metadata> {
//         increment_counter!("opendal_s3_stat_requests");

//         let p = self.get_abs_path(&args.path);
//         info!("object {} stat start", &p);

//         let blob_client =self.client
//                                         .as_container_client(&self.bucket)
//                                         .as_blob_client(&p);
        
//         let response_result = blob_client.get().execute()
//         .await.map_err(|e| parse_head_object_error(e, "stat", &p));

//         match response_result {
//             Ok(response) => {
//                 let mut m = Metadata::default();
//                 m.set_path(&args.path);
//                 m.set_content_length(response.blob.properties.content_length);

//                 if p.ends_with('/') {
//                     m.set_mode(ObjectMode::DIR);
//                 } else {
//                     m.set_mode(ObjectMode::FILE);
//                 };

//                 m.set_complete();

//                 info!("object {} stat finished", &p);
//                 Ok(m)
//             }
//             // Always returns empty dir object if path is endswith "/" and we got an
//             // ObjectNotExist error.
//             Err(e) if (e.kind() == Kind::ObjectNotExist && p.ends_with('/')) => {
//                 let mut m = Metadata::default();
//                 m.set_path(&args.path);
//                 m.set_content_length(0);
//                 m.set_mode(ObjectMode::DIR);
//                 m.set_complete();

//                 info!("object {} stat finished", &p);
//                 Ok(m)
//             }
//             Err(e) => {
//                 error!("object {} head_object: {:?}", &p, e);
//                 Err(e)
//             }
//         }

//     }
//     async fn delete(&self, args: &OpDelete) -> Result<()> {
//         increment_counter!("opendal_s3_delete_requests");

//         let p = self.get_abs_path(&args.path);
//         info!("object {} delete start", &p);

//         let blob_client =
//         self.client
//         .as_container_client(&self.bucket)
//         .as_blob_client(&p);

//         let res = blob_client
//         .delete()
//         .delete_snapshots_method(DeleteSnapshotsMethod::Include)
//         .execute()
//         .await.map_err(|e| parse_unexpect_error(e, "delete", &p))?;

//         info!("object {} delete finished", &p);
//         Ok(())
//     }
//     async fn list(&self, args: &OpList) -> Result<BoxedObjectStream> {
//         increment_counter!("opendal_s3_list_requests");

//         let mut path = self.get_abs_path(&args.path);
//         // Make sure list path is endswith '/'
//         if !path.ends_with('/') && !path.is_empty() {
//             path.push('/')
//         }
//         info!("object {} list start", &path);
//         //prefix would look like that         .prefix("root/firstfolder/")
//         //if path have / as prefix, then remove it
//         if path.starts_with("/") {path =path[1..].to_string();}
//         let max_results =NonZeroU32::new(5u32).unwrap();
//         let container_client = self.client.as_container_client(&self.bucket);
//         let mut stream = Box::pin(
//             container_client
//                 .list_blobs()
//                 .prefix(path)
//                 .max_results(max_results)
//                 .stream());
//         todo!()
//     }
// }
