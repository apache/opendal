use futures::TryStreamExt;
use http::HeaderValue;

use std::collections::HashMap;

use crate::ops::HeaderRange;
use anyhow::anyhow;
use async_trait::async_trait;
use metrics::increment_counter;
use reqsign::services::azure::signer::Signer;
use reqwest::{Body, Response, Url};
use std::str::FromStr;

use crate::credential::Credential;
use crate::error::Error;
use crate::error::Kind;
use crate::error::Result;
use crate::object::BoxedObjectStream;
use crate::object::Metadata;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::BoxedAsyncReader;
use log::debug;
use log::error;
use log::info;
use log::warn;
pub const DELETE_SNAPSHOTS: &str = "x-ms-delete-snapshots";
use crate::readers::ReaderStream;
use std::sync::Arc;

#[derive(Default, Debug, Clone)]
pub struct Builder {
    root: Option<String>,
    bucket: String, // in Azure, bucket =  container
    credential: Option<Credential>,
    endpoint: Option<String>,
}

impl Builder {
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }
    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        self.bucket = bucket.to_string();

        self
    }
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = Some(endpoint.to_string());

        self
    }
    pub fn credential(&mut self, credential: Credential) -> &mut Self {
        self.credential = Some(credential);

        self
    }
    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        info!("backend build started: {:?}", &self);

        let root = match &self.root {
            // Use "/" as root if user not specified.
            None => "/".to_string(),
            Some(v) => {
                let mut v = Backend::normalize_path(v);
                if !v.starts_with('/') {
                    v.insert(0, '/');
                }
                if !v.ends_with('/') {
                    v.push('/')
                }
                v
            }
        };

        info!("backend use root {}", root);

        // Handle endpoint, region and bucket name.
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(Error::Backend {
                kind: Kind::BackendConfigurationInvalid,
                context: HashMap::from([("bucket".to_string(), "".to_string())]),
                source: anyhow!("bucket is empty"),
            }),
        }?;
        debug!("backend use bucket {}", &bucket);

        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => "blob.core.windows.net".to_string(),
        };

        debug!("backend use endpoint {} to detect region", &endpoint);

        let mut context: HashMap<String, String> = HashMap::from([
            ("endpoint".to_string(), endpoint.to_string()),
            ("bucket".to_string(), bucket.to_string()),
        ]);

        let mut access_name = String::new();
        let mut shared_key = String::new();
        if let Some(cred) = &self.credential {
            context.insert("credential".to_string(), "*".to_string());
            match cred {
                Credential::HMAC {
                    access_key_id,
                    secret_access_key,
                } => {
                    access_name = access_key_id.to_string();
                    shared_key = secret_access_key.to_string();
                }
                // We don't need to do anything if user tries to read credential from env.
                Credential::Plain => {
                    warn!("backend got empty credential, fallback to read from env.")
                }
                _ => {
                    return Err(Error::Backend {
                        kind: Kind::BackendConfigurationInvalid,
                        context: context.clone(),
                        source: anyhow!("credential is invalid"),
                    });
                }
            }
        }
        let client = reqwest::Client::new();

        let mut signer_builder = Signer::builder();
        signer_builder
            .access_name(&access_name)
            .shared_key(&shared_key);

        let signer = signer_builder.build().await?;

        info!("backend build finished: {:?}", &self);
        Ok(Arc::new(Backend {
            root: root,
            endpoint,
            signer: Arc::new(signer),
            bucket: self.bucket.clone(),
            client,
            access_name,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct Backend {
    bucket: String,
    client: reqwest::Client,
    root: String, // root will be "/" or /abc/
    endpoint: String,
    signer: Arc<Signer>,
    access_name: String,
}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }

    pub(crate) fn normalize_path(path: &str) -> String {
        let has_trailing = path.ends_with('/');

        let mut p = path
            .split('/')
            .filter(|v| !v.is_empty())
            .collect::<Vec<&str>>()
            .join("/");

        if has_trailing && !p.eq("/") {
            p.push('/')
        }

        p
    }
    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        let path = Backend::normalize_path(path);
        // root must be normalized like `/abc/`
        format!("{}{}", self.root, path)
            .trim_start_matches('/')
            .to_string()
    }
    #[warn(dead_code)]
    pub(crate) fn get_rel_path(&self, path: &str) -> String {
        let path = format!("/{}", path);

        match path.strip_prefix(&self.root) {
            Some(v) => v.to_string(),
            None => unreachable!(
                "invalid path {} that not start with backend root {}",
                &path, &self.root
            ),
        }
    }
}
#[async_trait]
impl Accessor for Backend {
    async fn read(&self, args: &OpRead) -> Result<BoxedAsyncReader> {
        increment_counter!("opendal_azure_read_requests");

        let p = self.get_abs_path(&args.path);
        info!(
            "object {} read start: offset {:?}, size {:?}",
            &p, args.offset, args.size
        );

        let resp = self.get_object(&p, args.offset, args.size).await?;

        info!(
            "object {} reader created: offset {:?}, size {:?}",
            &p, args.offset, args.size
        );
        Ok(Box::new(
            resp.bytes_stream()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .into_async_read(),
        ))
    }
    async fn write(&self, r: BoxedAsyncReader, args: &OpWrite) -> Result<usize> {
        let p = self.get_abs_path(&args.path);
        info!("object {} write start: size {}", &p, args.size);

        let resp = self.put_object(&p, r, args.size).await?;
        println!("resp :{resp:?}");
        match resp.status() {
            http::StatusCode::CREATED | http::StatusCode::OK => {
                info!("object {} write finished: size {:?}", &p, args.size);
                Ok(args.size as usize)
            }
            _ => Err(Error::Object {
                kind: Kind::Unexpected,
                op: "write",
                path: p.to_string(),
                source: anyhow!("{:?}", resp),
            }),
        }
    }
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        let _ = args;
        unimplemented!()
    }
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        increment_counter!("opendal_azure_delete_requests");

        let p = self.get_abs_path(&args.path);
        info!("object {} delete start", &p);

        let _ = self.delete_object(&p).await?;

        info!("object {} delete finished", &p);
        Ok(())
    }
    async fn list(&self, args: &OpList) -> Result<BoxedObjectStream> {
        let _ = args;
        unimplemented!()
    }
}

impl Backend {
    pub(crate) async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<Response> {
        let mut req = reqwest::Request::new(
            http::Method::GET,
            Url::from_str(&format!(
                "https://{}.{}/{}/{}",
                self.access_name, self.endpoint, self.bucket, path
            ))
            .expect("url must be valid"),
        );

        if offset.is_some() || size.is_some() {
            req.headers_mut().insert(
                http::header::RANGE,
                HeaderRange::new(offset, size)
                    .to_string()
                    .parse()
                    .expect("header must be valid"),
            );
        }

        self.signer.sign(&mut req).await.expect("sign must success");
        println!("req: {req:?}");
        let resp = self.client.execute(req).await.map_err(|e| {
            error!("object {} get_object: {:?}", path, e);
            Error::Unexpected(anyhow::Error::from(e))
        });
        println!("resp: {resp:?}");
        resp
    }
    pub(crate) async fn put_object(
        &self,
        path: &str,
        r: BoxedAsyncReader,
        size: u64,
    ) -> Result<Response> {
        // let hash = md5::compute(&data[..]).into();

        let mut req = reqwest::Request::new(
            http::Method::PUT,
            Url::from_str(&format!(
                "https://{}.{}/{}/{}",
                self.access_name, self.endpoint, self.bucket, path
            ))
            .expect("url must be valid"),
        );

        // Set content length.
        req.headers_mut().insert(
            http::header::CONTENT_LENGTH,
            size.to_string()
                .parse()
                .expect("content length must be valid"),
        );
        req.headers_mut().insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain"),
        );

        req.headers_mut()
            .insert("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));

        *req.body_mut() = Some(Body::from(hyper::body::Body::wrap_stream(
            ReaderStream::new(r),
        )));

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.execute(req).await.map_err(|e| {
            error!("object {} put_object: {:?}", path, e);
            Error::Unexpected(anyhow::Error::from(e))
        })
    }
    #[warn(dead_code)]
    pub(crate) async fn head_object(&self, path: &str) -> Result<Response> {
        let _ = path;
        unimplemented!()
    }

    pub(crate) async fn delete_object(&self, path: &str) -> Result<Response> {
        let mut req = reqwest::Request::new(
            http::Method::DELETE,
            Url::from_str(&format!(
                "https://{}.{}/{}/{}",
                self.access_name, self.endpoint, self.bucket, path
            ))
            .expect("url must be valid"),
        );

        req.headers_mut()
            .insert(DELETE_SNAPSHOTS, HeaderValue::from_static("include"));

        self.signer.sign(&mut req).await.expect("sign must success");
        println!("req: {req:?}");
        let resp = self.client.execute(req).await.map_err(|e| {
            error!("object {} delete_object: {:?}", path, e);
            Error::Unexpected(anyhow::Error::from(e))
        });
        println!("resp: {resp:?}");
        resp
    }
    #[warn(dead_code)]
    pub(crate) async fn list_object(
        &self,
        path: &str,
        continuation_token: &str,
    ) -> Result<Response> {
        let _ = path;
        let _ = continuation_token;
        unimplemented!()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::Operator;
    use futures::AsyncReadExt;
    use std::env;

    async fn azblob_new_access() -> Result<Arc<dyn Accessor>> {
        dotenv::from_filename(".env").ok();

        let root = &env::var("OPENDAL_AZBLOB_ROOT")
            .unwrap_or_else(|_| format!("/{}", uuid::Uuid::new_v4()));

        let mut builder = Backend::build();

        builder.root(root);
        builder.bucket(&env::var("OPENDAL_AZBLOB_BUCKET").expect("OPENDAL_AZBLOB_BUCKET must set"));
        builder.endpoint(&env::var("OPENDAL_AZBLOB_ENDPOINT").unwrap_or_default());
        builder.credential(Credential::hmac(
            &env::var("OPENDAL_AZBLOB_ACCESS_KEY_ID").unwrap_or_default(),
            &env::var("OPENDAL_AZBLOB_SECRET_ACCESS_KEY").unwrap_or_default(),
        ));

        let acc = builder.finish().await;

        acc
    }

    #[tokio::test]
    async fn test_write_read_delete() -> Result<()> {
        let acc = azblob_new_access().await?;

        let operator = Operator::new(acc);

        let path = "put_blob_object1";
        println!("Generate a file: {}", &path);

        //content of object is a vec of b'a' size = 1024
        let size: usize = 1024;
        let content = vec![97; size];
        // first step: write content to azblob
        let w = operator.object(&path).writer();
        let n = w.write_bytes(content.clone()).await?;
        assert_eq!(n, size, "write to azblob success");

        // // second step: read content from azblob
        // let mut buf = Vec::new();
        // let mut r = operator.object(&path).reader();
        // let n = r.read_to_end(&mut buf).await.expect("read to end");
        // assert_eq!(n, buf.len(), "read to azblob success");

        // // thrid setp: delete azblob
        // let _ = operator.object(&path).delete().await?;

        Ok(())
    }
}
