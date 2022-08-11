use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

use anyhow::anyhow;
use http::StatusCode;
use isahc::AsyncBody;
use log::{debug, error, info, warn};
use metrics::increment_counter;
use reqsign::services::google::Signer;

use crate::accessor::AccessorCapability;
use crate::error::{other, BackendError, ObjectError};
use crate::http_util::{
    new_http_channel, parse_content_length, parse_error_kind, parse_error_response,
    parse_error_status_code, parse_etag, parse_last_modified, percent_encode_path, HttpBodyWriter,
    HttpClient,
};
use crate::ops::{
    BytesRange, OpCreate, OpDelete, OpList, OpPresign, OpRead, OpStat, OpWrite, Operation,
    PresignedRequest,
};
use crate::services::gcs::dir_stream::DirStream;
use crate::AccessorMetadata;
use crate::{Accessor, BytesReader, BytesWriter, DirStreamer, ObjectMetadata, ObjectMode, Scheme};

const DEFAULT_GCS_ENDPOINT: &'static str = "https://storage.googleapis.com";
const DEFAULT_GCS_AUTH: &'static str = "https://www.googleapis.com/auth";

// TODO: Server side encryption support

/// GCS storage backend builder
#[derive(Clone)]
pub struct Builder {
    /// root URI, all operations happens under `root`
    root: Option<String>,
    /// bucket name
    bucket: String,
    /// endpoint URI of GCS service,
    /// default is "https://storage.googleapis.com"
    endpoint: Option<String>,

    /// credential string for GCS service
    credentials: Option<String>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            root: None,
            bucket: "".to_string(),
            endpoint: None,
            credentials: None,
        }
    }
}

impl Builder {
    pub fn root(&mut self, root: &str) -> &mut Self {
        if root.is_empty() {
            self.root = None;
        } else {
            self.root = Some(root.to_string());
        }
        self
    }

    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        self.bucket = bucket.to_string();
        self
    }

    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };
        self
    }

    /// Establish connection to GCS and finish making GCS backend
    pub fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        log::info!("backend build started: {:?}", self);

        let root = match &self.root {
            None => "/".to_string(),
            Some(v) => {
                // remove successive '/'s
                let mut v = v
                    .split('/')
                    .filter(|s: &str| !s.is_empty())
                    .collect::<Vec<&str>>()
                    .join("/");
                // path should start with '/'
                v.insert(0, '/');

                // path should end with '/'
                if !v.ends_with('/') {
                    v.push('/');
                }
                v
            }
        };

        info!("backend use root: {}", &root);

        // Handle endpoint and bucket name
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(other(BackendError::new(
                HashMap::from([("bucket".to_string(), "".to_string())]),
                anyhow!("bucket name is empty"),
            ))),
        }?;

        // setup error context
        let mut cxt = HashMap::from([("bucket".to_string(), bucket.to_string())]);

        // TODO: server side encryption

        // build http client
        let client = HttpClient::new();
        let endpoint = self.endpoint.unwrap_or(DEFAULT_GCS_ENDPOINT.to_string());
        cxt.insert("endpoint".to_string(), endpoint);
        log::debug!("backend use endpoint: {}", endpoint);

        // build signer
        let auth_url = [
            DEFAULT_GCS_AUTH.to_string(),
            bucket,
            "read_write".to_string(),
        ]
        .join("/");
        let mut signer_builder = Signer::builder();
        signer_builder.scope(auth_url.as_str());
        if let Some(cred) = &self.credentials {
            signer_builder.credential_from_content(cred.as_str());
        }
        let signer = signer_builder
            .build()
            .map_err(|e| other(BackendError::new(ctx, e)))?;
        let signer = Arc::new(signer);

        let backend = Backend {
            root,
            endpoint: endpoint.clone(),
            bucket: bucket.clone(),
            signer,
            client,
        };

        Ok(Arc::new(Backend))
    }
}
impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint);
        if self.credentials.is_some() {
            ds.field("credentials", &"<redacted>");
        }
        ds.finish()
    }
}
/// GCS storage backend
#[derive(Debug, Clone)]
pub struct Backend {
    endpoint: String,
    bucket: String,
    // root should end with "/"
    root: String,

    client: HttpClient,
    signer: Arc<Signer>,
}

impl Backend {
    pub fn builder() -> Builder {
        Builder::default()
    }

    // normalized paths, relative path -> absolute path
    pub fn get_abs_path(&self, path: &str) -> String {
        if path == "/" {
            return self.root.trim_start_matches('/').to_string();
        }

        format!("{}{}", self.root, path)
            .trim_start_matches('/')
            .to_string()
    }

    // absolute path -> relative path
    pub fn get_rel_path(&self, path: &str) -> String {
        let path = format!("/{}", path);

        match path.strip_prefix(&self.root) {
            Some(p) => p.to_string(),
            None => unreachable!(
                "invalid path {} that not start with backend root {}",
                &path, &self.root
            ),
        }
    }
}

impl Backend {
    pub(crate) fn get_object_request(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<isahc::Request<isahc::AsyncBody>> {
        let url = format!(
            "{}/storage/v1/b/{}/o/{}?alt=media",
            self.endpoint,
            self.bucket,
            percent_encode_path(path)
        );

        let mut req = isahc::Request::get(&url);

        if offset.is_some() || size.is_some() {
            req = req.header(
                http::header::RANGE,
                BytesRange::new(offset, size).to_string(),
            );
        }

        let req = req.body(isahc::AsyncBody::empty()).map_err(|e| {
            log::error!("object {path} get_object: {url} {e:?}");
            other(ObjectError::new(
                "read",
                path,
                anyhow!("build request {url}: {e:?}"),
            ))
        })?;

        Ok(req)
    }

    #[trace("get_object")]
    pub(crate) async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<isahc::Response<isahc::AsyncBody>> {
        let mut req = self.get_object_request(path, offset, size)?;
        let url = req.uri().to_string();

        self.signer.sign(&mut req).map_err(|e| {
            error!("object {path} get_object: {url} {e:?}");
            other(ObjectError::new(
                "read",
                path,
                anyhow!("sign request: {url}: {e:?}"),
            ))
        })?;

        self.client.send_async(req).await.map_err(|e| {
            error!("object {path} get_object: {url} {e:?}");

            Error::new(
                parse_error_kind(&e),
                ObjectError::new("read", path, anyhow!("send request: {url}: {e:?}")),
            )
        })
    }

    pub(crate) fn insert_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        body: isahc::AsyncBody,
    ) -> Result<isahc::Request<isahc::AsyncBody>> {
        let url = format!(
            "{}/upload/storage/b/{}/o?name={}",
            self.endpoint,
            self.bucket,
            percent_encode_path(path)
        );

        let mut req = isahc::Request::post(&url);

        // Set content length.
        if let Some(size) = size {
            req = req.header(http::header::CONTENT_LENGTH, size.to_string());
        }

        // Set body
        let req = req.body(body).map_err(|e| {
            error!("object {path} put_object: {url} {e:?}");
            other(ObjectError::new(
                "write",
                path,
                anyhow!("build request {url}: {e:?}"),
            ))
        })?;

        Ok(req)
    }

    #[trace("insert_object")]
    pub(crate) async fn insert_object(
        &self,
        path: &str,
        size: u64,
        body: isahc::AsyncBody,
    ) -> Result<isahc::Request<isahc::AsyncBody>> {
        let mut req = self.insert_object_request(path, Some(size), body)?;
        let url = req.uri().to_string();

        self.signer.sign(&mut req).map_err(|e| {
            error!("object {path} insert_object: {url} {e:?}");
            other(ObjectError::new(
                "write",
                path,
                anyhow!("sign request: {url}: {e:?}"),
            ))
        })?;

        Ok(req)
    }

    #[trace("get_object_metadata")]
    pub(crate) async fn get_object_metadata(
        &self,
        path: &str,
    ) -> Result<isahc::Response<isahc::AsyncBody>> {
        let url = format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            self.bucket,
            percent_encode_path(path)
        );

        let mut req = isahc::Request::get(&url);

        let mut req = req.body(isahc::AsyncBody::empty()).map_err(|e| {
            error!("object {path} get_object_metadata: {url} {e:?}");
            other(ObjectError::new(
                "stat",
                path,
                anyhow!("build request {url}: {e:?}"),
            ))
        })?;

        self.signer.sign(&mut req).map_err(|e| {
            error!("object {path} get_object_metadata: {url} {e:?}");
            other(ObjectError::new(
                "stat",
                path,
                anyhow!("sign request: {url}: {e:?}"),
            ))
        })?;

        self.client.send_async(req).await.map_err(|e| {
            error!("object {path} get_object_metadata: {url} {e:?}");

            Error::new(
                parse_error_kind(&e),
                ObjectError::new("stat", path, anyhow!("send request: {url}: {e:?}")),
            )
        })
    }

    #[trace("delete_object")]
    pub(crate) async fn delete_object(&self, path: &str) -> Result<isahc::Response<AsyncBody>> {
        let url = format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            self.bucket,
            percent_encode_path(path)
        );

        let mut req = isahc::Request::delete(&url)
            .body(AsyncBody::empty())
            .map_err(|e| {
                error!("object {path} delete_object: {url} {e:?}");
                other(ObjectError::new(
                    "delete",
                    path,
                    anyhow!("build request {url}: {e:?}"),
                ))
            })?;

        self.signer.sign(&mut req).map_err(|e| {
            error!("object {path} delete_object: {url} {e:?}");
            other(ObjectError::new(
                "delete",
                path,
                anyhow!("sign request {url}: {e:?}"),
            ))
        })?;

        self.client.send_async(req).await.map_err(|e| {
            error!("object {path} delete_object: {url} {e:?}");

            Error::new(
                parse_error_kind(&e),
                ObjectError::new("delete", path, anyhow!("send request: {url}: {e:?}")),
            )
        })
    }

    #[trace("list_objects")]
    pub(crate) async fn list_objects(
        &self,
        path: &str,
        page_token: &str,
    ) -> Result<isahc::Response<AsyncBody>> {
        let mut url = format!(
            "{}/storage/v1/b/{}/o?delimiter=/&prefix={}",
            self.endpoint,
            self.bucket,
            percent_encode_path(path)
        );
        if !page_token.is_empty() {
            // NOTE:
            //
            // GCS uses pageToken in request and nextPageToken in response
            //
            // Don't know how will those tokens be like so this part are copied
            // directly from AWS S3 service.
            write!(url, "&pageToken={}", percent_encode_path(page_token))
                .expect("write into string must succeed");
        }

        let mut req = isahc::Request::get(&url)
            .body(AsyncBody::empty())
            .map_err(|e| {
                error!("object {path} list_objects: {url} {e:?}");
                other(ObjectError::new(
                    "list",
                    path,
                    anyhow!("build request {url}: {e:?}"),
                ))
            })?;

        self.signer.sign(&mut req).map_err(|e| {
            error!("object {path} list_objects: {url} {e:?}");
            other(ObjectError::new(
                "list",
                path,
                anyhow!("sign request {url}: {e:?}"),
            ))
        })?;

        self.client.send_async(req).await.map_err(|e| {
            error!("object {path} list_object: {url} {e:?}");

            Error::new(
                parse_error_kind(&e),
                ObjectError::new("list", path, anyhow!("send request: {url}: {e:?}")),
            )
        })
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Gcs)
            .set_root(&self.root)
            .set_name(&self.bucket)
            .set_capabilities(AccessorCapability::Presign);
        am
    }

    #[trace("create")]
    async fn create(&self, args: &OpCreate) -> Result<()> {
        increment_counter!("opendal_gcs_read_request");

        let p = self.get_abs_path(args.path());
        let req = self
            .insert_object(p.as_str(), 0, AsyncBody::from_bytes_static(b""))
            .await?;
        let resp = self.client.send_async(req).await.map_err(|e| {
            error!("object {} insert_object: {:?}", p, e);
            Error::new(
                parse_error_kind(&e),
                ObjectError::new("create", args.path(), anyhow!("send request: {e:?}")),
            )
        })?;

        match resp.status() {
            // 2xx is all ok
            StatusCode(num) if 200 <= num && num < 300 => {
                debug!("object {} create finished", args.path());
                Ok(())
            }
            _ => {
                let err =
                    parse_error_response("create", args.path(), parse_error_status_code, resp)
                        .await;
                warn!("object {} create: {:?}", args.path(), err);
                Err(err)
            }
        }
    }
    #[trace("read")]
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        increment_counter!("opendal_gcs_read_requests");

        let p = self.get_abs_path(args.path());
        debug!(
            "object {} read start: offset {:?}, size {:?}",
            &p,
            args.offset(),
            args.size()
        );

        let resp = self
            .get_object(&p, args.offset(), args.size())
            .await
            .map_err(|e| {
                error!("object {} get_object: {:?}", p, e);
                e
            })?;

        match resp.status() {
            StatusCode(x) if 200 <= x && x < 300 => {
                debug!(
                    "object {} reader created: offset {:?}, size {:?}",
                    &p,
                    args.offset(),
                    args.size()
                );

                Ok(Box::new(resp.into_body()))
            }
            _ => {
                let err =
                    parse_error_response("read", args.path(), parse_error_status_code, resp).await;
                warn!("object {} read: {:?}", args.path(), err);
                Err(err)
            }
        }
    }

    #[trace("write")]
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        let p = self.get_abs_path(args.path());
        debug!("object {} write start: size {}", &p, args.size());

        let (tx, body) = new_http_channel(args.size());

        let req = self.insert_object(&p, args.size(), body).await?;

        let bs = HttpBodyWriter::new(
            args,
            tx,
            self.client.send_async(req),
            HashSet::from([StatusCode::CREATED, StatusCode::OK]),
            parse_error_status_code,
        );

        Ok(Box::new(bs))
    }

    #[trace("stat")]
    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        increment_counter!("opendal_gcs_stat_requests");

        let p = self.get_abs_path(args.path());
        debug!("object {} stat start", &p);

        // Stat root always returns a DIR.
        if self.get_rel_path(&p).is_empty() {
            let mut m = ObjectMetadata::default();
            m.set_mode(ObjectMode::DIR);

            debug!("backed root object stat finished");
            return Ok(m);
        }

        let resp = self.get_object_metadata(&p).await?;

        match resp.status() {
            StatusCode(code) if 200 <= code && code < 300 => {
                let mut m = ObjectMetadata::default();

                if let Some(v) = parse_content_length(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_content_length(v);
                }

                if let Some(v) = parse_etag(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_etag(v);
                    m.set_content_md5(v.trim_matches('"'));
                }

                if let Some(v) = parse_last_modified(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_last_modified(v);
                }

                if p.ends_with('/') {
                    m.set_mode(ObjectMode::DIR);
                } else {
                    m.set_mode(ObjectMode::FILE);
                };

                debug!("object {} stat finished: {:?}", &p, m);
                Ok(m)
            }
            StatusCode::NOT_FOUND if p.ends_with('/') => {
                let mut m = ObjectMetadata::default();
                m.set_mode(ObjectMode::DIR);

                debug!("object {} stat finished", &p);
                Ok(m)
            }
            _ => {
                let err =
                    parse_error_response("stat", args.path(), parse_error_status_code, resp).await;
                warn!("object {} stat: {:?}", args.path(), err);
                Err(err)
            }
        }
    }

    #[trace("delete")]
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        increment_counter!("opendal_gcs_delete_requests");

        let p = self.get_abs_path(args.path());
        debug!("object {} delete start", &p);

        let resp = self.delete_object(&p).await?;

        match resp.status() {
            StatusCode(code) if 200 <= code && code < 300 => {
                debug!("object {} delete finished", &p);
                Ok(())
            }
            _ => {
                let err =
                    parse_error_response("delete", args.path(), parse_error_status_code, resp)
                        .await;
                warn!("object {} delete: {:?}", args.path(), err);
                Err(err)
            }
        }
    }

    #[trace("list")]
    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        increment_counter!("opendal_gcs_list_requests");

        let mut path = self.get_abs_path(args.path());
        // Make sure list path is endswith '/'
        if !path.ends_with('/') && !path.is_empty() {
            path.push('/')
        }
        debug!("object {} list start", &path);

        Ok(Box::new(DirStream::new(Arc::new(self.clone()), &path)))
    }

    fn presign(&self, args: &OpPresign) -> Result<PresignedRequest> {
        increment_counter!("opendal_gcs_presign_requests");

        let path = self.get_abs_path(args.path());

        // We will not send this request out, just for signing.
        let mut req = match args.operation() {
            Operation::Read => self.get_object_request(&path, None, None)?,
            Operation::Write => {
                self.insert_object_request(&path, None, isahc::AsyncBody::empty())?
            }
            op => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    ObjectError::new(
                        "presign",
                        &path,
                        anyhow!("presign for {op} is not supported"),
                    ),
                ))
            }
        };
        let url = req.uri().to_string();

        self.signer
            .sign_query(&mut req, args.expire())
            .map_err(|e| {
                error!("object {path} presign: {url} {e:?}");
                other(ObjectError::new(
                    "presign",
                    &path,
                    anyhow!("sign request: {url}: {e:?}"),
                ))
            })?;

        // We don't need this request anymore, consume it directly.
        let (parts, _) = req.into_parts();

        Ok(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        ))
    }
}

// TODO: Add tests for GCS backend
