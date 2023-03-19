// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;
use std::sync::Arc;

use async_trait::async_trait;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use reqsign::GoogleSigner;
use serde::Deserialize;
use serde_json;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::error::parse_error;
use super::pager::GcsPager;
use super::uri::percent_encode_path;
use super::writer::GcsWriter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

const DEFAULT_GCS_ENDPOINT: &str = "https://storage.googleapis.com";
const DEFAULT_GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

/// Google Cloud Storage service.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] list
/// - [x] scan
/// - [ ] presign
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the work directory for backend
/// - `bucket`: Set the container name for backend
/// - `endpoint`: Customizable endpoint setting
/// - `credentials`: Credential string for GCS OAuth2
///
/// You can refer to [`GcsBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Gcs;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // create backend builder
///     let mut builder = Gcs::default();
///
///     // set the storage bucket for OpenDAL
///     builder.bucket("test");
///     // set the working directory root for GCS
///     // all operations will happen within it
///     builder.root("/path/to/dir");
///     // set the credentials for GCS OAUTH2 authentication
///     builder.credential("authentication token");
///
///     let op: Operator = Operator::new(builder)?.finish();
///     Ok(())
/// }
/// ```
#[derive(Clone, Default)]
pub struct GcsBuilder {
    /// root URI, all operations happens under `root`
    root: Option<String>,
    /// bucket name
    bucket: String,
    /// endpoint URI of GCS service,
    /// default is `https://storage.googleapis.com`
    endpoint: Option<String>,
    /// Scope for gcs.
    scope: Option<String>,
    /// Service Account for gcs.
    service_account: Option<String>,

    /// credential string for GCS service
    credential: Option<String>,
    /// credential path for GCS service.
    credential_path: Option<String>,

    http_client: Option<HttpClient>,
    signer: Option<Arc<GoogleSigner>>,
}

impl GcsBuilder {
    /// set the working directory root of backend
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string())
        }

        self
    }

    /// set the container's name
    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        self.bucket = bucket.to_string();
        self
    }

    /// set the GCS service scope
    ///
    /// If not set, we will use `https://www.googleapis.com/auth/devstorage.read_write`.
    ///
    /// # Valid scope examples
    ///
    /// - read-only: `https://www.googleapis.com/auth/devstorage.read_only`
    /// - read-write: `https://www.googleapis.com/auth/devstorage.read_write`
    /// - full-control: `https://www.googleapis.com/auth/devstorage.full_control`
    ///
    /// Reference: [Cloud Storage authentication](https://cloud.google.com/storage/docs/authentication)
    pub fn scope(&mut self, scope: &str) -> &mut Self {
        if !scope.is_empty() {
            self.scope = Some(scope.to_string())
        };
        self
    }

    /// Set the GCS service account.
    ///
    /// service account will be used for fetch token from vm metadata.
    /// If not set, we will try to fetch with `default` service account.
    pub fn service_account(&mut self, service_account: &str) -> &mut Self {
        if !service_account.is_empty() {
            self.service_account = Some(service_account.to_string())
        };
        self
    }

    /// set the endpoint GCS service uses
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            self.endpoint = Some(endpoint.to_string())
        };
        self
    }

    /// set the base64 hashed credentials string used for OAuth2
    pub fn credential(&mut self, credential: &str) -> &mut Self {
        if !credential.is_empty() {
            self.credential = Some(credential.to_string())
        };
        self
    }

    /// set the credentials path of GCS.
    pub fn credential_path(&mut self, path: &str) -> &mut Self {
        if !path.is_empty() {
            self.credential_path = Some(path.to_string())
        };
        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(&mut self, client: HttpClient) -> &mut Self {
        self.http_client = Some(client);
        self
    }

    /// Specify the signer directly instead of building by OpenDAL.
    ///
    /// If signer is specified, the following settings will not be used
    /// any more:
    ///
    /// - `scope`
    /// - `service_account`
    /// - `credential`
    /// - `credential_path`
    ///
    /// PLEASE USE THIS API CAREFULLY.
    pub fn signer(&mut self, signer: GoogleSigner) -> &mut Self {
        self.signer = Some(Arc::new(signer));
        self
    }
}

impl Debug for GcsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint);
        if self.credential.is_some() {
            ds.field("credentials", &"<redacted>");
        }
        ds.finish()
    }
}

impl Builder for GcsBuilder {
    const SCHEME: Scheme = Scheme::Gcs;
    type Accessor = GcsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = GcsBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("bucket").map(|v| builder.bucket(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("credential").map(|v| builder.credential(v));
        map.get("scope").map(|v| builder.scope(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        // Handle endpoint and bucket name
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::Gcs),
            ),
        }?;

        // TODO: server side encryption

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Gcs)
            })?
        };

        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| DEFAULT_GCS_ENDPOINT.to_string());

        debug!("backend use endpoint: {endpoint}");

        let signer = if let Some(signer) = &self.signer {
            signer.clone()
        } else {
            // build signer
            let mut signer_builder = GoogleSigner::builder();
            if let Some(scope) = &self.scope {
                signer_builder.scope(scope);
            } else {
                signer_builder.scope(DEFAULT_GCS_SCOPE);
            }
            if let Some(account) = &self.service_account {
                signer_builder.service_account(account);
            }
            if let Some(cred) = &self.credential {
                signer_builder.credential_content(cred);
            }
            if let Some(cred) = &self.credential_path {
                signer_builder.credential_path(cred);
            }
            let signer = signer_builder.build().map_err(|e| {
                Error::new(ErrorKind::ConfigInvalid, "build GoogleSigner")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::Gcs)
                    .with_context("bucket", bucket)
                    .with_context("endpoint", &endpoint)
                    .set_source(e)
            })?;
            Arc::new(signer)
        };

        let backend = GcsBackend {
            root,
            endpoint,
            bucket: bucket.clone(),
            signer,
            client,
        };

        Ok(backend)
    }
}

/// GCS storage backend
#[derive(Clone)]
pub struct GcsBackend {
    endpoint: String,
    bucket: String,
    // root should end with "/"
    root: String,

    pub client: HttpClient,
    pub signer: Arc<GoogleSigner>,
}

impl Debug for GcsBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("Backend");
        de.field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("client", &self.client)
            .field("signer", &"<redacted>")
            .finish()
    }
}

#[async_trait]
impl Accessor for GcsBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = GcsWriter;
    type BlockingWriter = ();
    type Pager = GcsPager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        use AccessorCapability::*;
        use AccessorHint::*;

        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Gcs)
            .set_root(&self.root)
            .set_name(&self.bucket)
            .set_capabilities(Read | Write | List | Scan)
            .set_hints(ReadStreamable);
        am
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let mut req = self.gcs_insert_object_request(path, Some(0), None, AsyncBody::Empty)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        let resp = self.client.send_async(req).await?;

        if resp.status().is_success() {
            resp.into_body().consume().await?;
            Ok(RpCreate::default())
        } else {
            Err(parse_error(resp).await?)
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.gcs_get_object(path, args.range()).await?;

        if resp.status().is_success() {
            let meta = parse_into_metadata(path, resp.headers())?;
            Ok((RpRead::with_metadata(meta), resp.into_body()))
        } else {
            Err(parse_error(resp).await?)
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.append() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "append write is not supported",
            ));
        }

        Ok((
            RpWrite::default(),
            GcsWriter::new(self.clone(), args, path.to_string()),
        ))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.gcs_get_object_metadata(path).await?;

        if resp.status().is_success() {
            // read http response body
            let slc = resp.into_body().bytes().await?;
            let meta: GetObjectJsonResponse =
                serde_json::from_slice(&slc).map_err(new_json_deserialize_error)?;

            let mode = if path.ends_with('/') {
                EntryMode::DIR
            } else {
                EntryMode::FILE
            };
            let mut m = Metadata::new(mode);

            m.set_etag(&meta.etag);
            m.set_content_md5(&meta.md5_hash);

            let size = meta
                .size
                .parse::<u64>()
                .map_err(|e| Error::new(ErrorKind::Unexpected, "parse u64").set_source(e))?;
            m.set_content_length(size);
            if !meta.content_type.is_empty() {
                m.set_content_type(&meta.content_type);
            }

            let datetime = OffsetDateTime::parse(&meta.updated, &Rfc3339).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "parse date time with rfc 3339").set_source(e)
            })?;
            m.set_last_modified(datetime);

            Ok(RpStat::new(m))
        } else if resp.status() == StatusCode::NOT_FOUND && path.ends_with('/') {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            Err(parse_error(resp).await?)
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.gcs_delete_object(path).await?;

        // deleting not existing objects is ok
        if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
            Ok(RpDelete::default())
        } else {
            Err(parse_error(resp).await?)
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        Ok((
            RpList::default(),
            GcsPager::new(Arc::new(self.clone()), &self.root, path, "/", args.limit()),
        ))
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        Ok((
            RpScan::default(),
            GcsPager::new(Arc::new(self.clone()), &self.root, path, "", args.limit()),
        ))
    }
}

impl GcsBackend {
    fn gcs_get_object_request(&self, path: &str, range: BytesRange) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/storage/v1/b/{}/o/{}?alt=media",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header());
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    async fn gcs_get_object(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.gcs_get_object_request(path, range)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    pub fn gcs_insert_object_request(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        let mut req = Request::post(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        if let Some(mime) = content_type {
            req = req.header(CONTENT_TYPE, mime)
        }

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    async fn gcs_get_object_metadata(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        let req = Request::get(&url);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    async fn gcs_delete_object(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        let mut req = Request::delete(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    pub(crate) async fn gcs_list_objects(
        &self,
        path: &str,
        page_token: &str,
        delimiter: &str,
        limit: Option<usize>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/storage/v1/b/{}/o?prefix={}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );
        if !delimiter.is_empty() {
            write!(url, "&delimiter={delimiter}").expect("write into string must succeed");
        }
        if let Some(limit) = limit {
            write!(url, "&maxResults={limit}").expect("write into string must succeed");
        }
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

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }
}

/// The raw json response returned by [`get`](https://cloud.google.com/storage/docs/json_api/v1/objects/get)
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct GetObjectJsonResponse {
    /// GCS will return size in string.
    ///
    /// For example: `"size": "56535"`
    size: String,
    /// etag is not quoted.
    ///
    /// For example: `"etag": "CKWasoTgyPkCEAE="`
    etag: String,
    /// RFC3339 styled datetime string.
    ///
    /// For example: `"updated": "2022-08-15T11:33:34.866Z"`
    updated: String,
    /// Content md5 hash
    ///
    /// For example: `"md5Hash": "fHcEH1vPwA6eTPqxuasXcg=="`
    md5_hash: String,
    /// Content type of this object.
    ///
    /// For example: `"contentType": "image/png",`
    content_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_get_object_json_response() {
        let content = r#"{
  "kind": "storage#object",
  "id": "example/1.png/1660563214863653",
  "selfLink": "https://www.googleapis.com/storage/v1/b/example/o/1.png",
  "mediaLink": "https://content-storage.googleapis.com/download/storage/v1/b/example/o/1.png?generation=1660563214863653&alt=media",
  "name": "1.png",
  "bucket": "example",
  "generation": "1660563214863653",
  "metageneration": "1",
  "contentType": "image/png",
  "storageClass": "STANDARD",
  "size": "56535",
  "md5Hash": "fHcEH1vPwA6eTPqxuasXcg==",
  "crc32c": "j/un9g==",
  "etag": "CKWasoTgyPkCEAE=",
  "timeCreated": "2022-08-15T11:33:34.866Z",
  "updated": "2022-08-15T11:33:34.866Z",
  "timeStorageClassUpdated": "2022-08-15T11:33:34.866Z"
}"#;

        let meta: GetObjectJsonResponse =
            serde_json::from_str(content).expect("json Deserialize must succeed");

        assert_eq!(meta.size, "56535");
        assert_eq!(meta.updated, "2022-08-15T11:33:34.866Z");
        assert_eq!(meta.md5_hash, "fHcEH1vPwA6eTPqxuasXcg==");
        assert_eq!(meta.etag, "CKWasoTgyPkCEAE=");
        assert_eq!(meta.content_type, "image/png");
    }
}
