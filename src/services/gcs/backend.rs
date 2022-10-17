// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;
use std::io::Result;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use log::info;
use reqsign::GoogleSigner;
use serde::Deserialize;
use serde_json;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::dir_stream::DirStream;
use super::error::parse_error;
use super::uri::percent_encode_path;
use crate::accessor::AccessorCapability;
use crate::error::new_other_backend_error;
use crate::error::new_other_object_error;
use crate::http_util::new_request_build_error;
use crate::http_util::new_request_send_error;
use crate::http_util::new_request_sign_error;
use crate::http_util::new_response_consume_error;
use crate::http_util::parse_error_response;
use crate::http_util::AsyncBody;
use crate::http_util::HttpClient;
use crate::http_util::IncomingAsyncBody;
use crate::object::ObjectPageStreamer;
use crate::ops::BytesRange;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
use crate::path::build_abs_path;
use crate::path::normalize_root;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::ObjectStreamer;
use crate::Scheme;

const DEFAULT_GCS_ENDPOINT: &str = "https://storage.googleapis.com";
const DEFAULT_GCS_AUTH: &str = "https://www.googleapis.com/auth/devstorage.read_write";

// TODO: Server side encryption support

/// GCS storage backend builder
#[derive(Clone, Default)]
pub struct Builder {
    /// root URI, all operations happens under `root`
    root: Option<String>,
    /// bucket name
    bucket: String,
    /// endpoint URI of GCS service,
    /// default is `https://storage.googleapis.com`
    endpoint: Option<String>,

    /// credential string for GCS service
    credential: Option<String>,
}

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();
        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "bucket" => builder.bucket(v),
                "endpoint" => builder.endpoint(v),
                "credential" => builder.credential(v),
                _ => continue,
            };
        }
        builder
    }

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

    /// Establish connection to GCS and finish making GCS backend
    pub fn build(&mut self) -> Result<impl Accessor> {
        info!("backend build started: {:?}", self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        info!("backend use root {}", root);

        // Handle endpoint and bucket name
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(new_other_backend_error(
                HashMap::from([("bucket".to_string(), "".to_string())]),
                anyhow!("bucket name is empty"),
            )),
        }?;

        // setup error context
        let mut ctx = HashMap::from([("bucket".to_string(), bucket.to_string())]);

        // TODO: server side encryption

        // build http client
        let client = HttpClient::new();
        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| DEFAULT_GCS_ENDPOINT.to_string());
        ctx.insert("endpoint".to_string(), endpoint.clone());

        debug!("backend use endpoint: {endpoint}");

        // build signer
        let auth_url = DEFAULT_GCS_AUTH.to_string();
        let mut signer_builder = GoogleSigner::builder();
        signer_builder.scope(&auth_url);
        if let Some(cred) = &self.credential {
            signer_builder.credential_from_content(cred);
        }
        let signer = signer_builder
            .build()
            .map_err(|e| new_other_backend_error(ctx, e))?;
        let signer = Arc::new(signer);

        let backend = Backend {
            root,
            endpoint,
            bucket: bucket.clone(),
            signer,
            client,
        };

        Ok(backend)
    }
}

impl Debug for Builder {
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

/// GCS storage backend
#[derive(Clone)]
pub struct Backend {
    endpoint: String,
    bucket: String,
    // root should end with "/"
    root: String,

    client: HttpClient,
    signer: Arc<GoogleSigner>,
}

impl Debug for Backend {
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
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Gcs)
            .set_root(&self.root)
            .set_name(&self.bucket)
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            );
        am
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<()> {
        let mut req = self.gcs_insert_object_request(path, Some(0), None, AsyncBody::Empty)?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error(Operation::Create, path, e))?;

        let resp = self
            .client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Create, path, e))?;

        if resp.status().is_success() {
            resp.into_body()
                .consume()
                .await
                .map_err(|err| new_response_consume_error(Operation::Create, path, err))?;
            Ok(())
        } else {
            let er = parse_error_response(resp).await?;
            let e = parse_error(Operation::Create, path, er);
            Err(e)
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        let resp = self
            .gcs_get_object(path, args.offset(), args.size())
            .await?;

        if resp.status().is_success() {
            Ok(resp.into_body().reader())
        } else {
            let er = parse_error_response(resp).await?;
            let e = parse_error(Operation::Read, path, er);
            Err(e)
        }
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        let mut req = self.gcs_insert_object_request(
            path,
            Some(args.size()),
            Some(args.mime_type()),
            AsyncBody::Reader(r),
        )?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error(Operation::Write, path, e))?;

        let resp = self
            .client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Write, path, e))?;

        if (200..300).contains(&resp.status().as_u16()) {
            resp.into_body()
                .consume()
                .await
                .map_err(|err| new_response_consume_error(Operation::Write, path, err))?;
            Ok(args.size())
        } else {
            let er = parse_error_response(resp).await?;
            let err = parse_error(Operation::Write, path, er);
            Err(err)
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(ObjectMetadata::new(ObjectMode::DIR));
        }

        let resp = self.gcs_get_object_metadata(path).await?;

        if resp.status().is_success() {
            // read http response body
            let slc = resp.into_body().bytes().await.map_err(|e| {
                new_other_object_error(Operation::Stat, path, anyhow!("read response body: {e:?}"))
            })?;
            let meta: GetObjectJsonResponse = serde_json::from_slice(&slc).map_err(|e| {
                new_other_object_error(
                    Operation::Stat,
                    path,
                    anyhow!("parse response body into JSON: {e:?}"),
                )
            })?;

            let mode = if path.ends_with('/') {
                ObjectMode::DIR
            } else {
                ObjectMode::FILE
            };
            let mut m = ObjectMetadata::new(mode);

            m.set_etag(&meta.etag);
            m.set_content_md5(&meta.md5_hash);

            let size = meta.size.parse::<u64>().map_err(|e| {
                new_other_object_error(Operation::Stat, path, anyhow!("parse object size: {e:?}"))
            })?;
            m.set_content_length(size);

            let datetime = OffsetDateTime::parse(&meta.updated, &Rfc3339).map_err(|e| {
                new_other_object_error(
                    Operation::Stat,
                    path,
                    anyhow!("parse object updated: {e:?}"),
                )
            })?;
            m.set_last_modified(datetime);

            Ok(m)
        } else if resp.status() == StatusCode::NOT_FOUND && path.ends_with('/') {
            Ok(ObjectMetadata::new(ObjectMode::DIR))
        } else {
            let er = parse_error_response(resp).await?;
            let e = parse_error(Operation::Stat, path, er);
            Err(e)
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<()> {
        let resp = self.gcs_delete_object(path).await?;

        // deleting not existing objects is ok
        if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            let er = parse_error_response(resp).await?;
            let err = parse_error(Operation::Delete, path, er);
            Err(err)
        }
    }

    async fn list(&self, path: &str, _: OpList) -> Result<ObjectStreamer> {
        Ok(Box::new(ObjectPageStreamer::new(DirStream::new(
            Arc::new(self.clone()),
            &self.root,
            path,
        ))))
    }

    // inherits the default implementation of Accessor.
}

impl Backend {
    fn gcs_get_object_request(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/storage/v1/b/{}/o/{}?alt=media",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if offset.is_some() || size.is_some() {
            req = req.header(
                http::header::RANGE,
                BytesRange::new(offset, size).to_string(),
            );
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::Read, path, e))?;

        Ok(req)
    }

    async fn gcs_get_object(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.gcs_get_object_request(path, offset, size)?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error(Operation::Read, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Read, path, e))
    }

    fn gcs_insert_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        mime: Option<String>,
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

        if let Some(mime) = mime {
            req = req.header(CONTENT_TYPE, mime)
        }

        // Set body
        let req = req
            .body(body)
            .map_err(|e| new_request_build_error(Operation::Write, path, e))?;

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
            .map_err(|e| new_request_build_error(Operation::Stat, path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error(Operation::Stat, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Stat, path, e))
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
            .map_err(|e| new_request_build_error(Operation::Delete, path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error(Operation::Delete, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Delete, path, e))
    }

    pub(crate) async fn gcs_list_objects(
        &self,
        path: &str,
        page_token: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/storage/v1/b/{}/o?delimiter=/&prefix={}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
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

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::List, path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error(Operation::List, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::List, path, e))
    }
}

/// The raw json response returned by [`get`](https://cloud.google.com/storage/docs/json_api/v1/objects/get)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    }
}
