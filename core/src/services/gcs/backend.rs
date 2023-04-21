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
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use log::debug;
use reqsign::GoogleCredentialLoader;
use reqsign::GoogleSigner;
use reqsign::GoogleTokenLoad;
use reqsign::GoogleTokenLoader;
use serde::Deserialize;
use serde_json;

use super::core::GcsCore;
use super::error::parse_error;
use super::pager::GcsPager;
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
/// - [x] copy
/// - [ ] presign
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the work directory for backend
/// - `bucket`: Set the container name for backend
/// - `endpoint`: Customizable endpoint setting
/// - `credentials`: Credential string for GCS OAuth2
/// - `predefined_acl`: Predefined ACL for GCS
/// - `default_storage_class`: Default storage class for GCS
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
///     // set the predefined ACL for GCS
///     builder.predefined_acl("publicRead");
///     // set the default storage class for GCS
///     builder.default_storage_class("STANDARD");
///
///     let op: Operator = Operator::new(builder)?.finish();
///     Ok(())
/// }
/// ```
#[derive(Default)]
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
    customed_token_loader: Option<Box<dyn GoogleTokenLoad>>,
    predefined_acl: Option<String>,
    default_storage_class: Option<String>,
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

    /// Specify the customed token loader used by this service.
    pub fn customed_token_loader(&mut self, token_load: Box<dyn GoogleTokenLoad>) -> &mut Self {
        self.customed_token_loader = Some(token_load);
        self
    }

    /// Set the predefined acl for GCS.
    ///
    /// Available values are:
    /// - `authenticatedRead`
    /// - `bucketOwnerFullControl`
    /// - `bucketOwnerRead`
    /// - `private`
    /// - `projectPrivate`
    /// - `publicRead`
    pub fn predefined_acl(&mut self, acl: &str) -> &mut Self {
        if !acl.is_empty() {
            self.predefined_acl = Some(acl.to_string())
        };
        self
    }

    /// Set the default storage class for GCS.
    ///
    /// Available values are:
    /// - `STANDARD`
    /// - `NEARLINE`
    /// - `COLDLINE`
    /// - `ARCHIVE`
    pub fn default_storage_class(&mut self, class: &str) -> &mut Self {
        if !class.is_empty() {
            self.default_storage_class = Some(class.to_string())
        };
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
        if self.predefined_acl.is_some() {
            ds.field("predefined_acl", &self.predefined_acl);
        }
        ds.field("default_storage_class", &self.default_storage_class);
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
        map.get("predefined_acl").map(|v| builder.predefined_acl(v));
        map.get("default_storage_class")
            .map(|v| builder.default_storage_class(v));

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

        let mut cred_loader = GoogleCredentialLoader::default();
        if let Some(cred) = &self.credential {
            cred_loader = cred_loader.with_content(cred);
        }
        if let Some(cred) = &self.credential_path {
            cred_loader = cred_loader.with_path(cred);
        }

        let scope = if let Some(scope) = &self.scope {
            scope
        } else {
            DEFAULT_GCS_SCOPE
        };

        let mut token_loader = GoogleTokenLoader::new(scope, client.client());
        if let Some(account) = &self.service_account {
            token_loader = token_loader.with_service_account(account);
        }
        if let Ok(Some(cred)) = cred_loader.load() {
            token_loader = token_loader.with_credentials(cred)
        }
        if let Some(loader) = self.customed_token_loader.take() {
            token_loader = token_loader.with_customed_token_loader(loader)
        }

        let signer = GoogleSigner::new("storage");

        let backend = GcsBackend {
            core: Arc::new(GcsCore {
                endpoint,
                bucket: bucket.to_string(),
                root,
                client,
                signer,
                token_loader,
                credential_loader: cred_loader,
                predefined_acl: self.predefined_acl.clone(),
                default_storage_class: self.default_storage_class.clone(),
            }),
        };

        Ok(backend)
    }
}

/// GCS storage backend
#[derive(Clone, Debug)]
pub struct GcsBackend {
    core: Arc<GcsCore>,
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
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Gcs)
            .set_root(&self.core.root)
            .set_name(&self.core.bucket)
            .set_capability(Capability {
                read: true,
                read_can_next: true,
                write: true,
                list: true,
                scan: true,
                copy: true,
                ..Default::default()
            });
        am
    }

    async fn create_dir(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let mut req = self
            .core
            .gcs_insert_object_request(path, Some(0), None, AsyncBody::Empty)?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

        if resp.status().is_success() {
            resp.into_body().consume().await?;
            Ok(RpCreate::default())
        } else {
            Err(parse_error(resp).await?)
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self
            .core
            .gcs_get_object(path, args.range(), args.if_match(), args.if_none_match())
            .await?;

        if resp.status().is_success() {
            let meta = parse_into_metadata(path, resp.headers())?;
            Ok((RpRead::with_metadata(meta), resp.into_body()))
        } else {
            Err(parse_error(resp).await?)
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            GcsWriter::new(self.core.clone(), path, args),
        ))
    }

    async fn copy(&self, from: &str, to: &str, _: OpCopy) -> Result<RpCopy> {
        let resp = self.core.gcs_copy_object(from, to).await?;

        if resp.status().is_success() {
            resp.into_body().consume().await?;
            Ok(RpCopy::default())
        } else {
            Err(parse_error(resp).await?)
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.core.gcs_get_object_metadata(path).await?;

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

            m.set_last_modified(parse_datetime_from_rfc3339(&meta.updated)?);

            Ok(RpStat::new(m))
        } else if resp.status() == StatusCode::NOT_FOUND && path.ends_with('/') {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            Err(parse_error(resp).await?)
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.gcs_delete_object(path).await?;

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
            GcsPager::new(self.core.clone(), path, "/", args.limit()),
        ))
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        Ok((
            RpScan::default(),
            GcsPager::new(self.core.clone(), path, "", args.limit()),
        ))
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
