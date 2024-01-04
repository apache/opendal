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

use super::core::*;
use super::error::parse_error;
use super::lister::GcsLister;
use super::writer::GcsWriter;
use super::writer::GcsWriters;
use crate::raw::*;
use crate::*;

const DEFAULT_GCS_ENDPOINT: &str = "https://storage.googleapis.com";
const DEFAULT_GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

/// [Google Cloud Storage](https://cloud.google.com/storage) services support.
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct GcsConfig {
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
    /// The predefined acl for GCS.
    predefined_acl: Option<String>,
    /// The default storage class used by gcs.
    default_storage_class: Option<String>,
}

impl Debug for GcsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsConfig")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

/// [Google Cloud Storage](https://cloud.google.com/storage) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct GcsBuilder {
    config: GcsConfig,

    http_client: Option<HttpClient>,
    customed_token_loader: Option<Box<dyn GoogleTokenLoad>>,
}

impl Debug for GcsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("GcsBuilder");

        ds.field("config", &self.config);
        ds.finish_non_exhaustive()
    }
}

impl GcsBuilder {
    /// set the working directory root of backend
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.config.root = Some(root.to_string())
        }

        self
    }

    /// set the container's name
    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        self.config.bucket = bucket.to_string();
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
            self.config.scope = Some(scope.to_string())
        };
        self
    }

    /// Set the GCS service account.
    ///
    /// service account will be used for fetch token from vm metadata.
    /// If not set, we will try to fetch with `default` service account.
    pub fn service_account(&mut self, service_account: &str) -> &mut Self {
        if !service_account.is_empty() {
            self.config.service_account = Some(service_account.to_string())
        };
        self
    }

    /// set the endpoint GCS service uses
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.to_string())
        };
        self
    }

    /// set the base64 hashed credentials string used for OAuth2
    pub fn credential(&mut self, credential: &str) -> &mut Self {
        if !credential.is_empty() {
            self.config.credential = Some(credential.to_string())
        };
        self
    }

    /// set the credentials path of GCS.
    pub fn credential_path(&mut self, path: &str) -> &mut Self {
        if !path.is_empty() {
            self.config.credential_path = Some(path.to_string())
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
            self.config.predefined_acl = Some(acl.to_string())
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
            self.config.default_storage_class = Some(class.to_string())
        };
        self
    }
}

impl Builder for GcsBuilder {
    const SCHEME: Scheme = Scheme::Gcs;
    type Accessor = GcsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let config = GcsConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        GcsBuilder {
            config,
            ..GcsBuilder::default()
        }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", self);

        let root = normalize_root(&self.config.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        // Handle endpoint and bucket name
        let bucket = match self.config.bucket.is_empty() {
            false => Ok(&self.config.bucket),
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
            .config
            .endpoint
            .clone()
            .unwrap_or_else(|| DEFAULT_GCS_ENDPOINT.to_string());
        debug!("backend use endpoint: {endpoint}");

        let mut cred_loader = GoogleCredentialLoader::default();
        if let Some(cred) = &self.config.credential {
            cred_loader = cred_loader.with_content(cred);
        }
        if let Some(cred) = &self.config.credential_path {
            cred_loader = cred_loader.with_path(cred);
        }
        #[cfg(target_arch = "wasm32")]
        {
            cred_loader = cred_loader.with_disable_env();
            cred_loader = cred_loader.with_disable_well_known_location();
        }

        let scope = if let Some(scope) = &self.config.scope {
            scope
        } else {
            DEFAULT_GCS_SCOPE
        };

        let mut token_loader = GoogleTokenLoader::new(scope, client.client());
        if let Some(account) = &self.config.service_account {
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
                predefined_acl: self.config.predefined_acl.clone(),
                default_storage_class: self.config.default_storage_class.clone(),
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Accessor for GcsBackend {
    type Reader = IncomingAsyncBody;
    type Writer = GcsWriters;
    type Lister = oio::PageLister<GcsLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Gcs)
            .set_root(&self.core.root)
            .set_name(&self.core.bucket)
            .set_native_capability(Capability {
                stat: true,
                stat_with_if_match: true,
                stat_with_if_none_match: true,

                read: true,
                read_can_next: true,
                read_with_range: true,
                read_with_if_match: true,
                read_with_if_none_match: true,

                write: true,
                write_can_empty: true,
                write_can_multi: true,
                write_with_content_type: true,
                // The buffer size should be a multiple of 256 KiB (256 x 1024 bytes), unless it's the last chunk that completes the upload.
                // Larger chunk sizes typically make uploads faster, but note that there's a tradeoff between speed and memory usage.
                // It's recommended that you use at least 8 MiB for the chunk size.
                //
                // Reference: [Perform resumable uploads](https://cloud.google.com/storage/docs/performing-resumable-uploads)
                write_multi_align_size: Some(256 * 1024 * 1024),

                delete: true,
                copy: true,

                list: true,
                list_with_limit: true,
                list_with_start_after: true,
                list_with_recursive: true,

                batch: true,
                batch_max_operations: Some(100),
                presign: true,
                presign_stat: true,
                presign_read: true,
                presign_write: true,

                ..Default::default()
            });
        am
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.gcs_get_object_metadata(path, &args).await?;

        if !resp.status().is_success() {
            return Err(parse_error(resp).await?);
        }

        let slc = resp.into_body().bytes().await?;

        let meta: GetObjectJsonResponse =
            serde_json::from_slice(&slc).map_err(new_json_deserialize_error)?;

        let mut m = Metadata::new(EntryMode::FILE);

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
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.gcs_get_object(path, &args).await?;

        if resp.status().is_success() {
            let size = parse_content_length(resp.headers())?;
            Ok((RpRead::new().with_size(size), resp.into_body()))
        } else if resp.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            Ok((RpRead::new(), IncomingAsyncBody::empty()))
        } else {
            Err(parse_error(resp).await?)
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let w = GcsWriter::new(self.core.clone(), path, args);
        let w = oio::RangeWriter::new(w);

        Ok((RpWrite::default(), w))
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

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = GcsLister::new(
            self.core.clone(),
            path,
            args.recursive(),
            args.limit(),
            args.start_after(),
        );

        Ok((RpList::default(), oio::PageLister::new(l)))
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

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        // We will not send this request out, just for signing.
        let mut req = match args.operation() {
            PresignOperation::Stat(v) => self.core.gcs_head_object_xml_request(path, v)?,
            PresignOperation::Read(v) => self.core.gcs_get_object_xml_request(path, v)?,
            PresignOperation::Write(v) => {
                self.core
                    .gcs_insert_object_xml_request(path, v, AsyncBody::Empty)?
            }
        };

        self.core.sign_query(&mut req, args.expire()).await?;

        // We don't need this request anymore, consume it directly.
        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let ops = args.into_operation();
        if ops.len() > 100 {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "gcs services only allow delete less than 100 keys at once",
            )
            .with_context("length", ops.len().to_string()));
        }

        let paths: Vec<String> = ops.into_iter().map(|(p, _)| p).collect();
        let resp = self.core.gcs_delete_objects(paths.clone()).await?;

        let status = resp.status();

        if let StatusCode::OK = status {
            let content_type = parse_content_type(resp.headers())?.ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "gcs batch delete response content type is empty",
                )
            })?;
            let boundary = content_type
                .strip_prefix("multipart/mixed; boundary=")
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "gcs batch delete response content type is not multipart/mixed",
                    )
                })?
                .trim_matches('"');
            let multipart: Multipart<MixedPart> = Multipart::new()
                .with_boundary(boundary)
                .parse(resp.into_body().bytes().await?)?;
            let parts = multipart.into_parts();

            let mut batched_result = Vec::with_capacity(parts.len());

            for (i, part) in parts.into_iter().enumerate() {
                let resp = part.into_response();
                // TODO: maybe we can take it directly?
                let path = paths[i].clone();

                // deleting not existing objects is ok
                if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
                    batched_result.push((path, Ok(RpDelete::default().into())));
                } else {
                    batched_result.push((path, Err(parse_error(resp).await?)));
                }
            }

            Ok(RpBatch::new(batched_result))
        } else {
            // If the overall request isn't formatted correctly and Cloud Storage is unable to parse it into sub-requests, you receive a 400 error.
            // Otherwise, Cloud Storage returns a 200 status code, even if some or all of the sub-requests fail.
            Err(parse_error(resp).await?)
        }
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
