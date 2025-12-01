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

use std::fmt::Debug;
use std::sync::Arc;

use http::HeaderName;
use http::HeaderValue;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::IF_NONE_MATCH;
use reqsign::AzureStorageCredential;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

const X_MS_RENAME_SOURCE: &str = "x-ms-rename-source";
const X_MS_VERSION: &str = "x-ms-version";
pub const X_MS_VERSION_ID: &str = "x-ms-version-id";
pub const X_MS_META_PREFIX: &str = "x-ms-meta-";
pub const DIRECTORY: &str = "directory";
pub const FILE: &str = "file";

pub struct AzdlsCore {
    pub info: Arc<AccessorInfo>,
    pub filesystem: String,
    pub root: String,
    pub endpoint: String,

    pub loader: AzureStorageLoader,
    pub signer: AzureStorageSigner,
}

impl Debug for AzdlsCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzdlsCore")
            .field("filesystem", &self.filesystem)
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl AzdlsCore {
    async fn load_credential(&self) -> Result<AzureStorageCredential> {
        let cred = self
            .loader
            .load()
            .await
            .map_err(new_request_credential_error)?;

        if let Some(cred) = cred {
            Ok(cred)
        } else {
            Err(Error::new(
                ErrorKind::ConfigInvalid,
                "no valid credential found",
            ))
        }
    }

    pub async fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let cred = self.load_credential().await?;
        // Insert x-ms-version header for normal requests.
        req.headers_mut().insert(
            HeaderName::from_static(X_MS_VERSION),
            // 2022-11-02 is the version supported by Azurite V3 and
            // used by Azure Portal, We use this version to make
            // sure most our developer happy.
            //
            // In the future, we could allow users to configure this value.
            HeaderValue::from_static("2022-11-02"),
        );
        self.signer.sign(req, &cred).map_err(new_request_sign_error)
    }

    #[inline]
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.info.http_client().send(req).await
    }
}

impl AzdlsCore {
    pub async fn azdls_read(&self, path: &str, range: BytesRange) -> Result<Response<HttpBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header());
        }

        let mut req = req
            .extension(Operation::Read)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.info.http_client().fetch(req).await
    }

    /// resource should be one of `file` or `directory`
    ///
    /// ref: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
    pub async fn azdls_create(
        &self,
        path: &str,
        resource: &str,
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?resource={resource}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        // Content length must be 0 for create request.
        req = req.header(CONTENT_LENGTH, 0);

        if let Some(ty) = args.content_type() {
            req = req.header(CONTENT_TYPE, ty)
        }

        if let Some(pos) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, pos)
        }

        if args.if_not_exists() {
            req = req.header(IF_NONE_MATCH, "*")
        }

        if let Some(v) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, v)
        }

        // Set user metadata headers.
        if let Some(user_metadata) = args.user_metadata() {
            for (key, value) in user_metadata {
                req = req.header(format!("{X_MS_META_PREFIX}{key}"), value);
            }
        }

        let operation = if resource == DIRECTORY {
            Operation::CreateDir
        } else {
            Operation::Write
        };

        let mut req = req
            .extension(operation)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdls_rename(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let source = build_abs_path(&self.root, from);
        let target = build_abs_path(&self.root, to);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&target)
        );

        let source_path = format!("/{}/{}", self.filesystem, percent_encode_path(&source));

        let mut req = Request::put(&url)
            .header(X_MS_RENAME_SOURCE, source_path)
            .header(CONTENT_LENGTH, 0)
            .extension(Operation::Rename)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    /// ref: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    pub async fn azdls_append(
        &self,
        path: &str,
        size: Option<u64>,
        position: u64,
        flush: bool,
        close: bool,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/{}/{}?action=append&position={}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p),
            position
        );

        if flush {
            url.push_str("&flush=true");
        }
        if close {
            url.push_str("&close=true");
        }

        let mut req = Request::patch(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        let mut req = req
            .extension(Operation::Write)
            .body(body)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    /// Flush pending data appended by [`azdls_append`].
    pub async fn azdls_flush(
        &self,
        path: &str,
        position: u64,
        close: bool,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/{}/{}?action=flush&position={}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p),
            position
        );

        if close {
            url.push_str("&close=true");
        }

        let mut req = Request::patch(&url)
            .header(CONTENT_LENGTH, 0)
            .extension(Operation::Write)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdls_get_properties(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?action=getStatus",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let req = Request::head(&url);

        let mut req = req
            .extension(Operation::Stat)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdls_stat_metadata(&self, path: &str) -> Result<Metadata> {
        let resp = self.azdls_get_properties(path).await?;

        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let headers = resp.headers();
        let mut meta = parse_into_metadata(path, headers)?;

        if let Some(version_id) = parse_header_to_str(headers, X_MS_VERSION_ID)? {
            meta.set_version(version_id);
        }

        let user_meta = parse_prefixed_headers(headers, X_MS_META_PREFIX);
        if !user_meta.is_empty() {
            meta = meta.with_user_metadata(user_meta);
        }

        let resource = resp
            .headers()
            .get("x-ms-resource-type")
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "azdls should return x-ms-resource-type header, but it's missing",
                )
            })?
            .to_str()
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "azdls should return x-ms-resource-type header, but it's not a valid string",
                )
                .set_source(err)
            })?;

        match resource {
            FILE => Ok(meta.with_mode(EntryMode::FILE)),
            DIRECTORY => Ok(meta.with_mode(EntryMode::DIR)),
            v => Err(Error::new(
                ErrorKind::Unexpected,
                "azdls returns an unknown x-ms-resource-type",
            )
            .with_context("resource", v)),
        }
    }

    pub async fn azdls_delete(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::delete(&url)
            .extension(Operation::Delete)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdls_list(
        &self,
        path: &str,
        continuation: &str,
        limit: Option<usize>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let mut url = QueryPairsWriter::new(&format!("{}/{}", self.endpoint, self.filesystem))
            .push("resource", "filesystem")
            .push("recursive", "false");
        if !p.is_empty() {
            url = url.push("directory", &percent_encode_path(&p));
        }
        if let Some(limit) = limit {
            url = url.push("maxResults", &limit.to_string());
        }
        if !continuation.is_empty() {
            url = url.push("continuation", &percent_encode_path(continuation));
        }

        let mut req = Request::get(url.finish())
            .extension(Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdls_ensure_parent_path(&self, path: &str) -> Result<Option<Response<Buffer>>> {
        let abs_target_path = path.trim_end_matches('/').to_string();
        let abs_target_path = abs_target_path.as_str();
        let mut parts: Vec<&str> = abs_target_path
            .split('/')
            .filter(|x| !x.is_empty())
            .collect();

        if !parts.is_empty() {
            parts.pop();
        }

        if !parts.is_empty() {
            let parent_path = parts.join("/");
            let resp = self
                .azdls_create(&parent_path, DIRECTORY, &OpWrite::default())
                .await?;

            Ok(Some(resp))
        } else {
            Ok(None)
        }
    }
}
