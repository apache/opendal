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

use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;

use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::HeaderValue;
use http::Request;

use http::{HeaderName, StatusCode};
use reqsign::AzureStorageCredential;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;
use serde::Deserialize;


use crate::raw::*;
use crate::services::azdls::error::parse_error;
use crate::*;

const X_MS_RENAME_SOURCE: &str = "x-ms-rename-source";
const X_MS_VERSION: &str = "x-ms-version";

pub struct AzdlsCore {
    pub filesystem: String,
    pub root: String,
    pub endpoint: String,

    pub client: HttpClient,
    pub loader: AzureStorageLoader,
    pub signer: AzureStorageSigner,
}

impl Debug for AzdlsCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
}

impl AzdlsCore {
    pub async fn azdls_read(
        &self,
        path: &str,
        range: BytesRange,
        buf: oio::WritableBuf,
    ) -> Result<usize> {
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
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => body.read(buf).await,
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(0),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    /// Create a directory.
    pub async fn azdls_create_directory(&self, path: &str) -> Result<()> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?resource=directory",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url)
            // Content length must be 0 for create request.
            .header(CONTENT_LENGTH, 0)
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::CREATED | StatusCode::CONFLICT => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    /// Create a file
    pub async fn azdls_create_file(&self, path: &str, args: &OpWrite) -> Result<()> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?resource=file",
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

        // Set body
        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::CREATED | StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?.with_operation("Backend::azdls_create_request"))
            }
        }
    }

    pub async fn azdls_rename(&self, from: &str, to: &str) -> Result<()> {
        let source = build_abs_path(&self.root, from);
        let target = build_abs_path(&self.root, to);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&target)
        );

        let mut req = Request::put(&url)
            .header(
                X_MS_RENAME_SOURCE,
                format!("/{}/{}", self.filesystem, percent_encode_path(&source)),
            )
            .header(CONTENT_LENGTH, 0)
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::CREATED => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    /// ref: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    pub async fn azdls_update_request(
        &self,
        path: &str,
        size: Option<u64>,
        position: u64,
        body: RequestBody,
    ) -> Result<()> {
        let p = build_abs_path(&self.root, path);

        // - close: Make this is the final action to this file.
        // - flush: Flush the file directly.
        let url = format!(
            "{}/{}/{}?action=append&close=true&flush=true&position={}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p),
            position
        );

        let mut req = Request::patch(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        // Set body
        let mut req = req.body(body).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK | StatusCode::ACCEPTED => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?.with_operation("Backend::azdls_update_request"))
            }
        }
    }

    pub async fn azdls_get_properties(&self, path: &str) -> Result<Metadata> {
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
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();

        if parts.status != StatusCode::OK {
            let bs = body.to_bytes().await?;
            return Err(parse_error(parts, bs)?);
        }

        let mut meta = parse_into_metadata(path, &parts.headers)?;
        let resource = parts
            .headers
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

        meta = match resource {
            "file" => meta.with_mode(EntryMode::FILE),
            "directory" => meta.with_mode(EntryMode::DIR),
            v => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "azdls returns not supported x-ms-resource-type",
                )
                .with_context("resource", v))
            }
        };
        Ok(meta)
    }

    pub async fn azdls_delete(&self, path: &str) -> Result<()> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let req = Request::delete(&url);

        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK | StatusCode::NOT_FOUND => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn azdls_list(
        &self,
        path: &str,
        continuation: &str,
        limit: Option<usize>,
    ) -> Result<Option<(String, ListOutput)>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let mut url = format!(
            "{}/{}?resource=filesystem&recursive=false",
            self.endpoint, self.filesystem
        );
        if !p.is_empty() {
            write!(url, "&directory={}", percent_encode_path(&p))
                .expect("write into string must succeed");
        }
        if let Some(limit) = limit {
            write!(url, "&maxResults={limit}").expect("write into string must succeed");
        }
        if !continuation.is_empty() {
            write!(url, "&continuation={}", percent_encode_path(continuation))
                .expect("write into string must succeed");
        }

        let mut req = Request::get(&url)
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let token = parse_header_to_str(&parts.headers, "x-ms-continuation")?
                    .map(ToString::to_string)
                    .unwrap_or_default();
                let output: ListOutput = body.to_json().await?;
                Ok(Some((token, output)))
            }
            StatusCode::NOT_FOUND => Ok(None),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn azdls_ensure_parent_path(&self, path: &str) -> Result<()> {
        let abs_target_path = path.trim_end_matches('/').to_string();
        let abs_target_path = abs_target_path.as_str();
        let mut parts: Vec<&str> = abs_target_path
            .split('/')
            .filter(|x| !x.is_empty())
            .collect();

        // FIXME: we should make code here more clear.
        if !parts.is_empty() {
            parts.pop();
        }

        if parts.is_empty() {
            return Ok(());
        }

        let parent_path = parts.join("/");
        self.azdls_create_directory(&parent_path).await
    }
}

/// # Examples
///
/// ```json
/// {"paths":[{"contentLength":"1977097","etag":"0x8DACF9B0061305F","group":"$superuser","lastModified":"Sat, 26 Nov 2022 10:43:05 GMT","name":"c3b3ef48-7783-4946-81bc-dc07e1728878/d4ea21d7-a533-4011-8b1f-d0e566d63725","owner":"$superuser","permissions":"rw-r-----"}]}
/// ```
#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct ListOutput {
    pub paths: Vec<Path>,
}

#[derive(Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Path {
    #[serde(rename = "contentLength")]
    pub content_length: String,
    #[serde(rename = "etag")]
    pub etag: String,
    /// Azdls will return `"true"` and `"false"` for is_directory.
    #[serde(rename = "isDirectory")]
    pub is_directory: String,
    #[serde(rename = "lastModified")]
    pub last_modified: String,
    #[serde(rename = "name")]
    pub name: String,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_parse_path() {
        let bs = Bytes::from(
            r#"{"paths":[{"contentLength":"1977097","etag":"0x8DACF9B0061305F","group":"$superuser","lastModified":"Sat, 26 Nov 2022 10:43:05 GMT","name":"c3b3ef48-7783-4946-81bc-dc07e1728878/d4ea21d7-a533-4011-8b1f-d0e566d63725","owner":"$superuser","permissions":"rw-r-----"}]}"#,
        );
        let out: ListOutput = serde_json::from_slice(&bs).expect("must success");
        println!("{out:?}");

        assert_eq!(
            out.paths[0],
            Path {
                content_length: "1977097".to_string(),
                etag: "0x8DACF9B0061305F".to_string(),
                is_directory: "".to_string(),
                last_modified: "Sat, 26 Nov 2022 10:43:05 GMT".to_string(),
                name: "c3b3ef48-7783-4946-81bc-dc07e1728878/d4ea21d7-a533-4011-8b1f-d0e566d63725"
                    .to_string()
            }
        );
    }
}
