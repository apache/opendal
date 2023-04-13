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
use http::Request;
use http::Response;
use reqsign::AzureStorageCredential;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;

use crate::raw::*;
use crate::*;

const X_MS_RENAME_SOURCE: &str = "x-ms-rename-source";

pub struct AzdfsCore {
    pub filesystem: String,
    pub root: String,
    pub endpoint: String,

    pub client: HttpClient,
    pub loader: AzureStorageLoader,
    pub signer: AzureStorageSigner,
}

impl Debug for AzdfsCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzdfsCore")
            .field("filesystem", &self.filesystem)
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl AzdfsCore {
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
        self.signer.sign(req, &cred).map_err(new_request_sign_error)
    }

    #[inline]
    pub async fn send(&self, req: Request<AsyncBody>) -> Result<Response<IncomingAsyncBody>> {
        self.client.send(req).await
    }
}

impl AzdfsCore {
    pub async fn azdfs_read(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if !range.is_full() {
            // azblob doesn't support read with suffix range.
            //
            // ref: https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-the-range-header-for-blob-service-operations
            if range.offset().is_none() && range.size().is_some() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "azblob doesn't support read with suffix range",
                ));
            }

            req = req.header(http::header::RANGE, range.to_header());
        }

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    /// resource should be one of `file` or `directory`
    ///
    /// ref: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
    pub fn azdfs_create_request(
        &self,
        path: &str,
        resource: &str,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
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

        if let Some(ty) = content_type {
            req = req.header(CONTENT_TYPE, ty)
        }

        if let Some(pos) = content_disposition {
            req = req.header(CONTENT_DISPOSITION, pos)
        }

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn azdfs_rename(&self, from: &str, to: &str) -> Result<Response<IncomingAsyncBody>> {
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
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    /// ref: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    pub fn azdfs_update_request(
        &self,
        path: &str,
        size: Option<usize>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        // - close: Make this is the final action to this file.
        // - flush: Flush the file directly.
        let url = format!(
            "{}/{}/{}?action=append&close=true&flush=true&position=0",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::patch(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn azdfs_get_properties(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
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
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.client.send(req).await
    }

    pub async fn azdfs_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
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
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdfs_list(
        &self,
        path: &str,
        continuation: &str,
        limit: Option<usize>,
    ) -> Result<Response<IncomingAsyncBody>> {
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
            write!(url, "&maxresults={limit}").expect("write into string must succeed");
        }
        if !continuation.is_empty() {
            write!(url, "&continuation={continuation}").expect("write into string must succeed");
        }

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdfs_ensure_parent_path(
        &self,
        path: &str,
    ) -> Result<Option<Response<IncomingAsyncBody>>> {
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
            let mut req =
                self.azdfs_create_request(&parent_path, "directory", None, None, AsyncBody::Empty)?;

            self.sign(&mut req).await?;

            Ok(Some(self.send(req).await?))
        } else {
            Ok(None)
        }
    }
}
