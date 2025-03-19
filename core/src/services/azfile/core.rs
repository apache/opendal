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

use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::RANGE;
use http::HeaderName;
use http::HeaderValue;
use http::Request;
use http::Response;
use http::StatusCode;
use reqsign::AzureStorageCredential;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;
use std::sync::Arc;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

const X_MS_VERSION: &str = "x-ms-version";
const X_MS_WRITE: &str = "x-ms-write";
const X_MS_FILE_RENAME_SOURCE: &str = "x-ms-file-rename-source";
const X_MS_CONTENT_LENGTH: &str = "x-ms-content-length";
const X_MS_TYPE: &str = "x-ms-type";
const X_MS_FILE_RENAME_REPLACE_IF_EXISTS: &str = "x-ms-file-rename-replace-if-exists";

pub struct AzfileCore {
    pub info: Arc<AccessorInfo>,
    pub root: String,
    pub endpoint: String,
    pub share_name: String,
    pub loader: AzureStorageLoader,
    pub signer: AzureStorageSigner,
}

impl Debug for AzfileCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzfileCore")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("share_name", &self.share_name)
            .finish_non_exhaustive()
    }
}

impl AzfileCore {
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
            // consistent with azdls and azblob
            HeaderValue::from_static("2022-11-02"),
        );
        self.signer.sign(req, &cred).map_err(new_request_sign_error)
    }

    #[inline]
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.info.http_client().send(req).await
    }

    pub async fn azfile_read(&self, path: &str, range: BytesRange) -> Result<Response<HttpBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(RANGE, range.to_header());
        }

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.info.http_client().fetch(req).await
    }

    pub async fn azfile_create_file(
        &self,
        path: &str,
        size: usize,
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();
        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        // x-ms-content-length specifies the maximum size for the file, up to 4 tebibytes (TiB)
        // https://learn.microsoft.com/en-us/rest/api/storageservices/create-file
        req = req.header(X_MS_CONTENT_LENGTH, size);

        req = req.header(X_MS_TYPE, "file");

        // Content length must be 0 for create request.
        req = req.header(CONTENT_LENGTH, 0);

        if let Some(ty) = args.content_type() {
            req = req.header(CONTENT_TYPE, ty);
        }

        if let Some(pos) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, pos);
        }

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azfile_update(
        &self,
        path: &str,
        size: u64,
        position: u64,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?comp=range",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        req = req.header(CONTENT_LENGTH, size);

        req = req.header(X_MS_WRITE, "update");

        req = req.header(
            RANGE,
            BytesRange::from(position..position + size).to_header(),
        );

        let mut req = req.body(body).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azfile_get_file_properties(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let req = Request::head(&url);

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azfile_get_directory_properties(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}?restype=directory",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let req = Request::head(&url);

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azfile_rename(&self, path: &str, new_path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let new_p = build_abs_path(&self.root, new_path)
            .trim_start_matches('/')
            .to_string();

        let url = if path.ends_with('/') {
            format!(
                "{}/{}/{}?restype=directory&comp=rename",
                self.endpoint,
                self.share_name,
                percent_encode_path(&new_p)
            )
        } else {
            format!(
                "{}/{}/{}?comp=rename",
                self.endpoint,
                self.share_name,
                percent_encode_path(&new_p)
            )
        };

        let mut req = Request::put(&url);

        req = req.header(CONTENT_LENGTH, 0);

        // x-ms-file-rename-source specifies the file or directory to be renamed.
        // the value must be a URL style path
        // the official document does not mention the URL style path
        // find the solution from the community FAQ and implementation of the Java-SDK
        // ref: https://learn.microsoft.com/en-us/answers/questions/799611/azure-file-service-rest-api(rename)?page=1
        let source_url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        req = req.header(X_MS_FILE_RENAME_SOURCE, &source_url);

        req = req.header(X_MS_FILE_RENAME_REPLACE_IF_EXISTS, "true");

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azfile_create_dir(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?restype=directory",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        req = req.header(CONTENT_LENGTH, 0);

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azfile_delete_file(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let req = Request::delete(&url);

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azfile_delete_dir(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?restype=directory",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let req = Request::delete(&url);

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azfile_list(
        &self,
        path: &str,
        limit: &Option<usize>,
        continuation: &String,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let mut url = format!(
            "{}/{}/{}?restype=directory&comp=list&include=Timestamps,ETag",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p),
        );

        if !continuation.is_empty() {
            write!(url, "&marker={}", &continuation).expect("write into string must succeed");
        }

        if let Some(limit) = limit {
            write!(url, "&maxresults={}", limit).expect("write into string must succeed");
        }

        let req = Request::get(&url);

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn ensure_parent_dir_exists(&self, path: &str) -> Result<()> {
        let mut dirs = VecDeque::default();
        // azure file service does not support recursive directory creation
        let mut p = path;
        while p != "/" {
            p = get_parent(p);
            dirs.push_front(p);
        }

        let mut pop_dir_count = dirs.len();
        for dir in dirs.iter().rev() {
            let resp = self.azfile_get_directory_properties(dir).await?;
            if resp.status() == StatusCode::NOT_FOUND {
                pop_dir_count -= 1;
                continue;
            }
            break;
        }

        for dir in dirs.iter().skip(pop_dir_count) {
            let resp = self.azfile_create_dir(dir).await?;

            if resp.status() == StatusCode::CREATED {
                continue;
            }

            if resp
                .headers()
                .get("x-ms-error-code")
                .map(|value| value.to_str().unwrap_or(""))
                .unwrap_or_else(|| "")
                == "ResourceAlreadyExists"
            {
                continue;
            }

            return Err(parse_error(resp));
        }

        Ok(())
    }
}
