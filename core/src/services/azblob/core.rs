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
use std::time::Duration;

use http::header::HeaderName;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::IF_MATCH;
use http::header::IF_NONE_MATCH;
use http::HeaderValue;
use http::Request;
use http::Response;
use reqsign::AzureStorageCredential;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;

use crate::raw::*;
use crate::*;

mod constants {
    pub const X_MS_VERSION: &str = "x-ms-version";

    pub const X_MS_BLOB_TYPE: &str = "x-ms-blob-type";
    pub const X_MS_COPY_SOURCE: &str = "x-ms-copy-source";
    pub const X_MS_BLOB_CACHE_CONTROL: &str = "x-ms-blob-cache-control";
    pub const X_MS_BLOB_CONDITION_APPENDPOS: &str = "x-ms-blob-condition-appendpos";

    // Server-side encryption with customer-provided headers
    pub const X_MS_ENCRYPTION_KEY: &str = "x-ms-encryption-key";
    pub const X_MS_ENCRYPTION_KEY_SHA256: &str = "x-ms-encryption-key-sha256";
    pub const X_MS_ENCRYPTION_ALGORITHM: &str = "x-ms-encryption-algorithm";
}

pub struct AzblobCore {
    pub container: String,
    pub root: String,
    pub endpoint: String,
    pub encryption_key: Option<HeaderValue>,
    pub encryption_key_sha256: Option<HeaderValue>,
    pub encryption_algorithm: Option<HeaderValue>,
    pub client: HttpClient,
    pub loader: AzureStorageLoader,
    pub signer: AzureStorageSigner,
    pub batch_max_operations: usize,
}

impl Debug for AzblobCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzblobCore")
            .field("container", &self.container)
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl AzblobCore {
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

    pub async fn sign_query<T>(&self, req: &mut Request<T>) -> Result<()> {
        let cred = self.load_credential().await?;

        self.signer
            .sign_query(req, Duration::from_secs(3600), &cred)
            .map_err(new_request_sign_error)
    }

    pub async fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let cred = self.load_credential().await?;
        // Insert x-ms-version header for normal requests.
        req.headers_mut().insert(
            HeaderName::from_static(constants::X_MS_VERSION),
            // 2022-11-02 is the version supported by Azurite V3 and
            // used by Azure Portal, We use this version to make
            // sure most our developer happy.
            //
            // In the future, we could allow users to configure this value.
            HeaderValue::from_static("2022-11-02"),
        );
        self.signer.sign(req, &cred).map_err(new_request_sign_error)
    }

    async fn batch_sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let cred = self.load_credential().await?;
        self.signer.sign(req, &cred).map_err(new_request_sign_error)
    }

    #[inline]
    pub async fn send(&self, req: Request<AsyncBody>) -> Result<Response<IncomingAsyncBody>> {
        self.client.send(req).await
    }

    pub fn insert_sse_headers(&self, mut req: http::request::Builder) -> http::request::Builder {
        if let Some(v) = &self.encryption_key {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(HeaderName::from_static(constants::X_MS_ENCRYPTION_KEY), v)
        }

        if let Some(v) = &self.encryption_key_sha256 {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_MS_ENCRYPTION_KEY_SHA256),
                v,
            )
        }

        if let Some(v) = &self.encryption_algorithm {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_MS_ENCRYPTION_ALGORITHM),
                v,
            )
        }

        req
    }
}

impl AzblobCore {
    pub fn azblob_get_blob_request(&self, path: &str, args: &OpRead) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let mut query_args = Vec::new();
        if let Some(override_content_disposition) = args.override_content_disposition() {
            query_args.push(format!(
                "rscd={}",
                percent_encode_path(override_content_disposition)
            ))
        }

        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let mut req = Request::get(&url);

        // Set SSE headers.
        req = self.insert_sse_headers(req);

        let range = args.range();
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

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn azblob_get_blob(
        &self,
        path: &str,
        args: &OpRead,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.azblob_get_blob_request(path, args)?;

        self.sign(&mut req).await?;

        self.send(req).await
    }

    pub fn azblob_put_blob_request(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        // Set SSE headers.
        req = self.insert_sse_headers(req);

        if let Some(cache_control) = args.cache_control() {
            req = req.header(constants::X_MS_BLOB_CACHE_CONTROL, cache_control);
        }
        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        if let Some(ty) = args.content_type() {
            req = req.header(CONTENT_TYPE, ty)
        }

        req = req.header(
            HeaderName::from_static(constants::X_MS_BLOB_TYPE),
            "BlockBlob",
        );

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    /// For appendable object, it could be created by `put` an empty blob
    /// with `x-ms-blob-type` header set to `AppendBlob`.
    /// And it's just initialized with empty content.
    ///
    /// If want to append content to it, we should use the following method `azblob_append_blob_request`.
    ///
    /// # Notes
    ///
    /// Appendable blob's custom header could only be set when it's created.
    ///
    /// The following custom header could be set:
    /// - `content-type`
    /// - `x-ms-blob-cache-control`
    ///
    /// # Reference
    ///
    /// https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob
    pub fn azblob_init_appendable_blob_request(
        &self,
        path: &str,
        args: &OpWrite,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        // Set SSE headers.
        req = self.insert_sse_headers(req);

        // The content-length header must be set to zero
        // when creating an appendable blob.
        req = req.header(CONTENT_LENGTH, 0);
        req = req.header(
            HeaderName::from_static(constants::X_MS_BLOB_TYPE),
            "AppendBlob",
        );

        if let Some(ty) = args.content_type() {
            req = req.header(CONTENT_TYPE, ty)
        }

        if let Some(cache_control) = args.cache_control() {
            req = req.header(constants::X_MS_BLOB_CACHE_CONTROL, cache_control);
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    /// Append content to an appendable blob.
    /// The content will be appended to the end of the blob.
    ///
    /// # Notes
    ///
    /// - The maximum size of the content could be appended is 4MB.
    /// - `Append Block` succeeds only if the blob already exists.
    ///
    /// # Reference
    ///
    /// https://learn.microsoft.com/en-us/rest/api/storageservices/append-block
    pub fn azblob_append_blob_request(
        &self,
        path: &str,
        position: u64,
        size: u64,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}?comp=appendblock",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        // Set SSE headers.
        req = self.insert_sse_headers(req);

        req = req.header(CONTENT_LENGTH, size);

        req = req.header(constants::X_MS_BLOB_CONDITION_APPENDPOS, position);

        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn azblob_head_blob_request(
        &self,
        path: &str,
        args: &OpStat,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::head(&url);

        // Set SSE headers.
        req = self.insert_sse_headers(req);

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn azblob_get_blob_properties(
        &self,
        path: &str,
        args: &OpStat,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.azblob_head_blob_request(path, args)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub fn azblob_delete_blob_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let req = Request::delete(&url);

        req.header(CONTENT_LENGTH, 0)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)
    }

    pub async fn azblob_delete_blob(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.azblob_delete_blob_request(path)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azblob_copy_blob(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        let source = build_abs_path(&self.root, from);
        let target = build_abs_path(&self.root, to);

        let source = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&source)
        );
        let target = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&target)
        );

        let mut req = Request::put(&target)
            .header(constants::X_MS_COPY_SOURCE, source)
            .header(CONTENT_LENGTH, 0)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azblob_list_blobs(
        &self,
        path: &str,
        next_marker: &str,
        delimiter: &str,
        limit: Option<usize>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/{}?restype=container&comp=list",
            self.endpoint, self.container
        );
        if !p.is_empty() {
            write!(url, "&prefix={}", percent_encode_path(&p))
                .expect("write into string must succeed");
        }
        if let Some(limit) = limit {
            write!(url, "&maxresults={limit}").expect("write into string must succeed");
        }
        if !delimiter.is_empty() {
            write!(url, "&delimiter={delimiter}").expect("write into string must succeed");
        }
        if !next_marker.is_empty() {
            write!(url, "&marker={next_marker}").expect("write into string must succeed");
        }

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azblob_batch_delete(
        &self,
        paths: &[String],
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = format!(
            "{}/{}?restype=container&comp=batch",
            self.endpoint, self.container
        );

        let mut multipart = Multipart::new();

        for (idx, path) in paths.iter().enumerate() {
            let mut req = self.azblob_delete_blob_request(path)?;
            self.batch_sign(&mut req).await?;

            multipart = multipart.part(
                MixedPart::from_request(req).part_header("content-id".parse().unwrap(), idx.into()),
            );
        }

        let req = Request::post(url);
        let mut req = multipart.apply(req)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }
}
