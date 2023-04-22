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
use std::str::FromStr;

use http::header::HeaderName;
use http::header::CACHE_CONTROL;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::IF_MATCH;
use http::header::IF_NONE_MATCH;
use http::Request;
use http::Response;
use http::Uri;
use reqsign::AzureStorageCredential;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;

use super::batch::BatchDeleteRequestBuilder;
use crate::raw::*;
use crate::*;

mod constants {
    pub const X_MS_BLOB_TYPE: &str = "x-ms-blob-type";
    pub const X_MS_COPY_SOURCE: &str = "x-ms-copy-source";
}

pub struct AzblobCore {
    pub container: String,
    pub root: String,
    pub endpoint: String,

    pub client: HttpClient,
    pub loader: AzureStorageLoader,
    pub signer: AzureStorageSigner,
    pub batch_signer: AzureStorageSigner,
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

    pub async fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let cred = self.load_credential().await?;
        self.signer.sign(req, &cred).map_err(new_request_sign_error)
    }

    async fn batch_sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let cred = self.load_credential().await?;
        self.batch_signer
            .sign(req, &cred)
            .map_err(new_request_sign_error)
    }

    #[inline]
    pub async fn send(&self, req: Request<AsyncBody>) -> Result<Response<IncomingAsyncBody>> {
        self.client.send(req).await
    }
}

impl AzblobCore {
    pub async fn azblob_get_blob(
        &self,
        path: &str,
        range: BytesRange,
        if_none_match: Option<&str>,
        if_match: Option<&str>,
        override_content_disposition: Option<&str>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let mut query_args = Vec::new();
        if let Some(override_content_disposition) = override_content_disposition {
            query_args.push(format!(
                "rscd={}",
                percent_encode_path(override_content_disposition)
            ))
        }

        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

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

        if let Some(if_none_match) = if_none_match {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(if_match) = if_match {
            req = req.header(IF_MATCH, if_match);
        }

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.send(req).await
    }

    pub fn azblob_put_blob_request(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        cache_control: Option<&str>,
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
        if let Some(cache_control) = cache_control {
            req = req.header(CACHE_CONTROL, cache_control);
        }
        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        if let Some(ty) = content_type {
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

    pub async fn azblob_get_blob_properties(
        &self,
        path: &str,
        if_none_match: Option<&str>,
        if_match: Option<&str>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::head(&url);

        if let Some(if_none_match) = if_none_match {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(if_match) = if_match {
            req = req.header(IF_MATCH, if_match);
        }

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azblob_delete_blob(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let req = Request::delete(&url);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

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
        // init batch request
        let url = format!(
            "{}/{}?restype=container&comp=batch",
            self.endpoint, self.container
        );
        let mut batch_delete_req_builder = BatchDeleteRequestBuilder::new(&url);

        for path in paths.iter() {
            // build sub requests
            let p = build_abs_path(&self.root, path);
            let encoded_path = percent_encode_path(&p);

            let url = Uri::from_str(&format!(
                "{}/{}/{}",
                self.endpoint, self.container, encoded_path
            ))
            .unwrap();

            let mut sub_req = Request::delete(&url.to_string())
                .header(CONTENT_LENGTH, 0)
                .body(AsyncBody::Empty)
                .map_err(new_request_build_error)?;

            self.batch_sign(&mut sub_req).await?;

            batch_delete_req_builder.append(sub_req);
        }

        let mut req = batch_delete_req_builder.try_into_req()?;

        self.sign(&mut req).await?;
        self.send(req).await
    }
}
