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

use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::HeaderValue;
use http::Request;
use http::Response;

use crate::raw::*;
use crate::*;

pub struct SupabaseCore {
    pub root: String,
    pub bucket: String,
    pub endpoint: String,

    /// The key used for authorization
    /// If loaded, the read operation will always access the nonpublic resources.
    /// If you want to read the public resources, please do not set the key.
    pub key: Option<String>,

    pub http_client: HttpClient,
}

impl Debug for SupabaseCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupabaseCore")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl SupabaseCore {
    pub fn new(
        root: &str,
        bucket: &str,
        endpoint: &str,
        key: Option<String>,
        client: HttpClient,
    ) -> Self {
        Self {
            root: root.to_string(),
            bucket: bucket.to_string(),
            endpoint: endpoint.to_string(),
            key,
            http_client: client,
        }
    }

    /// Add authorization header to the request if the key is set. Otherwise leave
    /// the request as-is.
    pub fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        if let Some(k) = &self.key {
            let v = HeaderValue::from_str(&format!("Bearer {}", k)).unwrap();
            req.headers_mut().insert(http::header::AUTHORIZATION, v);
        }
        Ok(())
    }
}

// requests
impl SupabaseCore {
    pub fn supabase_upload_object_request(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/storage/v1/object/{}/{}",
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

        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn supabase_delete_object_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/storage/v1/object/{}/{}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        Request::delete(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)
    }

    pub fn supabase_get_object_public_request(
        &self,
        path: &str,
        _: BytesRange,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/storage/v1/object/public/{}/{}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        let req = Request::get(&url);

        req.body(AsyncBody::Empty).map_err(new_request_build_error)
    }

    pub fn supabase_get_object_auth_request(
        &self,
        path: &str,
        _: BytesRange,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/storage/v1/object/authenticated/{}/{}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        let req = Request::get(&url);

        req.body(AsyncBody::Empty).map_err(new_request_build_error)
    }

    pub fn supabase_head_object_public_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/storage/v1/object/public/{}/{}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        Request::head(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)
    }

    pub fn supabase_head_object_auth_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/storage/v1/object/authenticated/{}/{}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        Request::head(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)
    }

    pub fn supabase_get_object_info_public_request(
        &self,
        path: &str,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/storage/v1/object/info/public/{}/{}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)
    }

    pub fn supabase_get_object_info_auth_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/storage/v1/object/info/authenticated/{}/{}",
            self.endpoint,
            self.bucket,
            percent_encode_path(&p)
        );

        Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)
    }
}

// core utils
impl SupabaseCore {
    pub async fn send(&self, req: Request<AsyncBody>) -> Result<Response<Buffer>> {
        self.http_client.send(req).await
    }

    pub async fn supabase_get_object(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<Buffer>> {
        let mut req = if self.key.is_some() {
            self.supabase_get_object_auth_request(path, range)?
        } else {
            self.supabase_get_object_public_request(path, range)?
        };
        self.sign(&mut req)?;
        self.send(req).await
    }

    pub async fn supabase_head_object(&self, path: &str) -> Result<Response<Buffer>> {
        let mut req = if self.key.is_some() {
            self.supabase_head_object_auth_request(path)?
        } else {
            self.supabase_head_object_public_request(path)?
        };
        self.sign(&mut req)?;
        self.send(req).await
    }

    pub async fn supabase_get_object_info(&self, path: &str) -> Result<Response<Buffer>> {
        let mut req = if self.key.is_some() {
            self.supabase_get_object_info_auth_request(path)?
        } else {
            self.supabase_get_object_info_public_request(path)?
        };
        self.sign(&mut req)?;
        self.send(req).await
    }

    pub async fn supabase_delete_object(&self, path: &str) -> Result<Response<Buffer>> {
        let mut req = self.supabase_delete_object_request(path)?;
        self.sign(&mut req)?;
        self.send(req).await
    }
}
