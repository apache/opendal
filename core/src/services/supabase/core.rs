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

use anyhow::anyhow;
use bytes::Bytes;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::HeaderValue;
use http::Request;
use http::Response;
use serde_json::json;

use crate::raw::*;
use crate::*;

mod constants {
    pub const AUTH_HEADER_KEY: &str = "authorization";
}

pub struct SupabaseCore {
    pub root: String,
    pub bucket: String,
    pub endpoint: String,

    /// The key used for authorization, initialized by environment variable you designated.
    /// Normally it is rather an anon_key(Client key) or an service_role_key(Secret Key)
    pub auth_key: Option<HeaderValue>,

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
    pub fn new(root: &str, bucket: &str, endpoint: &str, client: HttpClient) -> Self {
        Self {
            root: root.to_string(),
            bucket: bucket.to_string(),
            endpoint: endpoint.to_string(),
            auth_key: None,
            http_client: client,
        }
    }

    pub fn load_auth_key(&mut self, env: &str) {
        if let Ok(v) = std::env::var(env) {
            self.auth_key = Some(HeaderValue::from_str(&v).unwrap());
        }
    }

    pub fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        if let Some(k) = &self.auth_key {
            let v = HeaderValue::from_str(&format!("Bearer {}", k.to_str().unwrap())).unwrap();
            req.headers_mut().insert(constants::AUTH_HEADER_KEY, v);
            Ok(())
        } else {
            Err(new_request_sign_error(anyhow!(
                "The anon key is not loaded"
            )))
        }
    }
}

// requests
impl SupabaseCore {
    // ?: this defaults the bucket id to be the bucket name
    pub fn supabase_create_bucket_request(&self) -> Result<Request<AsyncBody>> {
        let url = format!("{}/bucket/", self.endpoint);
        let req = Request::post(&url);
        let body = json!({
            "name": self.bucket,
            "id": self.bucket,
            "public": true,
            "file_size_limit": 0,
            "allowed_mime_types": [
                "string"
            ]
        })
        .to_string();

        let req = req
            .body(AsyncBody::Bytes(Bytes::from(body)))
            .map_err(new_request_build_error)?;

        Ok(req)
    }

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

    pub fn supabase_get_object_public_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        self.supabase_get_object_request(path, false)
    }

    pub fn supabase_get_object_auth_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        self.supabase_get_object_request(path, true)
    }

    pub fn supabase_get_object_info_public_request(
        &self,
        path: &str,
    ) -> Result<Request<AsyncBody>> {
        self.supabase_get_object_info_request(path, false)
    }

    pub fn supabase_get_object_info_auth_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        self.supabase_get_object_info_request(path, true)
    }

    fn supabase_get_object_request(&self, path: &str, auth: bool) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/storage/v1/object/{}/{}/{}",
            self.endpoint,
            if auth { "authenticated" } else { "public" },
            self.bucket,
            percent_encode_path(&p)
        );

        let req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error);
        req
    }

    fn supabase_get_object_info_request(
        &self,
        path: &str,
        auth: bool,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/storage/v1/object/info/{}/{}/{}",
            self.endpoint,
            if auth { "authenticated" } else { "public" },
            self.bucket,
            percent_encode_path(&p)
        );

        let req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error);
        req
    }
}

// core utils
impl SupabaseCore {
    pub async fn send(&self, req: Request<AsyncBody>) -> Result<Response<IncomingAsyncBody>> {
        self.http_client.send(req).await
    }

    pub async fn supabase_create_bucket(&self) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.supabase_create_bucket_request()?;

        self.sign(&mut req)?;

        self.send(req).await
    }

    pub async fn supabase_get_object_public(
        &self,
        path: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        self.supabase_get_object(path, false).await
    }

    pub async fn supabase_get_object_auth(
        &self,
        path: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        self.supabase_get_object(path, true).await
    }

    pub async fn supabase_get_object_info_public(
        &self,
        path: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        self.supabase_get_object_info(path, false).await
    }

    pub async fn supabase_get_object_info_auth(
        &self,
        path: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        self.supabase_get_object_info(path, true).await
    }

    async fn supabase_get_object(
        &self,
        path: &str,
        auth: bool,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.supabase_get_object_request(path, auth)?;
        self.sign(&mut req)?;
        self.send(req).await
    }

    async fn supabase_get_object_info(
        &self,
        path: &str,
        auth: bool,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.supabase_get_object_info_request(path, auth)?;
        self.sign(&mut req)?;
        self.send(req).await
    }
}
