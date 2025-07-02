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

use std::sync::Arc;
use std::time::Duration;

use http::header;
use http::request;
use http::Request;
use http::Response;
use serde_json::json;

use crate::raw::new_json_serialize_error;
use crate::raw::percent_encode_path;
use crate::raw::Operation;
use crate::raw::QueryPairsWriter;
use crate::raw::{new_request_build_error, AccessorInfo, FormDataPart, Multipart};
// use crate::services::cloudflare_kv::model::CfKvGetPayload;
// use crate::services::cloudflare_kv::model::CfKvGetPayloadType;
use crate::services::cloudflare_kv::model::CfKvMetadata;
use crate::{Buffer, Result};

#[derive(Debug, Clone)]
pub struct CloudflareKvCore {
    pub api_token: String,
    pub account_id: String,
    pub namespace_id: String,
    pub expiration_ttl: Option<Duration>,
    pub info: Arc<AccessorInfo>,
}

impl CloudflareKvCore {
    #[inline]
    async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.info.http_client().send(req).await
    }

    fn sign(&self, req: request::Builder) -> request::Builder {
        req.header(header::AUTHORIZATION, &self.api_token)
    }

    fn url_prefix(&self) -> String {
        let url = format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{}",
            self.account_id, self.namespace_id
        );
        url
    }
}

impl CloudflareKvCore {
    pub async fn metadata(&self, path: &str) -> Result<Response<Buffer>> {
        let url = format!(
            "{}/metadata/{}",
            self.url_prefix(),
            percent_encode_path(path)
        );

        let req = Request::get(url);
        let req = self.sign(req);

        let req = req
            .extension(Operation::Stat)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn get(&self, path: &str) -> Result<Response<Buffer>> {
        let url = format!("{}/values/{}", self.url_prefix(), percent_encode_path(path));
        let req = Request::get(url);

        let req = self.sign(req);

        let req = req
            .extension(Operation::Read)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    // // The batch_get operation is currently disabled due to response 400 error
    // pub async fn batch_get(&self, paths: &[String]) -> Result<Response<Buffer>> {
    //     let url = format!("{}/bulk/get", self.url_prefix());
    //     let req = Request::post(url);

    //     let req = self.sign(req);

    //     // let paths = paths
    //     //     .iter()
    //     //     .map(|p| percent_encode_path(p))
    //     //     .collect::<Vec<String>>();

    //     let payload = CfKvGetPayload {
    //         keys: paths.to_vec(),
    //         get_type: Some(CfKvGetPayloadType::Json),
    //         with_metadata: Some(true),
    //     };

    //     let body_bytes = serde_json::to_vec(&payload).map_err(new_json_serialize_error)?;
    //     let body = Buffer::from(body_bytes);

    //     let req = req
    //         .extension(Operation::Read)
    //         .header(header::CONTENT_TYPE, "application/json")
    //         .body(body)
    //         .map_err(new_request_build_error)?;

    //     self.send(req).await
    // }

    pub async fn set(
        &self,
        path: &str,
        value: Buffer,
        metadata: CfKvMetadata,
    ) -> Result<Response<Buffer>> {
        let url = format!("{}/values/{}", self.url_prefix(), percent_encode_path(path));

        let req = Request::put(url);
        let req = self.sign(req);
        let req = req.extension(Operation::Write);

        let mut multipart = Multipart::new()
            .part(FormDataPart::new("value").content(value))
            .part(
                FormDataPart::new("metadata")
                    .content(serde_json::to_string(&metadata).map_err(new_json_serialize_error)?),
            );

        if let Some(expiration_ttl) = self.expiration_ttl {
            multipart = multipart.part(
                FormDataPart::new("expiration_ttl").content(expiration_ttl.as_secs().to_string()),
            );
        }

        let req = multipart.apply(req)?;

        // let resp = self.send(req).await?;
        // println!("QAQ resp {:?}", String::from_utf8_lossy(&resp.body().to_bytes()));

        // Ok(Response::new(Buffer::new()))
        self.send(req).await
    }

    pub async fn delete(&self, paths: &[String]) -> Result<Response<Buffer>> {
        let url = format!("{}/bulk/delete", self.url_prefix());

        let req = Request::post(&url);

        let req = self.sign(req);
        let req_body = &json!(paths);
        let req = req
            .extension(Operation::Delete)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Buffer::from(req_body.to_string()))
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn list(
        &self,
        prefix: &str,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> Result<Response<Buffer>> {
        let url = format!("{}/keys", self.url_prefix());
        let mut url = QueryPairsWriter::new(&url);
        if let Some(cursor) = cursor {
            if !cursor.is_empty() {
                url = url.push("cursor", &cursor);
            }
        }
        url = url.push("limit", &limit.unwrap_or(1000).to_string());
        url = url.push("prefix", &percent_encode_path(prefix));

        let req = Request::get(url.finish());

        let req = self.sign(req);
        let req = req
            .extension(Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }
}
