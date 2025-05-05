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
use std::fmt::Formatter;

use bytes::Buf;
use http::header;
use http::Request;
use http::StatusCode;
use serde::Deserialize;

use super::error::parse_error;
use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::CloudflareKvConfig;
use crate::ErrorKind;
use crate::*;

impl Configurator for CloudflareKvConfig {
    type Builder = CloudflareKvBuilder;
    fn into_builder(self) -> Self::Builder {
        CloudflareKvBuilder {
            config: self,
            http_client: None,
        }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct CloudflareKvBuilder {
    config: CloudflareKvConfig,

    /// The HTTP client used to communicate with CloudFlare.
    http_client: Option<HttpClient>,
}

impl Debug for CloudflareKvBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudFlareKvBuilder")
            .field("config", &self.config)
            .finish()
    }
}

impl CloudflareKvBuilder {
    /// Set the token used to authenticate with CloudFlare.
    pub fn token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string())
        }
        self
    }

    /// Set the account ID used to authenticate with CloudFlare.
    pub fn account_id(mut self, account_id: &str) -> Self {
        if !account_id.is_empty() {
            self.config.account_id = Some(account_id.to_string())
        }
        self
    }

    /// Set the namespace ID.
    pub fn namespace_id(mut self, namespace_id: &str) -> Self {
        if !namespace_id.is_empty() {
            self.config.namespace_id = Some(namespace_id.to_string())
        }
        self
    }

    /// Set the root within this backend.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }
}

impl Builder for CloudflareKvBuilder {
    const SCHEME: Scheme = Scheme::CloudflareKv;
    type Config = CloudflareKvConfig;

    fn build(self) -> Result<impl Access> {
        let authorization = match &self.config.token {
            Some(token) => format_authorization_by_bearer(token)?,
            None => return Err(Error::new(ErrorKind::ConfigInvalid, "token is required")),
        };

        let Some(account_id) = self.config.account_id.clone() else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "account_id is required",
            ));
        };

        let Some(namespace_id) = self.config.namespace_id.clone() else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "namespace_id is required",
            ));
        };

        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::CloudflareKv)
            })?
        };

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let url_prefix = format!(
            r"https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{}",
            account_id, namespace_id
        );

        Ok(CloudflareKvBackend::new(Adapter {
            authorization,
            account_id,
            namespace_id,
            client,
            url_prefix,
        })
        .with_normalized_root(root))
    }
}

pub type CloudflareKvBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    authorization: String,
    account_id: String,
    namespace_id: String,
    client: HttpClient,
    url_prefix: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Adapter")
            .field("account_id", &self.account_id)
            .field("namespace_id", &self.namespace_id)
            .finish()
    }
}

impl Adapter {
    fn sign<T>(&self, mut req: Request<T>) -> Result<Request<T>> {
        req.headers_mut()
            .insert(header::AUTHORIZATION, self.authorization.parse().unwrap());
        Ok(req)
    }
}

impl kv::Adapter for Adapter {
    type Scanner = kv::Scanner;

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::CloudflareKv,
            &self.namespace_id,
            Capability {
                read: true,
                write: true,
                list: true,
                shared: true,

                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let url = format!("{}/values/{}", self.url_prefix, path);
        let mut req = Request::get(&url);
        req = req.header(header::CONTENT_TYPE, "application/json");
        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        req = self.sign(req)?;
        let resp = self.client.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(Some(resp.into_body())),
            _ => Err(parse_error(resp)),
        }
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let url = format!("{}/values/{}", self.url_prefix, path);
        let req = Request::put(&url);
        let multipart = Multipart::new();
        let multipart = multipart
            .part(FormDataPart::new("metadata").content(serde_json::Value::Null.to_string()))
            .part(FormDataPart::new("value").content(value.to_vec()));
        let mut req = multipart.apply(req)?;
        req = self.sign(req)?;
        let resp = self.client.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let url = format!("{}/values/{}", self.url_prefix, path);
        let mut req = Request::delete(&url);
        req = req.header(header::CONTENT_TYPE, "application/json");
        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        req = self.sign(req)?;
        let resp = self.client.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn scan(&self, path: &str) -> Result<Self::Scanner> {
        let mut url = format!("{}/keys", self.url_prefix);
        if !path.is_empty() {
            url = format!("{}?prefix={}", url, path);
        }
        let mut req = Request::get(&url);
        req = req.header(header::CONTENT_TYPE, "application/json");
        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        req = self.sign(req)?;
        let resp = self.client.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => {
                let body = resp.into_body();
                let response: CfKvScanResponse =
                    serde_json::from_reader(body.reader()).map_err(|e| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!("failed to parse error response: {}", e),
                        )
                    })?;
                Ok(Box::new(kv::ScanStdIter::new(
                    response.result.into_iter().map(|r| Ok(r.name)),
                )))
            }
            _ => Err(parse_error(resp)),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct CfKvResponse {
    pub(super) errors: Vec<CfKvError>,
}

#[derive(Debug, Deserialize)]
pub(super) struct CfKvScanResponse {
    result: Vec<CfKvScanResult>,
    // According to https://developers.cloudflare.com/api/operations/workers-kv-namespace-list-a-namespace'-s-keys, result_info is used to determine if there are more keys to be listed
    // result_info: Option<CfKvResultInfo>,
}

#[derive(Debug, Deserialize)]
struct CfKvScanResult {
    name: String,
}

// #[derive(Debug, Deserialize)]
// struct CfKvResultInfo {
//     count: i64,
//     cursor: String,
// }

#[derive(Debug, Deserialize)]
pub(super) struct CfKvError {
    pub(super) code: i32,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deserialize_scan_json_response() {
        let json_str = r#"{
			"errors": [],
			"messages": [],
			"result": [
				{
				"expiration": 1577836800,
				"metadata": {
					"someMetadataKey": "someMetadataValue"
				},
				"name": "My-Key"
				}
			],
			"success": true,
			"result_info": {
				"count": 1,
				"cursor": "6Ck1la0VxJ0djhidm1MdX2FyDGxLKVeeHZZmORS_8XeSuhz9SjIJRaSa2lnsF01tQOHrfTGAP3R5X1Kv5iVUuMbNKhWNAXHOl6ePB0TUL8nw"
			}
		}"#;

        let response: CfKvScanResponse = serde_json::from_slice(json_str.as_bytes()).unwrap();

        assert_eq!(response.result.len(), 1);
        assert_eq!(response.result[0].name, "My-Key");
        // assert!(response.result_info.is_some());
        // if let Some(result_info) = response.result_info {
        //     assert_eq!(result_info.count, 1);
        //     assert_eq!(result_info.cursor, "6Ck1la0VxJ0djhidm1MdX2FyDGxLKVeeHZZmORS_8XeSuhz9SjIJRaSa2lnsF01tQOHrfTGAP3R5X1Kv5iVUuMbNKhWNAXHOl6ePB0TUL8nw");
        // }
    }

    #[test]
    fn test_deserialize_json_response() {
        let json_str = r#"{
			"errors": [],
			"messages": [],
			"result": {},
			"success": true
		}"#;

        let response: CfKvResponse = serde_json::from_slice(json_str.as_bytes()).unwrap();

        assert_eq!(response.errors.len(), 0);
    }
}
