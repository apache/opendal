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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;

use async_trait::async_trait;
use http::header;
use http::Request;
use http::StatusCode;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

use super::error::parse_error;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct D1Builder {
    root: Option<String>,
    endpoint: Option<String>,
    sql: Option<String>,
    params: Option<Vec<String>>,
    token: Option<String>,
    http_client: Option<HttpClient>,
}

impl Debug for D1Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("D1Builder");
        ds.field("endpoint", &self.endpoint);
        ds.field("sql", &self.sql);
        ds.field("params", &self.params);
        ds.field("root", &self.root);
        ds.finish()
    }
}

impl D1Builder {
    /// Set endpoint for http backend.
    ///
    /// For more information, please refer to [D1 Database API](https://developers.cloudflare.com/api/operations/cloudflare-d1-query-database)
    /// default: "https://api.cloudflare.com/client/v4"
    pub fn endpoint(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.endpoint = Some(v.trim_end_matches('/').to_string());
        }
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_owned());
        }
        self
    }

    /// Set D1 execution sql.
    pub fn sql(&mut self, sql: &str) -> &mut Self {
        if !sql.is_empty() {
            self.sql = Some(sql.to_string());
        }
        self
    }

    /// Set the sql value field of the d1 service.
    ///
    /// default: vec![]
    pub fn params(&mut self, params: Vec<String>) -> &mut Self {
        if !params.is_empty() {
            self.params = Some(params);
        }
        self
    }

    /// Set the bearer token for the d1 service.
    /// create a bearer token from [here](https://dash.cloudflare.com/profile/api-tokens)
    pub fn token(&mut self, token: &str) -> &mut Self {
        if !token.is_empty() {
            self.token = Some(token.to_string());
        }
        self
    }
}

impl Builder for D1Builder {
    const SCHEME: Scheme = Scheme::D1;
    type Accessor = D1Backend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = D1Builder::default();
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("sql").map(|v| builder.sql(v));
        map.get("params")
            .map(|v| builder.params(v.split(",").map(|s| s.to_string()).collect()));
        map.get("root").map(|v| builder.root(v));
        map.get("token").map(|v| builder.token(v));
        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| "https://api.cloudflare.com/client/v4".to_string());

        let sql = match self.sql.clone() {
            Some(v) => v,
            None => "".to_string(),
        };

        let params = match self.params.clone() {
            Some(v) => v,
            None => vec![],
        };

        let mut auth = None;
        if let Some(token) = &self.token {
            auth = Some(format_authorization_by_bearer(token)?)
        }

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::D1)
            })?
        };

        let root = normalize_root(
            self.root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );
        Ok(D1Backend::new(Adapter {
            root: root.clone(),
            endpoint,
            sql,
            params,
            authorization: auth,
            client,
        })
        .with_root(&root))
    }
}

pub type D1Backend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    root: String,
    endpoint: String,
    sql: String,
    params: Vec<String>,
    authorization: Option<String>,
    client: HttpClient,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("D1Adapter");
        ds.field("endpoint", &self.endpoint);
        ds.field("sql", &self.sql);
        ds.field("params", &self.params);
        ds.finish()
    }
}

impl Adapter {
    fn create_d1_query_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);
        let url: String = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::post(&url);
        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }
        req = req.header(header::CONTENT_TYPE, "application/json");

        let json = serde_json::json!({
            "sql": self.sql.clone(),
            "params": self.params.clone(),
        });
        let body_string = serde_json::to_string(&json).map_err(new_json_serialize_error)?;
        let body_bytes = body_string.as_bytes().to_owned();

        let req = req
            .body(AsyncBody::Bytes(body_bytes.into()))
            .map_err(new_request_build_error);
        req
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::D1,
            "D1",
            Capability {
                stat: true,
                read: true,
                write: true,
                delete: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let req = self.create_d1_query_request(path)?;
        let resp = self.client.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let body = resp.into_body().bytes().await?;
                Ok(Some(body.into()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn set(&self, path: &str, _: &[u8]) -> Result<()> {
        let req = self.create_d1_query_request(path)?;
        let resp = self.client.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let req = self.create_d1_query_request(path)?;
        let resp = self.client.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(()),
            _ => Err(parse_error(resp).await?),
        }
    }
}
