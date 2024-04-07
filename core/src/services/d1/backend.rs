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
use serde::Deserialize;
use serde_json::Value;

use super::error::parse_error;
use super::model::D1Response;
use crate::raw::adapters::kv;
use crate::raw::*;
use crate::ErrorKind;
use crate::*;

/// Config for [Cloudflare D1](https://developers.cloudflare.com/d1) backend support.
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct D1Config {
    /// Set the token of cloudflare api.
    pub token: Option<String>,
    /// Set the account id of cloudflare api.
    pub account_id: Option<String>,
    /// Set the database id of cloudflare api.
    pub database_id: Option<String>,

    /// Set the working directory of OpenDAL.
    pub root: Option<String>,
    /// Set the table of D1 Database.
    pub table: Option<String>,
    /// Set the key field of D1 Database.
    pub key_field: Option<String>,
    /// Set the value field of D1 Database.
    pub value_field: Option<String>,
}

impl Debug for D1Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("D1Config");
        ds.field("root", &self.root);
        ds.field("table", &self.table);
        ds.field("key_field", &self.key_field);
        ds.field("value_field", &self.value_field);
        ds.finish_non_exhaustive()
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct D1Builder {
    config: D1Config,

    http_client: Option<HttpClient>,
}

impl Debug for D1Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("D1Builder")
            .field("config", &self.config)
            .finish()
    }
}

impl D1Builder {
    /// Set api token for the cloudflare d1 service.
    ///
    /// create a api token from [here](https://dash.cloudflare.com/profile/api-tokens)
    pub fn token(&mut self, token: &str) -> &mut Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }
        self
    }

    /// Set the account identifier for the cloudflare d1 service.
    ///
    /// get the account identifier from Workers & Pages -> Overview -> Account ID
    /// If not specified, it will return an error when building.
    pub fn account_id(&mut self, account_id: &str) -> &mut Self {
        if !account_id.is_empty() {
            self.config.account_id = Some(account_id.to_string());
        }
        self
    }

    /// Set the database identifier for the cloudflare d1 service.
    ///
    /// get the database identifier from Workers & Pages -> D1 -> [Your Database] -> Database ID
    /// If not specified, it will return an error when building.
    pub fn database_id(&mut self, database_id: &str) -> &mut Self {
        if !database_id.is_empty() {
            self.config.database_id = Some(database_id.to_string());
        }
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.config.root = Some(root.to_owned());
        }
        self
    }

    /// Set the table name of the d1 service to read/write.
    ///
    /// If not specified, it will return an error when building.
    pub fn table(&mut self, table: &str) -> &mut Self {
        if !table.is_empty() {
            self.config.table = Some(table.to_owned());
        }
        self
    }

    /// Set the key field name of the d1 service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(&mut self, key_field: &str) -> &mut Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the d1 service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(&mut self, value_field: &str) -> &mut Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for D1Builder {
    const SCHEME: Scheme = Scheme::D1;
    type Accessor = D1Backend;

    fn from_map(map: HashMap<String, String>) -> Self {
        Self {
            config: D1Config::deserialize(ConfigDeserializer::new(map))
                .expect("config deserialize must succeed"),
            ..Default::default()
        }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let mut authorization = None;
        let config = &self.config;
        if let Some(token) = &config.token {
            authorization = Some(format_authorization_by_bearer(token)?)
        }

        let Some(account_id) = config.account_id.clone() else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "account_id is required",
            ));
        };

        let Some(database_id) = config.database_id.clone() else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "database_id is required",
            ));
        };

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::D1)
            })?
        };

        let Some(table) = config.table.clone() else {
            return Err(Error::new(ErrorKind::ConfigInvalid, "table is required"));
        };

        let key_field = config
            .key_field
            .clone()
            .unwrap_or_else(|| "key".to_string());

        let value_field = config
            .value_field
            .clone()
            .unwrap_or_else(|| "value".to_string());

        let root = normalize_root(
            config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );
        Ok(D1Backend::new(Adapter {
            authorization,
            account_id,
            database_id,
            client,
            table,
            key_field,
            value_field,
        })
        .with_root(&root))
    }
}

pub type D1Backend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    authorization: Option<String>,
    account_id: String,
    database_id: String,

    client: HttpClient,
    table: String,
    key_field: String,
    value_field: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("D1Adapter");
        ds.field("table", &self.table);
        ds.field("key_field", &self.key_field);
        ds.field("value_field", &self.value_field);
        ds.finish()
    }
}

impl Adapter {
    fn create_d1_query_request(
        &self,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<Request<RequestBody>> {
        let p = format!(
            "/accounts/{}/d1/database/{}/query",
            self.account_id, self.database_id
        );
        let url: String = format!(
            "{}{}",
            "https://api.cloudflare.com/client/v4",
            percent_encode_path(&p)
        );

        let mut req = Request::post(&url);
        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }
        req = req.header(header::CONTENT_TYPE, "application/json");

        let json = serde_json::json!({
            "sql": sql,
            "params": params,
        });

        let body = serde_json::to_vec(&json).map_err(new_json_serialize_error)?;
        req.body(RequestBody::Bytes(body.into()))
            .map_err(new_request_build_error)
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::D1,
            &self.table,
            Capability {
                read: true,
                write: true,
                // Cloudflare D1 supports 1MB as max in write_total.
                // refer to https://developers.cloudflare.com/d1/platform/limits/
                write_total_max_size: Some(1000 * 1000),
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let query = format!(
            "SELECT {} FROM {} WHERE {} = ? LIMIT 1",
            self.value_field, self.table, self.key_field
        );
        let req = self.create_d1_query_request(&query, vec![path.into()])?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let response: D1Response = body.to_json().await?;
                if !response.success {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        &format!("get from d1 failed for {response:?}"),
                    ));
                }
                Ok(response.get_result(&self.value_field))
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        let table = &self.table;
        let key_field = &self.key_field;
        let value_field = &self.value_field;
        let query = format!(
            "INSERT INTO {table} ({key_field}, {value_field}) \
                VALUES (?, ?) \
                ON CONFLICT ({key_field}) \
                    DO UPDATE SET {value_field} = EXCLUDED.{value_field}",
        );

        let params = vec![path.into(), value.into()];
        let req = self.create_d1_query_request(&query, params)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(()),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let query = format!("DELETE FROM {} WHERE {} = ?", self.table, self.key_field);
        let req = self.create_d1_query_request(&query, vec![path.into()])?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(()),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }
}
