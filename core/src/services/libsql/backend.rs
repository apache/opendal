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
use std::str;

use async_trait::async_trait;
use bytes::Bytes;
use hrana_client_proto::pipeline::{
    ClientMsg, Response, ServerMsg, StreamExecuteReq, StreamExecuteResult, StreamRequest,
    StreamResponse, StreamResponseError, StreamResponseOk,
};
use hrana_client_proto::Error as PipelineError;
use hrana_client_proto::{Stmt, StmtResult, Value};
use http::{Request, Uri};

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

use super::error::parse_error;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct LibsqlBuilder {
    connection_string: Option<String>,
    auth_token: Option<String>,

    table: Option<String>,
    key_field: Option<String>,
    value_field: Option<String>,
    root: Option<String>,
}

impl Debug for LibsqlBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("LibsqlBuilder");
        ds.field("connection_string", &self.connection_string)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .field("root", &self.root);

        if self.auth_token.is_some() {
            ds.field("auth_token", &"<redacted>");
        }
        ds.finish()
    }
}

impl LibsqlBuilder {
    /// Set the connection_string of the libsql service.
    ///
    /// This connection string is used to connect to the libsql service. There are url based formats:
    ///
    /// ## Url
    ///
    /// This format resembles the url format of the libsql client.
    ///
    /// for a remote database connection:
    ///
    /// - `http://example.com/db`
    /// - `https://example.com/db`
    /// - `libsql://example.com/db`
    pub fn connection_string(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.connection_string = Some(v.to_string());
        }
        self
    }

    /// set the authentication token for libsql service.
    ///
    /// default: no authentication token
    pub fn auth_token(&mut self, auth_token: &str) -> &mut Self {
        if !auth_token.is_empty() {
            self.auth_token = Some(auth_token.to_owned());
        }
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string());
        }
        self
    }

    /// Set the table name of the libsql service to read/write.
    pub fn table(&mut self, table: &str) -> &mut Self {
        if !table.is_empty() {
            self.table = Some(table.to_string());
        }
        self
    }

    /// Set the key field name of the libsql service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(&mut self, key_field: &str) -> &mut Self {
        if !key_field.is_empty() {
            self.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the libsql service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(&mut self, value_field: &str) -> &mut Self {
        if !value_field.is_empty() {
            self.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for LibsqlBuilder {
    const SCHEME: Scheme = Scheme::Libsql;
    type Accessor = LibsqlBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = LibsqlBuilder::default();
        map.get("connection_string")
            .map(|v| builder.connection_string(v));
        map.get("auth_token").map(|v| builder.auth_token(v));
        map.get("table").map(|v| builder.table(v));
        map.get("key_field").map(|v| builder.key_field(v));
        map.get("value_field").map(|v| builder.value_field(v));
        map.get("root").map(|v| builder.root(v));
        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let conn = self.get_connection_string()?;

        let table = match self.table.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", Scheme::Libsql))
            }
        };
        let key_field = match self.key_field.clone() {
            Some(v) => v,
            None => "key".to_string(),
        };
        let value_field = match self.value_field.clone() {
            Some(v) => v,
            None => "value".to_string(),
        };
        let root = normalize_root(
            self.root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let client = HttpClient::new().map_err(|err| {
            err.with_operation("Builder::build")
                .with_context("service", Scheme::Libsql)
        })?;

        Ok(LibsqlBackend::new(Adapter {
            client,
            connection_string: conn,
            auth_token: self.auth_token.clone(),
            table,
            key_field,
            value_field,
        })
        .with_root(&root))
    }
}

impl LibsqlBuilder {
    fn get_connection_string(&self) -> Result<String> {
        let connection_string = self
            .connection_string
            .clone()
            .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "connection_string is empty"))?;

        let ep_url = connection_string
            .replace("libsql://", "https://")
            .parse::<Uri>()
            .map_err(|e| {
                Error::new(ErrorKind::ConfigInvalid, "connection_string is invalid")
                    .with_context("service", Scheme::Libsql)
                    .with_context("connection_string", connection_string)
                    .set_source(e)
            })?;

        match ep_url.scheme_str() {
            None => Ok(format!("https://{ep_url}/")),
            Some("http") | Some("https") => Ok(ep_url.to_string()),
            Some(s) => Err(
                Error::new(ErrorKind::ConfigInvalid, "invalid or unsupported scheme")
                    .with_context("service", Scheme::Libsql)
                    .with_context("scheme", s),
            ),
        }
    }
}

/// Backend for libsql service
pub type LibsqlBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    client: HttpClient,
    connection_string: String,
    auth_token: Option<String>,

    table: String,
    key_field: String,
    value_field: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("LibsqlAdapter");
        ds.field("connection_string", &self.connection_string)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field);

        if self.auth_token.is_some() {
            ds.field("auth_token", &"<redacted>");
        }

        ds.finish()
    }
}

impl Adapter {
    async fn execute(&self, sql: String, args: Vec<Value>) -> Result<ServerMsg> {
        let url = format!("{}v2/pipeline", self.connection_string);

        let mut req = Request::post(&url);

        if let Some(auth_token) = self.auth_token.clone() {
            req = req.header("Authorization", format!("Bearer {}", auth_token));
        }

        let msg = ClientMsg {
            baton: None,
            requests: vec![StreamRequest::Execute(StreamExecuteReq {
                stmt: Stmt {
                    sql,
                    args,
                    named_args: vec![],
                    want_rows: true,
                },
            })],
        };
        let body = serde_json::to_string(&msg).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to serialize request")
                .with_context("service", Scheme::Libsql)
                .set_source(err)
        })?;

        let req = req
            .body(AsyncBody::Bytes(Bytes::from(body)))
            .map_err(new_request_build_error)?;

        let resp = self.client.send(req).await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp).await?);
        }

        let bs = resp.into_body().bytes().await?;

        let resp: ServerMsg = serde_json::from_slice(&bs).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "deserialize json from response").set_source(e)
        })?;

        if resp.results.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Unexpected empty response from server",
            ));
        }

        if resp.results.len() > 1 {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Unexpected multiple response from server",
            ));
        }

        Ok(resp)
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Libsql,
            &self.table,
            Capability {
                read: true,
                write: true,
                create_dir: true,
                delete: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let query = format!(
            "SELECT {} FROM {} WHERE `{}` = ? LIMIT 1",
            self.value_field, self.table, self.key_field
        );
        let mut resp = self.execute(query, vec![Value::from(path)]).await?;

        match resp.results.swap_remove(0) {
            Response::Ok(StreamResponseOk {
                response:
                    StreamResponse::Execute(StreamExecuteResult {
                        result: StmtResult { cols: _, rows, .. },
                    }),
            }) => {
                if rows.is_empty() || rows[0].is_empty() {
                    return Ok(None);
                } else {
                    let val = &rows[0][0];
                    match val {
                        Value::Null => Ok(None),
                        Value::Blob { value } => Ok(Some(value.to_owned())),
                        _ => Err(Error::new(ErrorKind::Unexpected, "invalid value type")),
                    }
                }
            }
            Response::Ok(_) => Err(Error::new(
                ErrorKind::Unexpected,
                "Unexpected response from server",
            )),
            Response::Error(StreamResponseError {
                error: PipelineError { message },
            }) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("get failed: {}", message).as_str(),
            )),
        }
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        let query = format!(
            "INSERT OR REPLACE INTO `{}` (`{}`, `{}`) VALUES (?, ?)",
            self.table, self.key_field, self.value_field
        );
        let mut resp = self
            .execute(query, vec![Value::from(path), Value::from(value.to_vec())])
            .await?;
        match resp.results.swap_remove(0) {
            Response::Ok(_) => Ok(()),
            Response::Error(StreamResponseError {
                error: PipelineError { message },
            }) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("set failed: {}", message).as_str(),
            )),
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let query = format!("DELETE FROM {} WHERE `{}` = ?", self.table, self.key_field);
        let mut resp = self.execute(query, vec![Value::from(path)]).await?;
        match resp.results.swap_remove(0) {
            Response::Ok(_) => Ok(()),
            Response::Error(StreamResponseError {
                error: PipelineError { message },
            }) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("delete failed: {}", message).as_str(),
            )),
        }
    }
}
