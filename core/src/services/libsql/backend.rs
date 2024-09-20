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
use std::str;

use bytes::Buf;
use bytes::Bytes;
use hrana_client_proto::pipeline::ClientMsg;
use hrana_client_proto::pipeline::Response;
use hrana_client_proto::pipeline::ServerMsg;
use hrana_client_proto::pipeline::StreamExecuteReq;
use hrana_client_proto::pipeline::StreamExecuteResult;
use hrana_client_proto::pipeline::StreamRequest;
use hrana_client_proto::pipeline::StreamResponse;
use hrana_client_proto::pipeline::StreamResponseError;
use hrana_client_proto::pipeline::StreamResponseOk;
use hrana_client_proto::Error as PipelineError;
use hrana_client_proto::Stmt;
use hrana_client_proto::StmtResult;
use hrana_client_proto::Value;
use http::Request;
use http::Uri;

use super::error::parse_error;
use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::LibsqlConfig;
use crate::*;

impl Configurator for LibsqlConfig {
    type Builder = LibsqlBuilder;
    fn into_builder(self) -> Self::Builder {
        LibsqlBuilder { config: self }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct LibsqlBuilder {
    config: LibsqlConfig,
}

impl Debug for LibsqlBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("LibsqlBuilder");

        d.field("config", &self.config);
        d.finish()
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
    pub fn connection_string(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.connection_string = Some(v.to_string());
        }
        self
    }

    /// set the authentication token for libsql service.
    ///
    /// default: no authentication token
    pub fn auth_token(mut self, auth_token: &str) -> Self {
        if !auth_token.is_empty() {
            self.config.auth_token = Some(auth_token.to_owned());
        }
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set the table name of the libsql service to read/write.
    pub fn table(mut self, table: &str) -> Self {
        if !table.is_empty() {
            self.config.table = Some(table.to_string());
        }
        self
    }

    /// Set the key field name of the libsql service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(mut self, key_field: &str) -> Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the libsql service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(mut self, value_field: &str) -> Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for LibsqlBuilder {
    const SCHEME: Scheme = Scheme::Libsql;
    type Config = LibsqlConfig;

    fn build(self) -> Result<impl Access> {
        let conn = self.get_connection_string()?;

        let table = match self.config.table.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", Scheme::Libsql))
            }
        };
        let key_field = match self.config.key_field.clone() {
            Some(v) => v,
            None => "key".to_string(),
        };
        let value_field = match self.config.value_field.clone() {
            Some(v) => v,
            None => "value".to_string(),
        };
        let root = normalize_root(
            self.config
                .root
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
            auth_token: self.config.auth_token.clone(),
            table,
            key_field,
            value_field,
        })
        .with_normalized_root(root))
    }
}

impl LibsqlBuilder {
    fn get_connection_string(&self) -> Result<String> {
        let connection_string =
            self.config.connection_string.clone().ok_or_else(|| {
                Error::new(ErrorKind::ConfigInvalid, "connection_string is empty")
            })?;

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
            .body(Buffer::from(Bytes::from(body)))
            .map_err(new_request_build_error)?;

        let resp = self.client.send(req).await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();

        let resp: ServerMsg = serde_json::from_reader(bs.reader()).map_err(|e| {
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

impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Libsql,
            &self.table,
            Capability {
                read: true,
                write: true,
                delete: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
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
                    Ok(None)
                } else {
                    let val = &rows[0][0];
                    match val {
                        Value::Null => Ok(None),
                        Value::Blob { value } => Ok(Some(Buffer::from(value.to_vec()))),
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

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
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
