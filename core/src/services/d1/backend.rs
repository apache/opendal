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
use serde_json::{de, Value};

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

use super::error::parse_error;
use super::model::D1Response;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct D1Builder {
    token: Option<String>,
    account_identifier: Option<String>,
    database_identifier: Option<String>,

    endpoint: Option<String>,
    http_client: Option<HttpClient>,
    root: Option<String>,

    table: Option<String>,
    key_field: Option<String>,
    value_field: Option<String>,
}

impl Debug for D1Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("D1Builder");
        ds.field("endpoint", &self.endpoint);
        ds.field("root", &self.root);
        ds.field("table", &self.table);
        ds.field("key_field", &self.key_field);
        ds.field("value_field", &self.value_field);
        ds.finish()
    }
}

impl D1Builder {
    /// Set api token for the cloudflare d1 service.
    ///
    /// create a api token from [here](https://dash.cloudflare.com/profile/api-tokens)
    pub fn token(&mut self, token: &str) -> &mut Self {
        if !token.is_empty() {
            self.token = Some(token.to_string());
        }
        self
    }

    /// Set the account identifier for the cloudflare d1 service.
    ///
    /// get the account identifier from Workers & Pages -> Overview -> Account ID
    /// default: "account-identifier"
    pub fn account_identifier(&mut self, account_identifier: &str) -> &mut Self {
        if !account_identifier.is_empty() {
            self.account_identifier = Some(account_identifier.to_string());
        }
        self
    }

    /// Set the database identifier for the cloudflare d1 service.
    ///
    /// get the database identifier from Workers & Pages -> D1 -> [Your Database] -> Database ID
    /// default: "database-identifier"
    pub fn database_identifier(&mut self, database_identifier: &str) -> &mut Self {
        if !database_identifier.is_empty() {
            self.database_identifier = Some(database_identifier.to_string());
        }
        self
    }

    /// Set endpoint for http backend.
    ///
    /// For more information, please refer to [D1 Database API](https://developers.cloudflare.com/api/operations/cloudflare-d1-query-database)
    /// default: "https://api.cloudflare.com/client/v4"
    pub fn endpoint(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.endpoint = Some(v.to_string());
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

    /// Set the table name of the d1 service to read/write.
    ///
    /// Default to `kv` if not specified.
    pub fn table(&mut self, table: &str) -> &mut Self {
        if !table.is_empty() {
            self.table = Some(table.to_owned());
        }
        self
    }

    /// Set the key field name of the d1 service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(&mut self, key_field: &str) -> &mut Self {
        if !key_field.is_empty() {
            self.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the d1 service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(&mut self, value_field: &str) -> &mut Self {
        if !value_field.is_empty() {
            self.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for D1Builder {
    const SCHEME: Scheme = Scheme::D1;
    type Accessor = D1Backend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = D1Builder::default();
        map.get("token").map(|v| builder.token(v));
        map.get("account_identifier")
            .map(|v| builder.account_identifier(v));
        map.get("database_identifier")
            .map(|v| builder.database_identifier(v));

        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("root").map(|v| builder.root(v));

        map.get("table").map(|v| builder.table(v));
        map.get("key_field").map(|v| builder.key_field(v));
        map.get("value_field").map(|v| builder.value_field(v));
        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let mut authorization = None;
        if let Some(token) = &self.token {
            authorization = Some(format_authorization_by_bearer(token)?)
        }

        let account_identifier = self
            .account_identifier
            .clone()
            .unwrap_or_else(|| "account-identifier".to_string());

        let database_identifier = self
            .database_identifier
            .clone()
            .unwrap_or_else(|| "database-identifier".to_string());

        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| "https://api.cloudflare.com/client/v4".to_string());

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::D1)
            })?
        };

        let table = self.table.clone().unwrap_or_else(|| "kv".to_string());

        let key_field = self.key_field.clone().unwrap_or_else(|| "key".to_string());

        let value_field = self
            .value_field
            .clone()
            .unwrap_or_else(|| "value".to_string());

        let root = normalize_root(
            self.root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );
        Ok(D1Backend::new(Adapter {
            authorization,
            account_identifier,
            database_identifier,
            endpoint,
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
    account_identifier: String,
    database_identifier: String,

    endpoint: String,
    client: HttpClient,

    table: String,
    key_field: String,
    value_field: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("D1Adapter");
        ds.field("endpoint", &self.endpoint);
        ds.field("table", &self.table);
        ds.field("key_field", &self.key_field);
        ds.field("value_field", &self.value_field);
        ds.finish()
    }
}

impl Adapter {
    fn create_d1_query_request(&self, sql: &str, params: Vec<Value>) -> Result<Request<AsyncBody>> {
        let p = format!(
            "/accounts/{}/d1/database/{}/query",
            self.account_identifier, self.database_identifier
        );
        let url: String = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::post(&url);
        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }
        req = req.header(header::CONTENT_TYPE, "application/json");

        let json = serde_json::json!({
            "sql": sql,
            "params": params,
        });

        let body_string = serde_json::to_string(&json).map_err(new_json_serialize_error)?;
        let body_bytes = body_string.as_bytes().to_owned();
        req.body(AsyncBody::Bytes(body_bytes.into()))
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

        let resp = self.client.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let body = resp.into_body().bytes().await?;
                let body = de::from_slice::<D1Response>(&body);
                if let Ok(body) = body {
                    if body.success {
                        if let Some(result) = body.result.get(0) {
                            if let Some(value) = result.results.get(0) {
                                match value {
                                    Value::Object(s) => {
                                        let value = s.get(&self.value_field);
                                        match value {
                                            Some(Value::Array(s)) => {
                                                let mut v = Vec::new();
                                                for i in s {
                                                    if let Value::Number(n) = i {
                                                        v.push(n.as_u64().unwrap() as u8);
                                                    }
                                                }
                                                return Ok(Some(v));
                                            }
                                            _ => return Ok(None),
                                        }
                                    }
                                    _ => return Ok(None),
                                }
                            }
                        }
                    }
                }
                Ok(None)
            }
            _ => Err(parse_error(resp).await?),
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

        let resp = self.client.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let query = format!("DELETE FROM {} WHERE {} = ?", self.table, self.key_field);
        let req = self.create_d1_query_request(&query, vec![path.into()])?;

        let resp = self.client.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(()),
            _ => Err(parse_error(resp).await?),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_something_async() -> Result<()> {
        let mut builder = D1Builder::default();
        builder
            .token("AvVw_T7HbYZz-tWpVV7ytwQqFkD0IPv60grGLA_v")
            .account_identifier("b386f5d906b87949002b545dec889cd5")
            .database_identifier("16aba954-2a17-4dd5-94bc-bbea9232a889")
            .table("Customers")
            .key_field("CustomerID")
            .value_field("CompanyName");

        let op = Operator::new(builder)?.finish();
        let source_path = "ALFKI";
        // set value to d1 "opendal test value" as Vec<u8>
        let value = "opendal test value".as_bytes();
        // write value to d1, the key is source_path
        op.write(source_path, value).await?;
        // read value from d1, the key is source_path
        let v = op.read(source_path).await?;
        assert_eq!(v, value);
        // delete value from d1, the key is source_path
        op.delete(source_path).await?;
        Ok(())
    }
}
