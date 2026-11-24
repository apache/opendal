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

use http::Request;
use http::StatusCode;
use http::header;
use serde_json::Value;

use super::model::*;
use opendal_core::raw::*;
use opendal_core::*;

#[derive(Clone)]
pub struct D1Core {
    pub authorization: Option<String>,
    pub account_id: String,
    pub database_id: String,

    pub table: String,
    pub key_field: String,
    pub value_field: String,
}

impl Debug for D1Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("D1Core")
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish_non_exhaustive()
    }
}

const CLOUDFLARE_API_BASE_URL: &str = "https://api.cloudflare.com/client/v4";

pub(crate) fn quote_ident(ident: &str) -> Result<String> {
    if ident.contains('\0') {
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            "d1 sql identifier must not contain NUL bytes",
        ));
    }
    Ok(format!("\"{}\"", ident.replace('"', "\"\"")))
}

impl D1Core {
    fn create_d1_query_request(
        &self,
        sql: &str,
        params: Vec<Value>,
        op: Operation,
        service_operation: &'static str,
    ) -> Result<Request<Buffer>> {
        let p = format!(
            "/accounts/{}/d1/database/{}/query",
            self.account_id, self.database_id
        );
        let url: String = format!("{}{}", CLOUDFLARE_API_BASE_URL, percent_encode_path(&p));

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
        req.extension(op)
            .extension(ServiceOperation(service_operation))
            .body(Buffer::from(body))
            .map_err(new_request_build_error)
    }

    pub async fn get(&self, ctx: &OperationContext, path: &str) -> Result<Option<Buffer>> {
        // d1 follows SQLite SQL syntax, we use `"` for identifier quote. A quoted identifier is case-sensitive and
        // can contain special characters.
        // Read more https://www.sqlite.org/quirks.html#double_quoted_string_literals
        //
        // We use identifier quoting with doubled `"` escaping for table and field configuration.
        let value_field = quote_ident(&self.value_field)?;
        let table = quote_ident(&self.table)?;
        let key_field = quote_ident(&self.key_field)?;
        let query = format!("SELECT {value_field} FROM {table} WHERE {key_field} = ? LIMIT 1");
        let req =
            self.create_d1_query_request(&query, vec![path.into()], Operation::Read, "Get")?;

        let resp = ctx.http_transport().send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let body = resp.into_body();
                let bs = body.to_bytes();
                let d1_response = D1Response::parse(&bs)?;
                d1_response.get_result(&self.value_field)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Query")),
                resp,
            )),
        }
    }

    pub async fn get_length(&self, ctx: &OperationContext, path: &str) -> Result<Option<usize>> {
        let value_field = quote_ident(&self.value_field)?;
        let table = quote_ident(&self.table)?;
        let key_field = quote_ident(&self.key_field)?;
        let query = format!(
            r#"SELECT LENGTH(CAST({value_field} AS BLOB)) AS "content_length" FROM {table} WHERE {key_field} = ? LIMIT 1"#
        );
        let req =
            self.create_d1_query_request(&query, vec![path.into()], Operation::Stat, "Stat")?;

        let resp = ctx.http_transport().send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let body = resp.into_body();
                let bs = body.to_bytes();
                let d1_response = D1Response::parse(&bs)?;
                d1_response.get_usize_result("content_length")
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Query")),
                resp,
            )),
        }
    }

    pub async fn set(&self, ctx: &OperationContext, path: &str, value: Buffer) -> Result<()> {
        let table = quote_ident(&self.table)?;
        let key_field = quote_ident(&self.key_field)?;
        let value_field = quote_ident(&self.value_field)?;
        let query = format!(
            r#"INSERT INTO {table} ({key_field}, {value_field})
                VALUES (?, ?)
                ON CONFLICT ({key_field})
                    DO UPDATE SET {value_field} = EXCLUDED.{value_field}"#,
        );

        let params = vec![path.into(), value.to_vec().into()];
        let req = self.create_d1_query_request(&query, params, Operation::Write, "Set")?;

        let resp = ctx.http_transport().send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let body = resp.into_body();
                let bs = body.to_bytes();
                let _ = D1Response::parse(&bs)?;
                Ok(())
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Query")),
                resp,
            )),
        }
    }

    pub async fn delete(&self, ctx: &OperationContext, path: &str) -> Result<()> {
        let table = quote_ident(&self.table)?;
        let key_field = quote_ident(&self.key_field)?;
        let query = format!("DELETE FROM {table} WHERE {key_field} = ?");
        let req =
            self.create_d1_query_request(&query, vec![path.into()], Operation::Delete, "Delete")?;

        let resp = ctx.http_transport().send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let body = resp.into_body();
                let bs = body.to_bytes();
                let _ = D1Response::parse(&bs)?;
                Ok(())
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Query")),
                resp,
            )),
        }
    }
}

use bytes::Buf;
use http::Response;
use serde_json::de;

/// Context needed to classify an error from this service.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ErrorContext {
    service_operation: ServiceOperation,
}

impl ErrorContext {
    pub(crate) const fn new(service_operation: ServiceOperation) -> Self {
        Self { service_operation }
    }
}

/// Parse an error response using its service request context.
pub(crate) fn parse_error(ctx: ErrorContext, resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let (mut kind, mut retryable) = match parts.status {
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        // Some services (like owncloud) return 403 while file locked.
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, true),
        // Allowing retry for resource locked.
        StatusCode::LOCKED => (ErrorKind::Unexpected, true),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let (message, d1_err) = de::from_reader::<_, D1Response>(bs.clone().reader())
        .map(|d1_err| (format!("{d1_err:?}"), Some(d1_err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(d1_err) = d1_err {
        (kind, retryable) = parse_d1_error_code(&d1_err.errors).unwrap_or((kind, retryable));
    }

    let mut err = Error::new(kind, message);

    err = err.with_context("service_operation", ctx.service_operation.0);
    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}

pub fn parse_d1_error_code(errors: &[D1Error]) -> Option<(ErrorKind, bool)> {
    if errors.is_empty() {
        return None;
    }

    match errors[0].code {
        // The request is malformed: failed to decode id.
        7400 => Some((ErrorKind::Unexpected, false)),
        // no such column: Xxxx.
        7500 => Some((ErrorKind::NotFound, false)),
        // Authentication error.
        10000 => Some((ErrorKind::PermissionDenied, false)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::stream;

    use super::*;

    struct D1MockTransport {
        sent_sql: Arc<Mutex<Vec<String>>>,
        responses: Mutex<VecDeque<(StatusCode, String)>>,
    }

    impl D1MockTransport {
        fn new(responses: Vec<(StatusCode, &str)>) -> (Self, Arc<Mutex<Vec<String>>>) {
            let sent_sql = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    sent_sql: sent_sql.clone(),
                    responses: Mutex::new(
                        responses
                            .into_iter()
                            .map(|(status, body)| (status, body.to_string()))
                            .collect(),
                    ),
                },
                sent_sql,
            )
        }
    }

    impl HttpTransport for D1MockTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let payload: Value =
                serde_json::from_slice(&req.into_body().to_bytes()).expect("valid JSON request");
            if let Some(sql) = payload.get("sql").and_then(Value::as_str) {
                self.sent_sql.lock().unwrap().push(sql.to_string());
            }

            let (status, body_str) = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra D1 HTTP request");
            let body = Buffer::from(body_str);
            let len = body.len() as u64;
            Response::builder()
                .status(status)
                .body(HttpBody::new(stream::iter([Ok(body)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    fn test_core(table: &str, key_field: &str, value_field: &str) -> D1Core {
        D1Core {
            authorization: Some("Bearer d1-sentinel-secret".to_string()),
            account_id: "acc123".to_string(),
            database_id: "db456".to_string(),
            table: table.to_string(),
            key_field: key_field.to_string(),
            value_field: value_field.to_string(),
        }
    }

    #[tokio::test]
    async fn set_and_delete_fail_on_200_ok_with_success_false() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let core = test_core("kv", "key", "value");

            let (transport, _) = D1MockTransport::new(vec![
                (
                    StatusCode::OK,
                    r#"{"success":false,"result":[],"errors":[{"code":7500,"message":"no such column"}]}"#,
                ),
                (
                    StatusCode::OK,
                    r#"{"success":false,"result":[],"errors":[{"code":10000,"message":"Authentication error"}]}"#,
                ),
            ]);
            let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(transport));

            let set_err = core
                .set(&ctx, "k1", Buffer::from("hello"))
                .await
                .unwrap_err();
            assert_eq!(set_err.kind(), ErrorKind::Unexpected);

            let del_err = core.delete(&ctx, "k1").await.unwrap_err();
            assert_eq!(del_err.kind(), ErrorKind::Unexpected);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn queries_escape_double_quotes_in_sql_identifiers() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let core = test_core(r#"my"table"#, r#"key"col"#, r#"val"col"#);

            let (transport, sent_sql) = D1MockTransport::new(vec![
                (
                    StatusCode::OK,
                    r#"{"success":true,"result":[{"results":[{"val\"col":[65,66]}]}],"errors":[]}"#,
                ),
                (
                    StatusCode::OK,
                    r#"{"success":true,"result":[{"results":[{"content_length":2}]}],"errors":[]}"#,
                ),
                (
                    StatusCode::OK,
                    r#"{"success":true,"result":[{"results":[]}],"errors":[]}"#,
                ),
                (
                    StatusCode::OK,
                    r#"{"success":true,"result":[{"results":[]}],"errors":[]}"#,
                ),
            ]);
            let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(transport));

            let got = core.get(&ctx, "p").await.unwrap().unwrap();
            assert_eq!(got.to_bytes().as_ref(), b"AB");
            assert_eq!(core.get_length(&ctx, "p").await.unwrap(), Some(2));
            core.set(&ctx, "p", Buffer::from("AB")).await.unwrap();
            core.delete(&ctx, "p").await.unwrap();

            let sqls = sent_sql.lock().unwrap();
            assert_eq!(sqls.len(), 4);
            assert_eq!(
                sqls[0],
                r#"SELECT "val""col" FROM "my""table" WHERE "key""col" = ? LIMIT 1"#
            );
            assert_eq!(
                sqls[1],
                r#"SELECT LENGTH(CAST("val""col" AS BLOB)) AS "content_length" FROM "my""table" WHERE "key""col" = ? LIMIT 1"#
            );
            assert!(sqls[2].contains(r#"INSERT INTO "my""table" ("key""col", "val""col")"#));
            assert!(sqls[2].contains(r#"ON CONFLICT ("key""col")"#));
            assert!(sqls[2].contains(r#"DO UPDATE SET "val""col" = EXCLUDED."val""col""#));
            assert_eq!(sqls[3], r#"DELETE FROM "my""table" WHERE "key""col" = ?"#);
        })
        .await
        .unwrap();
    }
}
