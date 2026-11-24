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
use std::sync::Arc;

use asyncband::once::OnceCell;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Database;
use surrealdb::types::Bytes;
use surrealdb::types::ErrorDetails;
use surrealdb::types::NotFoundError;
use surrealdb::types::Number;
use surrealdb::types::QueryError;
use surrealdb::types::Value;

use opendal_core::*;

#[derive(Clone)]
pub struct SurrealdbCore {
    pub db: OnceCell<Arc<Surreal<Any>>>,
    pub connection_string: String,

    pub username: String,
    pub password: String,
    pub namespace: String,
    pub database: String,

    pub table: String,
    pub key_field: String,
    pub value_field: String,
}

impl Debug for SurrealdbCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SurrealdbCore")
            .field("connection_string", &self.connection_string)
            .field("username", &self.username)
            .field("namespace", &self.namespace)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish_non_exhaustive()
    }
}

impl SurrealdbCore {
    async fn get_connection(&self) -> Result<&Surreal<Any>> {
        self.db
            .get_or_try_init(|| async {
                let namespace = self.namespace.as_str();
                let database = self.database.as_str();

                let db: Surreal<Any> = Surreal::init();
                db.connect(self.connection_string.clone())
                    .await
                    .map_err(parse_surrealdb_error)?;

                if !self.username.is_empty() && !self.password.is_empty() {
                    db.signin(Database {
                        namespace: namespace.to_string(),
                        database: database.to_string(),
                        username: self.username.clone(),
                        password: self.password.clone(),
                    })
                    .await
                    .map_err(parse_surrealdb_error)?;
                }
                db.use_ns(namespace)
                    .use_db(database)
                    .await
                    .map_err(parse_surrealdb_error)?;

                Ok(Arc::new(db))
            })
            .await
            .map(|v| v.as_ref())
    }

    pub async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let value_field = quote_identifier(&self.value_field);
        let query = if self.key_field == "id" {
            format!("SELECT {value_field} AS value FROM type::record($table, $path)")
        } else {
            let table = quote_identifier(&self.table);
            let key_field = quote_identifier(&self.key_field);
            format!("SELECT {value_field} AS value FROM {table} WHERE {key_field} = $path LIMIT 1")
        };

        let mut req = self
            .get_connection()
            .await?
            .query(query)
            .bind(("path", path.to_string()));
        if self.key_field == "id" {
            req = req.bind(("table", self.table.clone()));
        }

        let mut result = match req.await.map_err(parse_surrealdb_error)?.check() {
            Ok(result) => result,
            Err(err) if is_table_not_found(&err) => return Ok(None),
            Err(err) => return Err(parse_surrealdb_error(err)),
        };

        let value: Option<Value> = result.take((0, "value")).map_err(parse_surrealdb_error)?;

        value.map(decode_value_bytes).transpose()
    }

    pub async fn get_length(&self, path: &str) -> Result<Option<usize>> {
        let value_field = quote_identifier(&self.value_field);
        let content_length_expr = format!(
            "(IF type::is_bytes({value_field}) {{ bytes::len({value_field}) }} \
            ELSE IF type::is_array({value_field}) {{ array::len({value_field}) }} \
            ELSE {{ bytes::len({value_field}) }})"
        );
        let query = if self.key_field == "id" {
            format!(
                "SELECT {content_length_expr} AS content_length FROM type::record($table, $path)"
            )
        } else {
            let table = quote_identifier(&self.table);
            let key_field = quote_identifier(&self.key_field);
            format!(
                "SELECT {content_length_expr} AS content_length FROM {table} WHERE {key_field} = $path LIMIT 1"
            )
        };

        let mut req = self
            .get_connection()
            .await?
            .query(query)
            .bind(("path", path.to_string()));
        if self.key_field == "id" {
            req = req.bind(("table", self.table.clone()));
        }

        let mut result = match req.await.map_err(parse_surrealdb_error)?.check() {
            Ok(result) => result,
            Err(err) if is_table_not_found(&err) => return Ok(None),
            Err(err) => return Err(parse_surrealdb_error(err)),
        };

        let value: Option<i64> = result
            .take((0, "content_length"))
            .map_err(parse_surrealdb_error)?;

        value
            .map(|v| {
                v.try_into().map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "surrealdb value length is invalid")
                        .set_source(err)
                })
            })
            .transpose()
    }

    pub async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        if path.is_empty() {
            return Ok(());
        }

        let table = quote_identifier(&self.table);
        let key_field = quote_identifier(&self.key_field);
        let value_field = quote_identifier(&self.value_field);
        let query = format!(
            "INSERT INTO {table} ({key_field}, {value_field}) \
            VALUES ($path, $value) \
            ON DUPLICATE KEY UPDATE {value_field} = $value"
        );
        self.get_connection()
            .await?
            .query(query)
            .bind(("path", path.to_string()))
            .bind(("value", Bytes::from(value.to_bytes())))
            .await
            .map_err(parse_surrealdb_error)?
            .check()
            .map_err(parse_surrealdb_error)?;
        Ok(())
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        if path.is_empty() {
            return Ok(());
        }

        let query = if self.key_field == "id" {
            "DELETE FROM type::record($table, $path)".to_string()
        } else {
            let table = quote_identifier(&self.table);
            let key_field = quote_identifier(&self.key_field);
            format!("DELETE FROM {table} WHERE {key_field} = $path")
        };

        let mut req = self
            .get_connection()
            .await?
            .query(query)
            .bind(("path", path.to_string()));
        if self.key_field == "id" {
            req = req.bind(("table", self.table.clone()));
        }

        match req.await.map_err(parse_surrealdb_error)?.check() {
            Ok(_) => Ok(()),
            Err(err) if is_table_not_found(&err) => Ok(()),
            Err(err) => Err(parse_surrealdb_error(err)),
        }
    }
}

fn decode_value_bytes(value: Value) -> Result<Buffer> {
    match value {
        Value::Bytes(bs) => Ok(Buffer::from(bs.into_inner())),
        Value::Array(arr) => {
            let mut bytes = Vec::with_capacity(arr.len());
            for elem in arr {
                match elem {
                    Value::Number(Number::Int(n)) => match u8::try_from(n) {
                        Ok(b) => bytes.push(b),
                        Err(_) => {
                            return Err(Error::new(
                                ErrorKind::Unexpected,
                                "surrealdb legacy byte array element out of range",
                            )
                            .with_context("value", n.to_string()));
                        }
                    },
                    other => {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "surrealdb legacy byte array contains non-integer element",
                        )
                        .with_context("kind", other.kind().to_string()));
                    }
                }
            }
            Ok(Buffer::from(bytes))
        }
        other => Err(
            Error::new(ErrorKind::Unexpected, "surrealdb value has unexpected type")
                .with_context("kind", other.kind().to_string()),
        ),
    }
}

pub(crate) fn quote_identifier(ident: &str) -> String {
    let mut quoted = String::with_capacity(ident.len() + 2);
    quoted.push('`');
    for c in ident.chars() {
        match c {
            '\0' => quoted.push_str("\\0"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            '\n' => quoted.push_str("\\n"),
            '\x08' => quoted.push_str("\\u{8}"),
            '\x0C' => quoted.push_str("\\f"),
            '\\' => quoted.push_str("\\\\"),
            '`' => quoted.push_str("\\`"),
            _ => quoted.push(c),
        }
    }
    quoted.push('`');
    quoted
}

fn is_table_not_found(err: &surrealdb::Error) -> bool {
    matches!(err.not_found_details(), Some(NotFoundError::Table { .. }))
}

fn parse_surrealdb_error(err: surrealdb::Error) -> Error {
    let (kind, retryable) = match err.details() {
        ErrorDetails::NotAllowed(_) => (ErrorKind::PermissionDenied, false),
        ErrorDetails::Query(Some(
            QueryError::TransactionConflict | QueryError::TimedOut { .. },
        )) => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };
    let mut opendal_err = Error::new(kind, "unhandled error from surrealdb").set_source(err);
    if retryable {
        opendal_err = opendal_err.set_temporary();
    }
    opendal_err
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use surrealdb::types::Array;
    use surrealdb::types::SurrealValue;

    use super::*;

    #[test]
    fn bytes_binding_serializes_as_surreal_value_bytes() {
        let payload = Buffer::from("hello world");
        let encoded = Bytes::from(payload.to_bytes()).into_value();
        assert!(matches!(encoded, Value::Bytes(_)));

        let decoded = Bytes::from_value(encoded).unwrap();
        assert_eq!(decoded.into_inner().as_ref(), b"hello world");

        // Regression guard: Vec<u8> serializes as Value::Array in surrealdb 3.x,
        // which breaks bytes::len(...) in get_length.
        let vec_encoded = vec![1_u8, 2, 3].into_value();
        assert!(matches!(vec_encoded, Value::Array(_)));
    }

    #[test]
    fn decode_value_bytes_accepts_bytes_and_legacy_array() {
        let bytes_val = Bytes::from(b"hello".to_vec()).into_value();
        assert_eq!(
            decode_value_bytes(bytes_val).unwrap().to_bytes().as_ref(),
            b"hello"
        );

        let legacy_arr = Value::Array(Array::from(vec![
            Value::Number(Number::Int(104)),
            Value::Number(Number::Int(105)),
            Value::Number(Number::Int(106)),
        ]));
        assert_eq!(
            decode_value_bytes(legacy_arr).unwrap().to_bytes().as_ref(),
            b"hij"
        );

        let empty_arr = Value::Array(Array::new());
        assert!(decode_value_bytes(empty_arr).unwrap().is_empty());

        for out_of_range in [-1_i64, 256_i64] {
            let err = decode_value_bytes(Value::Array(Array::from(vec![Value::Number(
                Number::Int(out_of_range),
            )])))
            .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(!err.is_temporary());
        }

        let non_int_elem = Value::Array(Array::from(vec![Value::String("not-a-byte".to_string())]));
        let err = decode_value_bytes(non_int_elem).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let float_elem = Value::Array(Array::from(vec![Value::Number(Number::Float(1.5))]));
        let err = decode_value_bytes(float_elem).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        for invalid in [
            Value::String("hij".to_string()),
            Value::None,
            Value::Null,
            Value::Number(Number::Int(42)),
        ] {
            let err = decode_value_bytes(invalid).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(!err.is_temporary());
        }
    }

    #[test]
    fn quote_identifier_escapes_special_characters() {
        assert_eq!(quote_identifier("kv"), "`kv`");
        assert_eq!(quote_identifier("select"), "`select`");
        assert_eq!(quote_identifier("my-table"), "`my-table`");
        assert_eq!(quote_identifier("we`ird\\name"), r"`we\`ird\\name`");
        assert_eq!(
            quote_identifier("a\0b\rc\nd\te\x08f\x0Cg"),
            r"`a\0b\rc\nd\te\u{8}f\fg`"
        );
    }

    #[test]
    fn error_classification_handles_table_not_found_and_details() {
        let table_err = surrealdb::Error::not_found(
            "The table 'kv' does not exist".to_string(),
            NotFoundError::Table {
                name: "kv".to_string(),
            },
        );
        assert!(is_table_not_found(&table_err));

        let db_err = surrealdb::Error::not_found(
            "The database 'db' does not exist".to_string(),
            NotFoundError::Database {
                name: "db".to_string(),
            },
        );
        assert!(!is_table_not_found(&db_err));

        let denied =
            parse_surrealdb_error(surrealdb::Error::not_allowed("denied".to_string(), None));
        assert_eq!(denied.kind(), ErrorKind::PermissionDenied);
        assert!(!denied.is_temporary());

        let conflict = parse_surrealdb_error(surrealdb::Error::query(
            "write conflict".to_string(),
            QueryError::TransactionConflict,
        ));
        assert_eq!(conflict.kind(), ErrorKind::Unexpected);
        assert!(conflict.is_temporary());

        let timed_out = parse_surrealdb_error(surrealdb::Error::query(
            "timed out".to_string(),
            QueryError::TimedOut {
                duration: Duration::from_secs(1),
            },
        ));
        assert_eq!(timed_out.kind(), ErrorKind::Unexpected);
        assert!(timed_out.is_temporary());
    }
}
