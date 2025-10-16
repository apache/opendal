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

use super::backend::PostgresqlBuilder;
use serde::Deserialize;
use serde::Serialize;

/// Config for PostgreSQL services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct PostgresqlConfig {
    /// Root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// Default to `/` if not set.
    pub root: Option<String>,
    /// The URL should be with a scheme of either `postgres://` or `postgresql://`.
    ///
    /// - `postgresql://user@localhost`
    /// - `postgresql://user:password@%2Fvar%2Flib%2Fpostgresql/mydb?connect_timeout=10`
    /// - `postgresql://user@host1:1234,host2,host3:5678?target_session_attrs=read-write`
    /// - `postgresql:///mydb?user=user&host=/var/lib/postgresql`
    ///
    /// For more information, please visit <https://docs.rs/sqlx/latest/sqlx/postgres/struct.PgConnectOptions.html>.
    pub connection_string: Option<String>,
    /// the table of postgresql
    pub table: Option<String>,
    /// the key field of postgresql
    pub key_field: Option<String>,
    /// the value field of postgresql
    pub value_field: Option<String>,
}

impl Debug for PostgresqlConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("PostgresqlConfig");

        if self.connection_string.is_some() {
            d.field("connection_string", &"<redacted>");
        }

        d.field("root", &self.root)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish()
    }
}

impl crate::Configurator for PostgresqlConfig {
    type Builder = PostgresqlBuilder;
    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(authority) = uri.authority() {
            map.entry("connection_string".to_string())
                .or_insert_with(|| format!("postgresql://{authority}"));
        }

        if let Some(path) = uri.root() {
            if !path.is_empty() {
                let (table_segment, rest) = match path.split_once('/') {
                    Some((table, remainder)) => (table, Some(remainder)),
                    None => (path, None),
                };

                if !table_segment.is_empty() {
                    map.entry("table".to_string())
                        .or_insert_with(|| table_segment.to_string());
                }

                if let Some(root) = rest {
                    if !root.is_empty() {
                        map.insert("root".to_string(), root.to_string());
                    }
                }
            }
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        PostgresqlBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_connection_string_table_and_root() {
        let uri = OperatorUri::new(
            "postgresql://db.example.com:5432/kv/cache",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = PostgresqlConfig::from_uri(&uri).unwrap();
        assert_eq!(
            cfg.connection_string.as_deref(),
            Some("postgresql://db.example.com:5432")
        );
        assert_eq!(cfg.table.as_deref(), Some("kv"));
        assert_eq!(cfg.root.as_deref(), Some("cache"));
    }
}
