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

use serde::Deserialize;
use serde::Serialize;

use super::backend::PostgresqlBuilder;

/// Config for PostgreSQL services support.
///
/// When built from an [`opendal_core::OperatorUri`] (`postgresql://[user[:password]@]host[:port]/table[/root]`),
/// OpenDAL synthesizes `connection_string` from the URI authority and percent-encoded
/// credentials without a database path segment, while the URI path maps to `table` and
/// optional `root`. Explicit `connection_string`, `table`, and `root` options take precedence
/// over values derived from the URI. Without a database path in `connection_string`,
/// PostgreSQL connects to `PGDATABASE` if set or to the database matching the connecting
/// username; provide an explicit `connection_string` option to select a different database.
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresqlConfig")
            .field("root", &self.root)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish_non_exhaustive()
    }
}

fn percent_encode_userinfo(input: &str) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(input.len());
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn format_connection_string(
    authority: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> String {
    match (username, password) {
        (Some(user), Some(pass)) => format!(
            "postgresql://{}:{}@{authority}",
            percent_encode_userinfo(user),
            percent_encode_userinfo(pass)
        ),
        (Some(user), None) => format!("postgresql://{}@{authority}", percent_encode_userinfo(user)),
        (None, Some(pass)) => format!(
            "postgresql://:{}@{authority}",
            percent_encode_userinfo(pass)
        ),
        (None, None) => format!("postgresql://{authority}"),
    }
}

impl opendal_core::Configurator for PostgresqlConfig {
    type Builder = PostgresqlBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(authority) = uri.authority() {
            map.entry("connection_string".to_string())
                .or_insert_with(|| {
                    format_connection_string(authority, uri.username(), uri.password())
                });
        }

        if let Some(path) = uri.root()
            && !path.is_empty()
        {
            let (table_segment, rest) = match path.split_once('/') {
                Some((table, remainder)) => (table, Some(remainder)),
                None => (path, None),
            };

            if !table_segment.is_empty() {
                map.entry("table".to_string())
                    .or_insert_with(|| table_segment.to_string());
            }

            if let Some(root) = rest
                && !root.is_empty()
            {
                map.entry("root".to_string())
                    .or_insert_with(|| root.to_string());
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
    use opendal_core::Builder;
    use opendal_core::Configurator;
    use opendal_core::OperatorUri;
    use sqlx::postgres::PgConnectOptions;

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

    #[test]
    fn from_uri_preserves_credentials_and_builds() {
        let uri = OperatorUri::new(
            "postgresql://alice%40corp.com:p%3Aa%2Fs%25s%401@db.example.com:5432/kv/cache",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = PostgresqlConfig::from_uri(&uri).unwrap();
        assert_eq!(
            cfg.connection_string.as_deref(),
            Some("postgresql://alice%40corp.com:p%3Aa%2Fs%25s%401@db.example.com:5432")
        );
        assert_eq!(cfg.table.as_deref(), Some("kv"));
        assert_eq!(cfg.root.as_deref(), Some("cache"));
        let opts: PgConnectOptions = cfg.connection_string.as_deref().unwrap().parse().unwrap();
        assert_eq!(opts.get_username(), "alice@corp.com");
        cfg.into_builder().build().unwrap();

        let user_only = OperatorUri::new(
            "postgresql://postgres@[::1]:5432/kv",
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let cfg = PostgresqlConfig::from_uri(&user_only).unwrap();
        assert_eq!(
            cfg.connection_string.as_deref(),
            Some("postgresql://postgres@[::1]:5432")
        );
        assert_eq!(cfg.table.as_deref(), Some("kv"));
        assert_eq!(cfg.root, None);
        cfg.into_builder().build().unwrap();

        let pass_only = OperatorUri::new(
            "postgresql://:p%3Ass%251@db.example.com:5432/kv",
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let cfg = PostgresqlConfig::from_uri(&pass_only).unwrap();
        assert_eq!(
            cfg.connection_string.as_deref(),
            Some("postgresql://:p%3Ass%251@db.example.com:5432")
        );
        cfg.into_builder().build().unwrap();
    }

    #[test]
    fn from_uri_respects_explicit_root_table_and_connection_string_options() {
        let uri = OperatorUri::new(
            "postgresql://user:pass@db.example.com:5432/kv/path?root=override",
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let cfg = PostgresqlConfig::from_uri(&uri).unwrap();
        assert_eq!(
            cfg.connection_string.as_deref(),
            Some("postgresql://user:pass@db.example.com:5432")
        );
        assert_eq!(cfg.table.as_deref(), Some("kv"));
        assert_eq!(cfg.root.as_deref(), Some("override"));

        let explicit_table = OperatorUri::new(
            "postgresql://user:pass@db.example.com:5432/kv/path?table=custom_table",
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let cfg = PostgresqlConfig::from_uri(&explicit_table).unwrap();
        assert_eq!(cfg.table.as_deref(), Some("custom_table"));
        assert_eq!(cfg.root.as_deref(), Some("path"));

        let explicit_conn = OperatorUri::new(
            "postgresql://user:pass@db.example.com:5432/kv/path",
            [(
                "connection_string".to_string(),
                "postgresql://other:pw@127.0.0.1:5432/testdb".to_string(),
            )],
        )
        .unwrap();
        let cfg = PostgresqlConfig::from_uri(&explicit_conn).unwrap();
        assert_eq!(
            cfg.connection_string.as_deref(),
            Some("postgresql://other:pw@127.0.0.1:5432/testdb")
        );
        assert_eq!(cfg.table.as_deref(), Some("kv"));
        assert_eq!(cfg.root.as_deref(), Some("path"));

        let query_conn = OperatorUri::new(
            "postgresql://user:pass@db.example.com:5432/kv/path?connection_string=postgresql%3A%2F%2Fother%3Apw%40127.0.0.1%3A5432%2Ftestdb",
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let cfg = PostgresqlConfig::from_uri(&query_conn).unwrap();
        assert_eq!(
            cfg.connection_string.as_deref(),
            Some("postgresql://other:pw@127.0.0.1:5432/testdb")
        );
    }
}
