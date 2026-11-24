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

use super::backend::MongodbBuilder;

/// Config for Mongodb service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MongodbConfig {
    /// connection string of this backend
    pub connection_string: Option<String>,
    /// database of this backend
    pub database: Option<String>,
    /// collection of this backend
    pub collection: Option<String>,
    /// root of this backend
    pub root: Option<String>,
    /// key field of this backend
    pub key_field: Option<String>,
    /// value field of this backend
    pub value_field: Option<String>,
}

impl Debug for MongodbConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongodbConfig")
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("root", &self.root)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish_non_exhaustive()
    }
}

fn percent_encode_uri_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if matches!(
            byte,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~'
        ) {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }
    encoded
}

impl opendal_core::Configurator for MongodbConfig {
    type Builder = MongodbBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(path) = uri.root()
            && !path.is_empty()
        {
            let mut segments = path.splitn(3, '/');
            if let Some(db) = segments.next()
                && !db.is_empty()
            {
                map.entry("database".to_string())
                    .or_insert_with(|| db.to_string());
            }
            if let Some(collection) = segments.next()
                && !collection.is_empty()
            {
                map.entry("collection".to_string())
                    .or_insert_with(|| collection.to_string());
            }
            if let Some(rest) = segments.next()
                && !rest.is_empty()
            {
                map.entry("root".to_string())
                    .or_insert_with(|| rest.to_string());
            }
        }

        if let Some(authority) = uri.authority()
            && !map.contains_key("connection_string")
        {
            let userinfo = match (uri.username(), uri.password()) {
                (Some(user), Some(pass)) => format!(
                    "{}:{}@",
                    percent_encode_uri_component(user),
                    percent_encode_uri_component(pass)
                ),
                (Some(user), None) => format!("{}@", percent_encode_uri_component(user)),
                (None, Some(_)) => {
                    return Err(opendal_core::Error::new(
                        opendal_core::ErrorKind::ConfigInvalid,
                        "mongodb URI password requires a username",
                    )
                    .with_context("service", crate::MONGODB_SCHEME));
                }
                (None, None) => String::new(),
            };
            let db_path = map
                .get("database")
                .filter(|db| !db.is_empty())
                .map(|db| format!("/{}", percent_encode_uri_component(db)))
                .unwrap_or_default();
            map.insert(
                "connection_string".to_string(),
                format!("mongodb://{userinfo}{authority}{db_path}"),
            );
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        MongodbBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::options::ClientOptions;
    use mongodb::options::ConnectionString;
    use opendal_core::Builder;
    use opendal_core::Configurator;
    use opendal_core::ErrorKind;
    use opendal_core::OperatorUri;

    #[test]
    fn from_uri_sets_connection_string_database_collection_and_root() {
        let uri = OperatorUri::new(
            "mongodb://mongo.internal:27017/analytics/events/session",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = MongodbConfig::from_uri(&uri).unwrap();
        assert_eq!(
            cfg.connection_string.as_deref(),
            Some("mongodb://mongo.internal:27017/analytics")
        );
        assert_eq!(cfg.database.as_deref(), Some("analytics"));
        assert_eq!(cfg.collection.as_deref(), Some("events"));
        assert_eq!(cfg.root.as_deref(), Some("session"));

        let parsed = ConnectionString::parse(cfg.connection_string.as_deref().unwrap()).unwrap();
        assert_eq!(parsed.default_database.as_deref(), Some("analytics"));
        assert!(cfg.into_builder().build().is_ok());

        let no_db_uri = OperatorUri::new(
            "mongodb://mongo.internal:27017",
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let no_db_cfg = MongodbConfig::from_uri(&no_db_uri).unwrap();
        assert_eq!(
            no_db_cfg.connection_string.as_deref(),
            Some("mongodb://mongo.internal:27017")
        );
        assert_eq!(no_db_cfg.database, None);
    }

    #[tokio::test]
    async fn from_uri_preserves_and_percent_encodes_credentials_and_database() {
        let uri = OperatorUri::new(
            "mongodb://alice%40corp.com:p%40ss%3Aword%251@127.0.0.1:27017/analytics%231/events",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = MongodbConfig::from_uri(&uri).unwrap();
        let conn_str = cfg.connection_string.as_deref().unwrap();
        assert_eq!(
            conn_str,
            "mongodb://alice%40corp.com:p%40ss%3Aword%251@127.0.0.1:27017/analytics%231"
        );
        assert_eq!(cfg.database.as_deref(), Some("analytics#1"));

        let parsed = ConnectionString::parse(conn_str).unwrap();
        assert_eq!(parsed.default_database.as_deref(), Some("analytics#1"));
        let cred = parsed.credential.as_ref().unwrap();
        assert_eq!(cred.username.as_deref(), Some("alice@corp.com"));
        assert_eq!(cred.password.as_deref(), Some("p@ss:word%1"));

        let client_opts = ClientOptions::parse(parsed).await.unwrap();
        assert_eq!(
            client_opts
                .credential
                .as_ref()
                .and_then(|c| c.source.as_deref()),
            Some("analytics#1")
        );
        assert!(cfg.into_builder().build().is_ok());

        let user_only_uri = OperatorUri::new(
            "mongodb://alice@127.0.0.1:27017/analytics/events",
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let user_only_cfg = MongodbConfig::from_uri(&user_only_uri).unwrap();
        assert_eq!(
            user_only_cfg.connection_string.as_deref(),
            Some("mongodb://alice@127.0.0.1:27017/analytics")
        );
        assert!(user_only_cfg.into_builder().build().is_ok());

        let pass_only_uri = OperatorUri::new(
            "mongodb://:only%23pass@127.0.0.1:27017/analytics/events",
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let err = MongodbConfig::from_uri(&pass_only_uri).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn from_uri_preserves_explicit_options_over_uri_components() {
        let uri = OperatorUri::new(
            "mongodb://root:secret@mongo.internal:27017/analytics/events/session?root=override",
            vec![(
                "connection_string".to_string(),
                "mongodb://custom:27018".to_string(),
            )],
        )
        .unwrap();

        let cfg = MongodbConfig::from_uri(&uri).unwrap();
        assert_eq!(
            cfg.connection_string.as_deref(),
            Some("mongodb://custom:27018")
        );
        assert_eq!(cfg.database.as_deref(), Some("analytics"));
        assert_eq!(cfg.collection.as_deref(), Some("events"));
        assert_eq!(cfg.root.as_deref(), Some("override"));

        let db_override_uri = OperatorUri::new(
            "mongodb://demo_u:secret@mongo.internal:27017/ignored/events",
            vec![("database".to_string(), "target_db".to_string())],
        )
        .unwrap();
        let db_override_cfg = MongodbConfig::from_uri(&db_override_uri).unwrap();
        assert_eq!(
            db_override_cfg.connection_string.as_deref(),
            Some("mongodb://demo_u:secret@mongo.internal:27017/target_db")
        );
        assert_eq!(db_override_cfg.database.as_deref(), Some("target_db"));
        assert_eq!(db_override_cfg.collection.as_deref(), Some("events"));
    }
}
