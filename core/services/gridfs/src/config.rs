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

use opendal_core::*;
use serde::Deserialize;
use serde::Serialize;

use super::backend::GridfsBuilder;

/// Config for Grid file system support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GridfsConfig {
    /// The connection string of the MongoDB service.
    pub connection_string: Option<String>,
    /// The database name of the MongoDB GridFs service to read/write.
    pub database: Option<String>,
    /// The bucket name of the MongoDB GridFs service to read/write.
    pub bucket: Option<String>,
    /// The chunk size of the MongoDB GridFs service used to break the user file into chunks.
    pub chunk_size: Option<u32>,
    /// The working directory, all operations will be performed under it.
    pub root: Option<String>,
}

impl Debug for GridfsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GridFsConfig")
            .field("database", &self.database)
            .field("bucket", &self.bucket)
            .field("chunk_size", &self.chunk_size)
            .field("root", &self.root)
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

impl Configurator for GridfsConfig {
    type Builder = GridfsBuilder;

    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        let mut map = uri.options().clone();

        if let Some(path) = uri.root()
            && !path.is_empty()
        {
            let mut segments = path.splitn(3, '/');
            if let Some(database) = segments.next()
                && !database.is_empty()
            {
                map.entry("database".to_string())
                    .or_insert_with(|| database.to_string());
            }
            if let Some(bucket) = segments.next()
                && !bucket.is_empty()
            {
                map.entry("bucket".to_string())
                    .or_insert_with(|| bucket.to_string());
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
                (None, _) => String::new(),
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
        GridfsBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use mongodb::options::ClientOptions;
    use mongodb::options::ConnectionString;

    #[test]
    fn from_uri_sets_connection_database_bucket_and_root() -> Result<()> {
        let uri = OperatorUri::new(
            "gridfs://mongo.example.com:27017/app_files/assets/images",
            Vec::<(String, String)>::new(),
        )?;

        let cfg = GridfsConfig::from_uri(&uri)?;
        assert_eq!(
            cfg.connection_string.as_deref(),
            Some("mongodb://mongo.example.com:27017/app_files")
        );
        assert_eq!(cfg.database.as_deref(), Some("app_files"));
        assert_eq!(cfg.bucket.as_deref(), Some("assets"));
        assert_eq!(cfg.root.as_deref(), Some("images"));

        let parsed = ConnectionString::parse(cfg.connection_string.as_deref().unwrap()).unwrap();
        assert_eq!(parsed.default_database.as_deref(), Some("app_files"));
        assert!(cfg.into_builder().build().is_ok());

        let no_db_uri = OperatorUri::new(
            "gridfs://mongo.example.com:27017",
            Vec::<(String, String)>::new(),
        )?;
        let no_db_cfg = GridfsConfig::from_uri(&no_db_uri)?;
        assert_eq!(
            no_db_cfg.connection_string.as_deref(),
            Some("mongodb://mongo.example.com:27017")
        );
        assert_eq!(no_db_cfg.database, None);
        Ok(())
    }

    #[tokio::test]
    async fn from_uri_preserves_and_percent_encodes_credentials_and_database() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let uri = OperatorUri::new(
                "gridfs://alice%40corp.com:p%40ss%3Aword@127.0.0.1:27017/app%231/assets/images",
                Vec::<(String, String)>::new(),
            )?;

            let cfg = GridfsConfig::from_uri(&uri)?;
            let conn_str = cfg.connection_string.as_deref().unwrap();
            assert_eq!(
                conn_str,
                "mongodb://alice%40corp.com:p%40ss%3Aword@127.0.0.1:27017/app%231"
            );
            assert_eq!(cfg.database.as_deref(), Some("app#1"));
            assert_eq!(cfg.bucket.as_deref(), Some("assets"));
            assert_eq!(cfg.root.as_deref(), Some("images"));

            let parsed = ConnectionString::parse(conn_str).unwrap();
            assert_eq!(parsed.default_database.as_deref(), Some("app#1"));
            let cred = parsed.credential.as_ref().unwrap();
            assert_eq!(cred.username.as_deref(), Some("alice@corp.com"));
            assert_eq!(cred.password.as_deref(), Some("p@ss:word"));

            let client_opts = ClientOptions::parse(parsed).await.unwrap();
            assert_eq!(
                client_opts
                    .credential
                    .as_ref()
                    .and_then(|c| c.source.as_deref()),
                Some("app#1")
            );
            assert!(cfg.into_builder().build().is_ok());

            let user_only_uri = OperatorUri::new(
                "gridfs://alice@127.0.0.1:27017/app_files/assets",
                Vec::<(String, String)>::new(),
            )?;
            let user_only_cfg = GridfsConfig::from_uri(&user_only_uri)?;
            assert_eq!(
                user_only_cfg.connection_string.as_deref(),
                Some("mongodb://alice@127.0.0.1:27017/app_files")
            );
            assert!(user_only_cfg.into_builder().build().is_ok());

            Ok(())
        })
        .await
        .expect("test timed out")
    }

    #[test]
    fn from_uri_preserves_explicit_options_over_uri_components() -> Result<()> {
        let uri = OperatorUri::new(
            "gridfs://root:secret@mongo.example.com:27017/app_files/assets/images?root=override",
            vec![(
                "connection_string".to_string(),
                "mongodb://custom:27018".to_string(),
            )],
        )?;

        let cfg = GridfsConfig::from_uri(&uri)?;
        assert_eq!(
            cfg.connection_string.as_deref(),
            Some("mongodb://custom:27018")
        );
        assert_eq!(cfg.database.as_deref(), Some("app_files"));
        assert_eq!(cfg.bucket.as_deref(), Some("assets"));
        assert_eq!(cfg.root.as_deref(), Some("override"));

        let db_override_uri = OperatorUri::new(
            "gridfs://demo_u:secret@mongo.example.com:27017/ignored/assets",
            vec![("database".to_string(), "target_db".to_string())],
        )?;
        let db_override_cfg = GridfsConfig::from_uri(&db_override_uri)?;
        assert_eq!(
            db_override_cfg.connection_string.as_deref(),
            Some("mongodb://demo_u:secret@mongo.example.com:27017/target_db")
        );
        assert_eq!(db_override_cfg.database.as_deref(), Some("target_db"));
        assert_eq!(db_override_cfg.bucket.as_deref(), Some("assets"));
        Ok(())
    }

    #[test]
    fn from_uri_rejects_zero_chunk_size_on_build() -> Result<()> {
        let uri = OperatorUri::new(
            "gridfs://127.0.0.1:27017/app_files/fs?chunk_size=0",
            Vec::<(String, String)>::new(),
        )?;
        let cfg = GridfsConfig::from_uri(&uri)?;
        assert_eq!(cfg.chunk_size, Some(0));

        let err = cfg
            .into_builder()
            .build()
            .expect_err("chunk_size=0 must fail build");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        Ok(())
    }
}
