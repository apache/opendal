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

use opendal_core::raw::*;
use opendal_core::*;
use serde::Deserialize;
use serde::Serialize;

use super::REDIS_SCHEME;
use super::backend::RedisBuilder;

/// Config for Redis services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct RedisConfig {
    /// network address of the Redis service. Can be "tcp://127.0.0.1:6379", e.g.
    ///
    /// default is "tcp://127.0.0.1:6379"
    pub endpoint: Option<String>,
    /// network address of the Redis cluster service. Can be "tcp://127.0.0.1:6379,tcp://127.0.0.1:6380,tcp://127.0.0.1:6381", e.g.
    ///
    /// default is None
    pub cluster_endpoints: Option<String>,
    /// Route read-only commands for `read` and `stat` to Redis Cluster replicas when available.
    ///
    /// Defaults to `false`. Enabling this option requires `cluster_endpoints`.
    /// Replication lag can cause stale results. For example, `read` or `stat` can
    /// briefly return `ErrorKind::NotFound` for a just-written key. OpenDAL returns
    /// a replica miss without retrying on the primary.
    /// <!-- @default false -->
    pub read_from_replicas: bool,
    /// The maximum number of connections allowed.
    ///
    /// default is 10
    pub connection_pool_max_size: Option<usize>,
    /// the username to connect redis service.
    ///
    /// default is None
    pub username: Option<String>,
    /// the password for authentication
    ///
    /// default is None
    pub password: Option<String>,
    /// the working directory of the Redis service. Can be "/path/to/dir"
    ///
    /// default is "/"
    pub root: Option<String>,
    /// the number of DBs redis can take is unlimited
    ///
    /// default is db 0
    pub db: i64,
    /// The default ttl for put operations.
    pub default_ttl: Option<SignedDuration>,
}

impl Debug for RedisConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConfig")
            .field("endpoint", &self.endpoint)
            .field("cluster_endpoints", &self.cluster_endpoints)
            .field("read_from_replicas", &self.read_from_replicas)
            .field("username", &self.username)
            .field("root", &self.root)
            .field("db", &self.db)
            .field("default_ttl", &self.default_ttl)
            .finish_non_exhaustive()
    }
}

impl Configurator for RedisConfig {
    type Builder = RedisBuilder;

    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        let mut map = uri.options().clone();

        if let Some(authority) = uri.authority() {
            map.entry("endpoint".to_string())
                .or_insert_with(|| format!("redis://{authority}"));
        } else if !map.contains_key("endpoint") && !map.contains_key("cluster_endpoints") {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "endpoint or cluster_endpoints is required",
            )
            .with_context("service", REDIS_SCHEME));
        }

        if let Some(username) = uri.username() {
            map.entry("username".to_string())
                .or_insert_with(|| username.to_string());
        }
        if let Some(password) = uri.password() {
            map.entry("password".to_string())
                .or_insert_with(|| password.to_string());
        }

        if let Some(path) = uri.root()
            && !path.is_empty()
        {
            if let Some((first, rest)) = path.split_once('/') {
                if let Ok(db) = first.parse::<i64>() {
                    map.insert("db".to_string(), db.to_string());
                    if !rest.is_empty() {
                        map.insert("root".to_string(), rest.to_string());
                    }
                } else {
                    let mut root_value = first.to_string();
                    if !rest.is_empty() {
                        root_value.push('/');
                        root_value.push_str(rest);
                    }
                    map.insert("root".to_string(), root_value);
                }
            } else if let Ok(db) = path.parse::<i64>() {
                map.insert("db".to_string(), db.to_string());
            } else {
                map.insert("root".to_string(), path.to_string());
            }
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        RedisBuilder {
            config: self,
            default_ttl: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_uri_sets_endpoint_db_and_root() -> Result<()> {
        let uri = OperatorUri::new(
            "redis://localhost:6379/2/cache",
            Vec::<(String, String)>::new(),
        )?;

        let cfg = RedisConfig::from_uri(&uri)?;
        assert_eq!(cfg.endpoint.as_deref(), Some("redis://localhost:6379"));
        assert_eq!(cfg.db, 2);
        assert_eq!(cfg.root.as_deref(), Some("cache"));
        assert!(!cfg.read_from_replicas);
        Ok(())
    }

    #[test]
    fn from_uri_parses_read_from_replicas() -> Result<()> {
        let uri = OperatorUri::new(
            "redis://?cluster_endpoints=redis://localhost:6379&read_from_replicas=true",
            [],
        )?;

        let cfg = RedisConfig::from_uri(&uri)?;
        assert_eq!(
            cfg.cluster_endpoints.as_deref(),
            Some("redis://localhost:6379")
        );
        assert!(cfg.read_from_replicas);
        Ok(())
    }

    #[test]
    fn from_uri_treats_non_numeric_path_as_root() -> Result<()> {
        let uri = OperatorUri::new(
            "redis://localhost:6379/app/data",
            Vec::<(String, String)>::new(),
        )?;

        let cfg = RedisConfig::from_uri(&uri)?;
        assert_eq!(cfg.endpoint.as_deref(), Some("redis://localhost:6379"));
        assert_eq!(cfg.db, 0);
        assert_eq!(cfg.root.as_deref(), Some("app/data"));
        Ok(())
    }

    #[test]
    fn from_uri_extracts_credentials() -> Result<()> {
        let uri = OperatorUri::new(
            "redis://alice:s%40cret@localhost:6379/0",
            Vec::<(String, String)>::new(),
        )?;

        let cfg = RedisConfig::from_uri(&uri)?;
        assert_eq!(cfg.endpoint.as_deref(), Some("redis://localhost:6379"));
        assert_eq!(cfg.username.as_deref(), Some("alice"));
        assert_eq!(cfg.password.as_deref(), Some("s@cret"));
        assert!(!format!("{cfg:?}").contains("s@cret"));

        let password_only = OperatorUri::new(
            "redis://:onlypass@localhost:6379/1",
            Vec::<(String, String)>::new(),
        )?;
        let cfg = RedisConfig::from_uri(&password_only)?;
        assert_eq!(cfg.username, None);
        assert_eq!(cfg.password.as_deref(), Some("onlypass"));
        assert_eq!(cfg.db, 1);

        let override_uri = OperatorUri::new(
            "redis://alice:secret@localhost:6379/0",
            [
                ("username".to_string(), "bob".to_string()),
                ("password".to_string(), "override".to_string()),
            ],
        )?;
        let cfg = RedisConfig::from_uri(&override_uri)?;
        assert_eq!(cfg.username.as_deref(), Some("bob"));
        assert_eq!(cfg.password.as_deref(), Some("override"));
        Ok(())
    }

    #[test]
    fn from_iter_parses_default_ttl() -> Result<()> {
        let cfg = RedisConfig::from_iter([("default_ttl".to_string(), "5s".to_string())])?;

        assert_eq!(cfg.default_ttl, Some(SignedDuration::from_secs(5)));
        Ok(())
    }

    #[test]
    fn test_redis_builder_interface() {
        // Test that RedisBuilder still works with the new implementation
        let builder = RedisBuilder::default()
            .endpoint("redis://localhost:6379")
            .username("testuser")
            .password("testpass")
            .db(1)
            .root("/test");

        // The builder should be able to create configuration
        assert!(builder.config.endpoint.is_some());
        assert_eq!(
            builder.config.endpoint.as_ref().unwrap(),
            "redis://localhost:6379"
        );
        assert_eq!(builder.config.username.as_ref().unwrap(), "testuser");
        assert_eq!(builder.config.password.as_ref().unwrap(), "testpass");
        assert_eq!(builder.config.db, 1);
        assert_eq!(builder.config.root.as_ref().unwrap(), "/test");
    }
}
