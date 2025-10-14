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
use std::time::Duration;

use super::backend::RedisBuilder;
use serde::Deserialize;
use serde::Serialize;

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
    pub default_ttl: Option<Duration>,
}

impl Debug for RedisConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("RedisConfig");

        d.field("db", &self.db.to_string());
        d.field("root", &self.root);
        if let Some(endpoint) = self.endpoint.clone() {
            d.field("endpoint", &endpoint);
        }
        if let Some(cluster_endpoints) = self.cluster_endpoints.clone() {
            d.field("cluster_endpoints", &cluster_endpoints);
        }
        if let Some(username) = self.username.clone() {
            d.field("username", &username);
        }
        if self.password.is_some() {
            d.field("password", &"<redacted>");
        }

        d.finish_non_exhaustive()
    }
}

impl crate::Configurator for RedisConfig {
    type Builder = RedisBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(authority) = uri.authority() {
            map.entry("endpoint".to_string())
                .or_insert_with(|| format!("redis://{authority}"));
        } else if !map.contains_key("endpoint") && !map.contains_key("cluster_endpoints") {
            return Err(crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "endpoint or cluster_endpoints is required",
            )
            .with_context("service", crate::Scheme::Redis));
        }

        if let Some(path) = uri.root() {
            if !path.is_empty() {
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
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        RedisBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_endpoint_db_and_root() {
        let uri = OperatorUri::new(
            "redis://localhost:6379/2/cache".parse().unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = RedisConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("redis://localhost:6379"));
        assert_eq!(cfg.db, 2);
        assert_eq!(cfg.root.as_deref(), Some("cache"));
    }

    #[test]
    fn from_uri_treats_non_numeric_path_as_root() {
        let uri = OperatorUri::new(
            "redis://localhost:6379/app/data".parse().unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = RedisConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("redis://localhost:6379"));
        assert_eq!(cfg.db, 0);
        assert_eq!(cfg.root.as_deref(), Some("app/data"));
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
