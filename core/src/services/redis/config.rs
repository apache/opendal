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
    fn into_builder(self) -> Self::Builder {
        RedisBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
