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

use serde::Deserialize;
use serde::Serialize;

/// Config for Etcd services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct EtcdConfig {
    /// network address of the Etcd services.
    /// If use https, must set TLS options: `ca_path`, `cert_path`, `key_path`.
    /// e.g. "127.0.0.1:23790,127.0.0.1:23791,127.0.0.1:23792" or "http://127.0.0.1:23790,http://127.0.0.1:23791,http://127.0.0.1:23792" or "https://127.0.0.1:23790,https://127.0.0.1:23791,https://127.0.0.1:23792"
    ///
    /// default is "http://127.0.0.1:2379"
    pub endpoints: Option<String>,
    /// the username to connect etcd service.
    ///
    /// default is None
    pub username: Option<String>,
    /// the password for authentication
    ///
    /// default is None
    pub password: Option<String>,
    /// the working directory of the etcd service. Can be "/path/to/dir"
    ///
    /// default is "/"
    pub root: Option<String>,
    /// certificate authority file path
    ///
    /// default is None
    pub ca_path: Option<String>,
    /// cert path
    ///
    /// default is None
    pub cert_path: Option<String>,
    /// key path
    ///
    /// default is None
    pub key_path: Option<String>,
}

impl Debug for EtcdConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("EtcdConfig");

        ds.field("root", &self.root);
        if let Some(endpoints) = self.endpoints.clone() {
            ds.field("endpoints", &endpoints);
        }
        if let Some(username) = self.username.clone() {
            ds.field("username", &username);
        }
        if self.password.is_some() {
            ds.field("password", &"<redacted>");
        }
        if let Some(ca_path) = self.ca_path.clone() {
            ds.field("ca_path", &ca_path);
        }
        if let Some(cert_path) = self.cert_path.clone() {
            ds.field("cert_path", &cert_path);
        }
        if let Some(key_path) = self.key_path.clone() {
            ds.field("key_path", &key_path);
        }
        ds.finish()
    }
}
