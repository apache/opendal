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

use std::cell::OnceCell;
use std::collections::HashMap;
use zookeeper_client as zk;

use crate::raw::adapters::kv;
use crate::Scheme;
use async_trait::async_trait;

use crate::Builder;
use crate::Error;
use crate::ErrorKind;
use crate::*;

use std::fmt::Debug;
use std::fmt::Formatter;

const DEFAULT_ZOOKEEPER_ENDPOINT: &str = "127.0.0.1:2181";
/// The scheme for zookeeper authentication
/// currently we do not support sasl authentication
const ZOOKEEPER_AUTH_SCHEME: &str = "digest";

#[derive(Clone, Default)]
pub struct ZookeeperBuilder {
    /// network address of the Zookeeper service
    /// Default: 127.0.0.1:2181
    endpoint: Option<String>,
    /// the user to connect to zookeeper service, default None
    user: Option<String>,
    /// the password of the user to connect to zookeeper service, default None
    password: Option<String>,
}

impl ZookeeperBuilder {
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            self.endpoint = Some(endpoint.to_string());
        }
        self
    }

    pub fn user(&mut self, user: &str) -> &mut Self {
        if !user.is_empty() {
            self.user = Some(user.to_string());
        }
        self
    }

    pub fn password(&mut self, password: &str) -> &mut Self {
        if !password.is_empty() {
            self.password = Some(password.to_string());
        }
        self
    }
}

impl Debug for ZookeeperBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");
        if let Some(endpoint) = self.endpoint.clone() {
            ds.field("endpoint", &endpoint)
        }
        if let Some(user) = self.user.clone() {
            ds.field("user", &user)
        }
        if let Some(password) = self.password.clone() {
            ds.field("password", &password)
        }
        ds.finish()
    }
}

impl Builder for ZookeeperBuilder {
    const SCHEME: Scheme = Scheme::Zookeeper;
    type Accessor = ZookeeperBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = ZookeeperBuilder::default();

        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("user").map(|v| builder.user(v));
        map.get("password").map(|v| builder.password(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let endpoint = match self.endpoint.clone() {
            None => DEFAULT_ZOOKEEPER_ENDPOINT,
            Some(endpoint) => endpoint,
        };

        Ok(Backend::new(ZkAdapter {
            endpoint: endpoint.to_string(),
            user: self.user.clone(),
            password: self.password.clone(),
            client: OnceCell::new(),
        }))
    }
}

pub type Backend = kv::Backend<ZkAdapter>;

#[derive(Clone)]
pub struct ZkAdapter {
    endpoint: String,
    user: Option<String>,
    password: Option<String>,
    client: OnceCell<zk::Client>,
}

impl Debug for ZkAdapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.field("endpoint", &self.endpoint);
        ds.field("user", &self.user);
        ds.field("password", &self.password);
        ds.finish()
    }
}

impl ZkAdapter {
    async fn get_connection(&self) -> Result<zk::Client> {
        if let Some(client) = self.client.get() {
            return Ok(client.clone());
        }
        match zk::Client::connect(endpoint).await {
            Ok(client) => {
                match (self.user.clone(), self.password.clone()) {
                    (Some(user), Some(password)) => {
                        let auth = format!("{user}:{password}").as_bytes();
                        client.auth(ZOOKEEPER_AUTH_SCHEME.to_string(), auth.to_vec());
                    }
                    _ => log::warn!("username and password isn't set, default use `anyone` acl"),
                }
                self.client.set(client.clone()).ok();
                Ok(client)
            }
            Err(e) => Error::new(ErrorKind::Unexpected, "error from zookeeper").set_source(e),
        }
    }
}

#[async_trait]
impl kv::Adapter for ZkAdapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Zookeeper,
            "ZooKeeper",
            Capability {
                read: true,
                write: true,
                blocking: false,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        Ok(Some(self.get_connection().await?.get_data(path).0))
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        match self
            .get_connection()
            .await?
            .set_data(path, value, None)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => match e {
                zk::Error::NoNode => match (self.user.clone(), self.password.clone()) {
                    (Some(user), Some(password)) => self.get_connection().await?.create(
                        path,
                        value,
                        &zk::CreateOptions::new(zk::CreateMode::Persistent, zk::Acl::creator_all()),
                    ),
                    _ => self.get_connection().await?.create(
                        path,
                        value,
                        &zk::CreateOptions::new(zk::CreateMode::Persistent, zk::Acl::anyone_all()),
                    ),
                },
                _ => Error::new(ErrorKind::Unexpected, "error from zookeeper").set_source(e),
            },
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        match self.get_connection().await?.delete(path, None).await {
            Ok(()) => Ok(()),
            Err(e) => match e {
                zk::Error::NoNode => Ok(()),
                _ => Error::new(ErrorKind::Unexpected, "error from zookeeper").set_source(e),
            },
        }
    }
}
