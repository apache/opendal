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
use std::vec;

use bb8::PooledConnection;
use bb8::RunError;
use etcd_client::Certificate;
use etcd_client::Client;
use etcd_client::ConnectOptions;
use etcd_client::Error as EtcdError;
use etcd_client::GetOptions;
use etcd_client::Identity;
use etcd_client::TlsOptions;
use tokio::sync::OnceCell;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::EtcdConfig;
use crate::*;

const DEFAULT_ETCD_ENDPOINTS: &str = "http://127.0.0.1:2379";

impl Configurator for EtcdConfig {
    type Builder = EtcdBuilder;
    fn into_builder(self) -> Self::Builder {
        EtcdBuilder { config: self }
    }
}

/// [Etcd](https://etcd.io/) services support.
#[doc = include_str!("docs.md")]
#[derive(Clone, Default)]
pub struct EtcdBuilder {
    config: EtcdConfig,
}

impl Debug for EtcdBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("config", &self.config);
        ds.finish()
    }
}

impl EtcdBuilder {
    /// set the network address of etcd service.
    ///
    /// default: "http://127.0.0.1:2379"
    pub fn endpoints(mut self, endpoints: &str) -> Self {
        if !endpoints.is_empty() {
            self.config.endpoints = Some(endpoints.to_owned());
        }
        self
    }

    /// set the username for etcd
    ///
    /// default: no username
    pub fn username(mut self, username: &str) -> Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_owned());
        }
        self
    }

    /// set the password for etcd
    ///
    /// default: no password
    pub fn password(mut self, password: &str) -> Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_owned());
        }
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set the certificate authority file path.
    ///
    /// default is None
    pub fn ca_path(mut self, ca_path: &str) -> Self {
        if !ca_path.is_empty() {
            self.config.ca_path = Some(ca_path.to_string())
        }
        self
    }

    /// Set the certificate file path.
    ///
    /// default is None
    pub fn cert_path(mut self, cert_path: &str) -> Self {
        if !cert_path.is_empty() {
            self.config.cert_path = Some(cert_path.to_string())
        }
        self
    }

    /// Set the key file path.
    ///
    /// default is None
    pub fn key_path(mut self, key_path: &str) -> Self {
        if !key_path.is_empty() {
            self.config.key_path = Some(key_path.to_string())
        }
        self
    }
}

impl Builder for EtcdBuilder {
    const SCHEME: Scheme = Scheme::Etcd;
    type Config = EtcdConfig;

    fn build(self) -> Result<impl Access> {
        let endpoints = self
            .config
            .endpoints
            .clone()
            .unwrap_or_else(|| DEFAULT_ETCD_ENDPOINTS.to_string());

        let endpoints: Vec<String> = endpoints.split(',').map(|s| s.to_string()).collect();

        let mut options = ConnectOptions::new();

        if self.config.ca_path.is_some()
            && self.config.cert_path.is_some()
            && self.config.key_path.is_some()
        {
            let ca = self.load_pem(self.config.ca_path.clone().unwrap().as_str())?;
            let key = self.load_pem(self.config.key_path.clone().unwrap().as_str())?;
            let cert = self.load_pem(self.config.cert_path.clone().unwrap().as_str())?;

            let tls_options = TlsOptions::default()
                .ca_certificate(Certificate::from_pem(ca))
                .identity(Identity::from_pem(cert, key));
            options = options.with_tls(tls_options);
        }

        if let Some(username) = self.config.username.clone() {
            options = options.with_user(
                username,
                self.config.password.clone().unwrap_or("".to_string()),
            );
        }

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let client = OnceCell::new();
        Ok(EtcdBackend::new(Adapter {
            endpoints,
            client,
            options,
        })
        .with_normalized_root(root))
    }
}

impl EtcdBuilder {
    fn load_pem(&self, path: &str) -> Result<String> {
        std::fs::read_to_string(path)
            .map_err(|err| Error::new(ErrorKind::Unexpected, "invalid file path").set_source(err))
    }
}

/// Backend for etcd services.
pub type EtcdBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Manager {
    endpoints: Vec<String>,
    options: ConnectOptions,
}

#[async_trait::async_trait]
impl bb8::ManageConnection for Manager {
    type Connection = Client;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = Client::connect(self.endpoints.clone(), Some(self.options.clone()))
            .await
            .map_err(format_etcd_error)?;

        Ok(conn)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let _ = conn.status().await.map_err(format_etcd_error)?;
        Ok(())
    }

    /// Always allow reuse conn.
    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct Adapter {
    endpoints: Vec<String>,
    options: ConnectOptions,
    client: OnceCell<bb8::Pool<Manager>>,
}

// implement `Debug` manually, or password may be leaked.
impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");

        ds.field("endpoints", &self.endpoints.join(","));
        ds.field("options", &self.options.clone());
        ds.finish()
    }
}

impl Adapter {
    async fn conn(&self) -> Result<PooledConnection<'static, Manager>> {
        let client = self
            .client
            .get_or_try_init(|| async {
                bb8::Pool::builder()
                    .max_size(64)
                    .build(Manager {
                        endpoints: self.endpoints.clone(),
                        options: self.options.clone(),
                    })
                    .await
            })
            .await?;

        client.get_owned().await.map_err(|err| match err {
            RunError::User(err) => err,
            RunError::TimedOut => {
                Error::new(ErrorKind::Unexpected, "connection request: timeout").set_temporary()
            }
        })
    }
}

impl kv::Adapter for Adapter {
    type Scanner = kv::ScanStdIter<vec::IntoIter<Result<String>>>;

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::Etcd,
            &self.endpoints.join(","),
            Capability {
                read: true,
                write: true,
                list: true,
                shared: true,

                ..Default::default()
            },
        )
    }

    async fn get(&self, key: &str) -> Result<Option<Buffer>> {
        let mut client = self.conn().await?;
        let resp = client.get(key, None).await.map_err(format_etcd_error)?;
        if let Some(kv) = resp.kvs().first() {
            Ok(Some(Buffer::from(kv.value().to_vec())))
        } else {
            Ok(None)
        }
    }

    async fn set(&self, key: &str, value: Buffer) -> Result<()> {
        let mut client = self.conn().await?;
        let _ = client
            .put(key, value.to_vec(), None)
            .await
            .map_err(format_etcd_error)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut client = self.conn().await?;
        let _ = client.delete(key, None).await.map_err(format_etcd_error)?;
        Ok(())
    }

    async fn scan(&self, path: &str) -> Result<Self::Scanner> {
        let mut client = self.conn().await?;
        let get_options = Some(GetOptions::new().with_prefix().with_keys_only());
        let resp = client
            .get(path, get_options)
            .await
            .map_err(format_etcd_error)?;
        let mut res = Vec::default();
        for kv in resp.kvs() {
            let v = kv.key_str().map(String::from).map_err(|err| {
                Error::new(ErrorKind::Unexpected, "store key is not valid utf-8 string")
                    .set_source(err)
            })?;
            res.push(Ok(v));
        }

        Ok(kv::ScanStdIter::new(res.into_iter()))
    }
}

pub fn format_etcd_error(e: EtcdError) -> Error {
    Error::new(ErrorKind::Unexpected, e.to_string().as_str())
        .set_source(e)
        .set_temporary()
}
