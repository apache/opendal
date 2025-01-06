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

use tikv_client::Config;
use tikv_client::RawClient;
use tokio::sync::OnceCell;

use crate::raw::adapters::kv;
use crate::raw::Access;
use crate::services::TikvConfig;
use crate::Builder;
use crate::Capability;
use crate::Error;
use crate::ErrorKind;
use crate::Scheme;
use crate::*;

impl Configurator for TikvConfig {
    type Builder = TikvBuilder;
    fn into_builder(self) -> Self::Builder {
        TikvBuilder { config: self }
    }
}

/// TiKV backend builder
#[doc = include_str!("docs.md")]
#[derive(Clone, Default)]
pub struct TikvBuilder {
    config: TikvConfig,
}

impl Debug for TikvBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("TikvBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl TikvBuilder {
    /// Set the network address of the TiKV service.
    pub fn endpoints(mut self, endpoints: Vec<String>) -> Self {
        if !endpoints.is_empty() {
            self.config.endpoints = Some(endpoints)
        }
        self
    }

    /// Set the insecure connection to TiKV.
    pub fn insecure(mut self) -> Self {
        self.config.insecure = true;
        self
    }

    /// Set the certificate authority file path.
    pub fn ca_path(mut self, ca_path: &str) -> Self {
        if !ca_path.is_empty() {
            self.config.ca_path = Some(ca_path.to_string())
        }
        self
    }

    /// Set the certificate file path.
    pub fn cert_path(mut self, cert_path: &str) -> Self {
        if !cert_path.is_empty() {
            self.config.cert_path = Some(cert_path.to_string())
        }
        self
    }

    /// Set the key file path.
    pub fn key_path(mut self, key_path: &str) -> Self {
        if !key_path.is_empty() {
            self.config.key_path = Some(key_path.to_string())
        }
        self
    }
}

impl Builder for TikvBuilder {
    const SCHEME: Scheme = Scheme::Tikv;
    type Config = TikvConfig;

    fn build(self) -> Result<impl Access> {
        let endpoints = self.config.endpoints.ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "endpoints is required but not set",
            )
            .with_context("service", Scheme::Tikv)
        })?;

        if self.config.insecure
            && (self.config.ca_path.is_some()
                || self.config.key_path.is_some()
                || self.config.cert_path.is_some())
        {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "invalid tls configuration")
                    .with_context("service", Scheme::Tikv)
                    .with_context("endpoints", format!("{:?}", endpoints)),
            )?;
        }

        Ok(TikvBackend::new(Adapter {
            client: OnceCell::new(),
            endpoints,
            insecure: self.config.insecure,
            ca_path: self.config.ca_path.clone(),
            cert_path: self.config.cert_path.clone(),
            key_path: self.config.key_path.clone(),
        }))
    }
}

/// Backend for TiKV service
pub type TikvBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    client: OnceCell<RawClient>,
    endpoints: Vec<String>,
    insecure: bool,
    ca_path: Option<String>,
    cert_path: Option<String>,
    key_path: Option<String>,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");

        ds.field("endpoints", &self.endpoints);
        ds.finish()
    }
}

impl Adapter {
    async fn get_connection(&self) -> Result<RawClient> {
        if let Some(client) = self.client.get() {
            return Ok(client.clone());
        }
        let client = if self.insecure {
            RawClient::new(self.endpoints.clone())
                .await
                .map_err(parse_tikv_config_error)?
        } else if self.ca_path.is_some() && self.key_path.is_some() && self.cert_path.is_some() {
            let (ca_path, key_path, cert_path) = (
                self.ca_path.clone().unwrap(),
                self.key_path.clone().unwrap(),
                self.cert_path.clone().unwrap(),
            );
            let config = Config::default().with_security(ca_path, cert_path, key_path);
            RawClient::new_with_config(self.endpoints.clone(), config)
                .await
                .map_err(parse_tikv_config_error)?
        } else {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "invalid configuration")
                    .with_context("service", Scheme::Tikv)
                    .with_context("endpoints", format!("{:?}", self.endpoints)),
            );
        };
        self.client.set(client.clone()).ok();
        Ok(client)
    }
}

impl kv::Adapter for Adapter {
    type Scanner = ();

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::Tikv,
            "TiKV",
            Capability {
                read: true,
                write: true,
                blocking: false,
                shared: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let result = self
            .get_connection()
            .await?
            .get(path.to_owned())
            .await
            .map_err(parse_tikv_error)?;
        Ok(result.map(Buffer::from))
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        self.get_connection()
            .await?
            .put(path.to_owned(), value.to_vec())
            .await
            .map_err(parse_tikv_error)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.get_connection()
            .await?
            .delete(path.to_owned())
            .await
            .map_err(parse_tikv_error)
    }
}

fn parse_tikv_error(e: tikv_client::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "error from tikv").set_source(e)
}

fn parse_tikv_config_error(e: tikv_client::Error) -> Error {
    Error::new(ErrorKind::ConfigInvalid, "invalid configuration")
        .with_context("service", Scheme::Tikv)
        .set_source(e)
}
