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
use std::sync::Arc;

use tokio::sync::OnceCell;

use super::config::TikvConfig;
use super::core::*;
use super::deleter::TikvDeleter;
use super::writer::TikvWriter;
use crate::raw::*;
use crate::*;

/// TiKV backend builder
#[doc = include_str!("docs.md")]
#[derive(Clone, Default)]
pub struct TikvBuilder {
    pub(super) config: TikvConfig,
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
                    .with_context("endpoints", format!("{endpoints:?}")),
            )?;
        }

        Ok(TikvBackend::new(TikvCore {
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
#[derive(Clone, Debug)]
pub struct TikvBackend {
    core: Arc<TikvCore>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl TikvBackend {
    fn new(core: TikvCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Tikv.into_static());
        info.set_name("TiKV");
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            stat: true,
            write: true,
            write_can_empty: true,
            delete: true,
            shared: true,
            ..Default::default()
        });

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info: Arc::new(info),
        }
    }
}

impl Access for TikvBackend {
    type Reader = Buffer;
    type Writer = TikvWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<TikvDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p == build_abs_path(&self.root, "") {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            let bs = self.core.get(&p).await?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in tikv")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let bs = match self.core.get(&p).await? {
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "kv not found in tikv")),
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), TikvWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(TikvDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let _ = build_abs_path(&self.root, path);
        Ok((RpList::default(), ()))
    }
}
