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
use std::sync::Arc;

use super::config::D1Config;
use super::core::*;
use super::deleter::D1Deleter;
use super::writer::D1Writer;
use crate::raw::*;
use crate::*;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct D1Builder {
    pub(super) config: D1Config,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    pub(super) http_client: Option<HttpClient>,
}

impl Debug for D1Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("D1Builder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl D1Builder {
    /// Set api token for the cloudflare d1 service.
    ///
    /// create a api token from [here](https://dash.cloudflare.com/profile/api-tokens)
    pub fn token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }
        self
    }

    /// Set the account identifier for the cloudflare d1 service.
    ///
    /// get the account identifier from Workers & Pages -> Overview -> Account ID
    /// If not specified, it will return an error when building.
    pub fn account_id(mut self, account_id: &str) -> Self {
        if !account_id.is_empty() {
            self.config.account_id = Some(account_id.to_string());
        }
        self
    }

    /// Set the database identifier for the cloudflare d1 service.
    ///
    /// get the database identifier from Workers & Pages -> D1 -> [Your Database] -> Database ID
    /// If not specified, it will return an error when building.
    pub fn database_id(mut self, database_id: &str) -> Self {
        if !database_id.is_empty() {
            self.config.database_id = Some(database_id.to_string());
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

    /// Set the table name of the d1 service to read/write.
    ///
    /// If not specified, it will return an error when building.
    pub fn table(mut self, table: &str) -> Self {
        if !table.is_empty() {
            self.config.table = Some(table.to_owned());
        }
        self
    }

    /// Set the key field name of the d1 service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(mut self, key_field: &str) -> Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the d1 service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(mut self, value_field: &str) -> Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for D1Builder {
    type Config = D1Config;

    fn build(self) -> Result<impl Access> {
        let mut authorization = None;
        let config = self.config;

        if let Some(token) = config.token {
            authorization = Some(format_authorization_by_bearer(&token)?)
        }

        let Some(account_id) = config.account_id else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "account_id is required",
            ));
        };

        let Some(database_id) = config.database_id.clone() else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "database_id is required",
            ));
        };

        #[allow(deprecated)]
        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::D1)
            })?
        };

        let Some(table) = config.table.clone() else {
            return Err(Error::new(ErrorKind::ConfigInvalid, "table is required"));
        };

        let key_field = config
            .key_field
            .clone()
            .unwrap_or_else(|| "key".to_string());

        let value_field = config
            .value_field
            .clone()
            .unwrap_or_else(|| "value".to_string());

        let root = normalize_root(
            config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );
        Ok(D1Backend::new(D1Core {
            authorization,
            account_id,
            database_id,
            client,
            table,
            key_field,
            value_field,
        })
        .with_normalized_root(root))
    }
}

/// Backend for D1 services.
#[derive(Clone, Debug)]
pub struct D1Backend {
    core: Arc<D1Core>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl D1Backend {
    pub fn new(core: D1Core) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(Scheme::D1.into_static());
        info.set_name(&core.table);
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            stat: true,
            write: true,
            write_can_empty: true,
            // Cloudflare D1 supports 1MB as max in write_total.
            // refer to https://developers.cloudflare.com/d1/platform/limits/
            write_total_max_size: Some(1000 * 1000),
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

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info.set_root(&root);
        self.root = root;
        self
    }
}

impl Access for D1Backend {
    type Reader = Buffer;
    type Writer = D1Writer;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<D1Deleter>;

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
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in d1")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let bs = match self.core.get(&p).await? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(ErrorKind::NotFound, "kv not found in d1"));
            }
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), D1Writer::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(D1Deleter::new(self.core.clone(), self.root.clone())),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let _ = build_abs_path(&self.root, path);
        Ok((RpList::default(), ()))
    }
}
