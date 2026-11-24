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

use super::D1_SCHEME;
use super::config::D1Config;
use super::core::*;
use super::deleter::D1Deleter;
use super::reader::*;
use super::writer::D1Writer;
use opendal_core::raw::*;
use opendal_core::*;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct D1Builder {
    pub(super) config: D1Config,
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

    fn build(self) -> Result<impl Service> {
        let mut authorization = None;
        let config = self.config;

        if let Some(token) = config.token {
            if token.is_empty() {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "token must not be empty",
                ));
            }
            authorization = Some(format_authorization_by_bearer(&token)?);
        }

        let Some(account_id) = config.account_id.filter(|v| !v.is_empty()) else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "account_id is required",
            ));
        };

        let Some(database_id) = config.database_id.filter(|v| !v.is_empty()) else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "database_id is required",
            ));
        };

        let Some(table) = config.table.filter(|v| !v.is_empty()) else {
            return Err(Error::new(ErrorKind::ConfigInvalid, "table is required"));
        };
        let _ = quote_ident(&table)?;

        let key_field = match config.key_field {
            Some(v) if v.is_empty() => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "key_field must not be empty",
                ));
            }
            Some(v) => v,
            None => "key".to_string(),
        };
        let _ = quote_ident(&key_field)?;

        let value_field = match config.value_field {
            Some(v) if v.is_empty() => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "value_field must not be empty",
                ));
            }
            Some(v) => v,
            None => "value".to_string(),
        };
        let _ = quote_ident(&value_field)?;

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
    pub(crate) core: Arc<D1Core>,
    pub(crate) root: String,
    pub(crate) info: ServiceInfo,
    pub(crate) capability: Capability,
}

impl D1Backend {
    pub fn new(core: D1Core) -> Self {
        let info = ServiceInfo::new(D1_SCHEME, "/", &core.table);
        let capability = Capability {
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
        };

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info,
            capability,
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info = self.info.with_root(&root);
        self.root = root;
        self
    }
}

impl Service for D1Backend {
    type Reader = oio::StreamReader<D1Reader>;
    type Writer = D1Writer;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<D1Deleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_absolute_path(&self.root, path);

        if p == build_absolute_path(&self.root, "") {
            Ok(RpStat::new(MetadataBuilder::dir().build()))
        } else {
            match self.core.get_length(ctx, &p).await? {
                Some(length) => Ok(RpStat::new({
                    let metadata = MetadataBuilder::file(length as u64);
                    metadata.build()
                })),
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in d1")),
            }
        }
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<D1Reader> = {
            Ok(oio::StreamReader::new(D1Reader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
        let output: D1Writer = {
            let p = build_absolute_path(&self.root, path);
            Ok(D1Writer::new(self.core.clone(), ctx.clone(), p))
        }?;

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<D1Deleter> = {
            Ok(oio::OneShotDeleter::new(D1Deleter::new(
                self.core.clone(),
                ctx.clone(),
                self.root.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, _path: &str, _args: OpList) -> Result<Self::Lister> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_rejects_empty_and_nul_config_fields() {
        let valid = D1Config {
            account_id: Some("acc123".to_string()),
            database_id: Some("db456".to_string()),
            table: Some("kv".to_string()),
            ..Default::default()
        };

        let mut empty_account = valid.clone();
        empty_account.account_id = Some(String::new());

        let mut empty_db = valid.clone();
        empty_db.database_id = Some(String::new());

        let mut empty_table = valid.clone();
        empty_table.table = Some(String::new());

        let mut empty_token = valid.clone();
        empty_token.token = Some(String::new());

        let mut empty_key_field = valid.clone();
        empty_key_field.key_field = Some(String::new());

        let mut empty_value_field = valid.clone();
        empty_value_field.value_field = Some(String::new());

        let mut nul_table = valid.clone();
        nul_table.table = Some("kv\0bad".to_string());

        let mut nul_key_field = valid.clone();
        nul_key_field.key_field = Some("k\0bad".to_string());

        let mut nul_value_field = valid;
        nul_value_field.value_field = Some("v\0bad".to_string());

        for cfg in [
            empty_account,
            empty_db,
            empty_table,
            empty_token,
            empty_key_field,
            empty_value_field,
            nul_table,
            nul_key_field,
            nul_value_field,
        ] {
            let err = cfg.into_builder().build().err().unwrap();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }

        for empty_required in ["account_id", "database_id", "table"] {
            let mut map = vec![
                ("account_id".to_string(), "acc123".to_string()),
                ("database_id".to_string(), "db456".to_string()),
                ("table".to_string(), "kv".to_string()),
            ];
            for (k, v) in &mut map {
                if k == empty_required {
                    v.clear();
                }
            }
            let cfg = D1Config::from_iter(map).unwrap();
            let err = cfg.into_builder().build().err().unwrap();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }
    }

    #[test]
    fn backend_debug_redacts_token() {
        let sentinel = "d1-backend-secret-token";
        let backend = D1Builder::default()
            .token(sentinel)
            .account_id("acc123")
            .database_id("db456")
            .table("kv")
            .build()
            .unwrap();
        assert!(!format!("{backend:?}").contains(sentinel));
    }
}
