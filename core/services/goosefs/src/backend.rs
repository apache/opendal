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

use log::debug;

use super::GOOSEFS_SCHEME;
use super::config::GooseFsConfig;
use super::core::GooseFsCore;
use super::deleter::GooseFsDeleter;
use super::lister::GooseFsLister;
use super::reader::GooseFsReader;
use super::writer::GooseFsWriter;
use super::writer::GooseFsWriters;
use opendal_core::raw::*;
use opendal_core::*;

/// [GooseFS](https://cloud.tencent.com/product/goosefs) services support via native gRPC.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct GooseFsBuilder {
    pub(super) config: GooseFsConfig,
}

impl Debug for GooseFsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GooseFsBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl GooseFsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };
        self
    }

    /// Set master address(es).
    ///
    /// Single master: `"10.0.0.1:9200"`
    /// HA (comma-separated): `"10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200"`
    ///
    /// When provided here it **overrides** any address discovered from
    /// `goosefs-site.properties` or `GOOSEFS_MASTER_ADDR`. When left unset
    /// the builder falls back to the SDK's auto-discovery chain
    /// (defaults → properties → env) — see `goosefs-sdk`
    /// `docs/CLIENT_CONFIGURATION.md` §1. `build()` fails with
    /// `ConfigInvalid` only if **no** source supplies a master address.
    pub fn master_addr(mut self, addr: &str) -> Self {
        if !addr.is_empty() {
            self.config.master_addr = Some(addr.to_string());
        }
        self
    }

    /// Set block size for new files (bytes).
    pub fn block_size(mut self, size: u64) -> Self {
        self.config.block_size = Some(size);
        self
    }

    /// Set chunk size for streaming RPCs (bytes).
    pub fn chunk_size(mut self, size: u64) -> Self {
        self.config.chunk_size = Some(size);
        self
    }

    /// Set default write type.
    ///
    /// Values: `"must_cache"`, `"cache_through"`, `"through"`, `"async_through"`
    pub fn write_type(mut self, wt: &str) -> Self {
        if !wt.is_empty() {
            self.config.write_type = Some(wt.to_string());
        }
        self
    }

    /// Set authentication type.
    ///
    /// Values: `"nosasl"`, `"simple"` (default: `"simple"`).
    /// - `"nosasl"` — skip SASL authentication entirely.
    /// - `"simple"` — PLAIN SASL with username (server does not verify password).
    pub fn auth_type(mut self, auth_type: &str) -> Self {
        if !auth_type.is_empty() {
            self.config.auth_type = Some(auth_type.to_string());
        }
        self
    }

    /// Set authentication username.
    ///
    /// Used in SIMPLE mode as the login identity.
    /// Default: current OS user (`$USER` / `$USERNAME`).
    pub fn auth_username(mut self, username: &str) -> Self {
        if !username.is_empty() {
            self.config.auth_username = Some(username.to_string());
        }
        self
    }
}

impl Builder for GooseFsBuilder {
    type Config = GooseFsConfig;

    /// Build the backend and return a GooseFsBackend.
    fn build(self) -> Result<impl Access> {
        debug!("GooseFsBuilder::build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("GooseFsBuilder use root {}", &root);

        // ── Step 1: establish the base SDK config ─────────────────────────────
        //
        // We follow the same priority chain that `FileSystemContext::connect`
        // (and its `ConfigRefresher`) uses — see
        // `docs/CLIENT_CONFIGURATION.md` §1 "Configuration Loading Priority":
        //
        //   defaults  <  goosefs-site.properties  <  GOOSEFS_* env vars
        //
        // `GooseFsConfig::from_properties_auto()` already implements this
        // chain and is the *same* function the SDK calls every 60s to refresh
        // the transparent-acceleration switches. Using it here keeps the
        // initial OpenDAL build and the in-process hot-reload semantically
        // aligned — users who deploy `goosefs-site.properties` get the exact
        // same config from both paths.
        //
        // Failure policy:
        //   * no properties file found  → silently uses defaults + env
        //     (`from_properties_auto` handles this internally)
        //   * properties file present but malformed → hard-fail
        //     (broken config must not be silently dropped)
        //
        // Builder-explicit fields (Step 2) have the final say, overriding
        // anything discovered from properties / env.
        let mut goosefs_config = goosefs_sdk::config::GooseFsConfig::from_properties_auto()
            .map_err(|e| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    format!("failed to auto-load goosefs config: {e}"),
                )
                .with_operation("Builder::build")
                .with_context("service", GOOSEFS_SCHEME)
            })?;

        // Root always comes from OpenDAL (it's an OpenDAL-layer concept).
        goosefs_config.root = root.clone();

        // ── Step 2: overlay builder-explicit fields (authoritative) ───────────
        if let Some(ref master_addr) = self.config.master_addr {
            let addrs: Vec<String> = master_addr
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if addrs.is_empty() {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "master_addr is empty after trimming",
                )
                .with_operation("Builder::build")
                .with_context("service", GOOSEFS_SCHEME));
            }

            if addrs.len() == 1 {
                goosefs_config.master_addr = addrs[0].clone();
                goosefs_config.master_addrs = Vec::new();
            } else {
                goosefs_config.master_addr = addrs[0].clone();
                goosefs_config.master_addrs = addrs;
            }
        }

        // After properties/env auto-load + builder overlay, we must still
        // have at least one usable master address. If we don't, fail fast
        // with a message that points the user at all three sources.
        if goosefs_config.master_addr.is_empty() && goosefs_config.master_addrs.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "master_addr is not configured: set it via GooseFsBuilder::master_addr(...), \
                 the `master_addr` config key, the GOOSEFS_MASTER_ADDR env var, \
                 or `goosefs.master.hostname`/`goosefs.master.rpc.addresses` in goosefs-site.properties",
            )
            .with_operation("Builder::build")
            .with_context("service", GOOSEFS_SCHEME));
        }
        debug!(
            "GooseFsBuilder use master_addr {} (addrs={:?})",
            &goosefs_config.master_addr, &goosefs_config.master_addrs
        );

        if let Some(block_size) = self.config.block_size {
            goosefs_config.block_size = block_size;
        }
        if let Some(chunk_size) = self.config.chunk_size {
            goosefs_config.chunk_size = chunk_size;
        }

        // Parse write_type string → goosefs_sdk::WritePType i32
        if let Some(ref wt) = self.config.write_type {
            let wt_i32 = match wt.as_str() {
                "must_cache" | "MUST_CACHE" => 1,
                "try_cache" | "TRY_CACHE" => 2,
                "cache_through" | "CACHE_THROUGH" => 3,
                "through" | "THROUGH" => 4,
                "async_through" | "ASYNC_THROUGH" => 5,
                _ => 1, // default to MUST_CACHE
            };
            goosefs_config.write_type = Some(wt_i32);
        }

        // Parse auth_type string → goosefs_sdk::auth::AuthType
        if let Some(ref auth_type_str) = self.config.auth_type {
            goosefs_config = goosefs_config
                .with_auth_type_str(auth_type_str)
                .map_err(|e| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        format!("invalid auth_type: {}", e),
                    )
                    .with_operation("Builder::build")
                    .with_context("service", GOOSEFS_SCHEME)
                })?;
        }

        if let Some(ref auth_username) = self.config.auth_username {
            goosefs_config = goosefs_config.with_auth_username(auth_username);
        }

        // ── Step 3: validate the final merged config ──────────────────────────
        goosefs_config.validate().map_err(|e| {
            Error::new(
                ErrorKind::ConfigInvalid,
                format!("invalid goosefs config: {e}"),
            )
            .with_operation("Builder::build")
            .with_context("service", GOOSEFS_SCHEME)
        })?;

        Ok(GooseFsBackend {
            core: Arc::new(GooseFsCore::new(
                {
                    let am = AccessorInfo::default();
                    am.set_scheme(GOOSEFS_SCHEME)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            read: true,
                            write: true,
                            write_can_multi: true,
                            // GooseFS createFile fails with AlreadyExists if file exists,
                            // which naturally provides if_not_exists semantics.
                            // Lance Dataset relies on this for manifest commit safety.
                            write_with_if_not_exists: true,
                            create_dir: true,
                            delete: true,
                            list: true,
                            rename: true,
                            shared: true,
                            ..Default::default()
                        });
                    am.into()
                },
                root,
                goosefs_config,
            )),
        })
    }
}

#[derive(Debug, Clone)]
pub struct GooseFsBackend {
    core: Arc<GooseFsCore>,
}

impl Access for GooseFsBackend {
    type Reader = GooseFsReader;
    type Writer = GooseFsWriters;
    type Lister = oio::PageLister<GooseFsLister>;
    type Deleter = oio::OneShotDeleter<GooseFsDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.create_dir(path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let file_info = self.core.get_status(path).await?;
        Ok(RpStat::new(self.core.file_info_to_metadata(&file_info)))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let reader = GooseFsReader::new(self.core.clone(), path.to_string(), args);
        Ok((RpRead::new(), reader))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let w = GooseFsWriter::new(self.core.clone(), args.clone(), path.to_string());
        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(GooseFsDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = GooseFsLister::new(self.core.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        self.core.rename(from, to).await?;
        Ok(RpRename::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_build() {
        let builder = GooseFsBuilder::default()
            .root("/data")
            .master_addr("127.0.0.1:9200")
            .build();
        assert!(builder.is_ok());
    }

    #[test]
    fn test_builder_ha() {
        let builder = GooseFsBuilder::default()
            .root("/data")
            .master_addr("10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200")
            .build();
        assert!(builder.is_ok());
    }

    /// `master_addr` is mandatory — `build()` must fail with `ConfigInvalid`
    /// when it cannot be resolved from any source. This test exercises the
    /// "explicitly blank" form (empty / whitespace / comma-only), which is
    /// environment-independent: Step 2 short-circuits on a blank override
    /// before any auto-load value can rescue it.
    #[test]
    fn test_builder_blank_master_addr_fails() {
        let err = GooseFsBuilder::default()
            .root("/data")
            .master_addr("   ,  , ")
            .build()
            .expect_err("build must fail when master_addr is blank");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(
            err.to_string().contains("master_addr is empty"),
            "unexpected error message: {err}"
        );
    }
}
