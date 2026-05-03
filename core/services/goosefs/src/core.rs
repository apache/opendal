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
use std::sync::atomic::{AtomicU32, Ordering};

use goosefs_sdk::client::MasterClient;
use goosefs_sdk::config::GooseFsConfig as ClientConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::io::{GooseFsFileReader, GooseFsFileWriter};
use goosefs_sdk::proto::grpc::file::FileInfo;
use tokio::sync::RwLock;

use super::error::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

/// GooseFS core that encapsulates all interactions with goosefs-sdk.
///
/// # Connection architecture
///
/// Unlike AlluxioCore which directly builds HTTP requests, `GooseFsCore`
/// delegates to the goosefs-sdk high-level API which handles:
/// - HA master discovery (`PollingMasterInquireClient`)
/// - Consistent hash worker routing (`WorkerRouter`)
/// - Block-level bidirectional streaming I/O (`GrpcBlockReader/Writer`)
/// - gRPC flow control and ACK management
///
/// `GooseFsCore` holds a lazily-initialised [`FileSystemContext`] shared by all
/// operations on the same backend instance.  The context owns:
///
/// - One persistent gRPC channel to the Master (reused by all metadata RPCs)
/// - One persistent gRPC channel to the WorkerManager
/// - One shared `WorkerClientPool` (reused by all streaming writers/readers)
/// - One shared `WorkerRouter` with TTL-refreshed worker list
/// - One background `ConfigRefresher` that reloads `goosefs-site.properties`
///   every 60s and updates the transparent-acceleration switches
///
/// The context is built on **first** RPC (not at `build()` time) because
/// [`raw::Builder::build`] is synchronous but `FileSystemContext::connect` is
/// async.  Subsequent RPCs are zero-cost `Arc` clones — no extra TCP+SASL.
///
/// # Cross-process safety
///
/// When LanceDB (via VectorDBBench) uses Python `multiprocessing.spawn`,
/// child processes re-import modules and reconstruct Python objects but
/// **inherit** a stale `GooseFsCore`.  The gRPC channels and SASL streams
/// from the parent are invalid in the child because they belong to a
/// different tokio runtime and OS-level TCP connections.
///
/// To handle this, `GooseFsCore` records the **PID** of the process that
/// created the `FileSystemContext`.  On each call to [`ctx()`] the current
/// PID is compared; if it differs the old context is discarded and a
/// fresh `FileSystemContext::connect` is performed.
#[derive(Clone)]
pub struct GooseFsCore {
    pub info: Arc<AccessorInfo>,
    /// Normalized root path (e.g. `/data/`)
    pub root: String,
    /// GooseFS client configuration (also used to seed the context).
    pub config: ClientConfig,
    /// Lazily-initialised shared context, protected by `RwLock` so it can
    /// be replaced when the process ID changes (i.e. after `fork`/`spawn`).
    ctx: Arc<RwLock<Option<Arc<FileSystemContext>>>>,
    /// The PID of the process that initialised `ctx`.  `0` means not yet
    /// initialised.
    ctx_pid: Arc<AtomicU32>,
}

impl Debug for GooseFsCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pid = self.ctx_pid.load(Ordering::Relaxed);
        f.debug_struct("GooseFsCore")
            .field("root", &self.root)
            .field("master_addr", &self.config.master_addr)
            .field("ctx_pid", &pid)
            .finish_non_exhaustive()
    }
}

impl GooseFsCore {
    /// Create a new `GooseFsCore`.
    ///
    /// The [`FileSystemContext`] is **not** connected here; it is established
    /// on the first RPC and then reused for the lifetime of this core.
    pub fn new(info: Arc<AccessorInfo>, root: String, config: ClientConfig) -> Self {
        Self {
            info,
            root,
            config,
            ctx: Arc::new(RwLock::new(None)),
            ctx_pid: Arc::new(AtomicU32::new(0)),
        }
    }

    // ── Shared-connection acquisition ─────────────────────────────────────

    /// Return the shared [`FileSystemContext`], connecting on first use.
    ///
    /// The connection attempt includes:
    /// - Master gRPC handshake (HA-aware via `PollingMasterInquireClient`)
    /// - WorkerManager gRPC handshake
    /// - Initial worker-list fetch
    /// - Spawning the background worker-refresh and config-refresh tasks
    ///
    /// All of this happens **once per process per `GooseFsCore`**; subsequent
    /// calls within the same process are O(1) `Arc` clones.
    ///
    /// # Cross-process reconnection
    ///
    /// If the current PID differs from the PID that originally created the
    /// context (e.g. after Python `multiprocessing.spawn`), the stale
    /// context is dropped and a fresh one is established.  This ensures
    /// gRPC channels and SASL streams are always valid for the calling
    /// process.
    async fn ctx(&self) -> Result<Arc<FileSystemContext>> {
        let current_pid = std::process::id();
        let stored_pid = self.ctx_pid.load(Ordering::Relaxed);

        // Fast path: context exists and belongs to this process.
        if stored_pid == current_pid {
            let guard = self.ctx.read().await;
            if let Some(ref ctx) = *guard {
                return Ok(Arc::clone(ctx));
            }
            // Stored PID matches but ctx is None — fall through to init.
        }

        // Slow path: first call OR process changed → take write lock.
        let mut guard = self.ctx.write().await;

        // Double-check after acquiring write lock: another task may have
        // initialised it while we were waiting.
        let pid_now = self.ctx_pid.load(Ordering::Relaxed);
        if pid_now == current_pid {
            if let Some(ref ctx) = *guard {
                return Ok(Arc::clone(ctx));
            }
        }

        // If we had a stale context from a different process, drop it.
        if pid_now != 0 && pid_now != current_pid {
            log::info!(
                "GooseFsCore: PID changed {} -> {}, re-establishing FileSystemContext",
                pid_now,
                current_pid,
            );
            *guard = None;
        }

        // Create a brand-new context.
        // `FileSystemContext::connect` already returns `Arc<FileSystemContext>`.
        let ctx = FileSystemContext::connect(self.config.clone())
            .await
            .map_err(parse_error)?;
        *guard = Some(Arc::clone(&ctx));
        self.ctx_pid.store(current_pid, Ordering::Relaxed);

        Ok(ctx)
    }

    /// Acquire the shared Master client — zero network I/O after first connect.
    async fn master(&self) -> Result<Arc<MasterClient>> {
        Ok(self.ctx().await?.acquire_master())
    }

    /// Reset the shared [`FileSystemContext`], forcing the next `ctx()` call
    /// to establish a fresh connection.
    ///
    /// This is used as a last-resort recovery when an authentication failure
    /// propagates up from the goosefs-sdk layer, indicating that the entire
    /// context (including all cached gRPC channels and SASL streams) is stale.
    async fn reset_ctx(&self) {
        let mut guard = self.ctx.write().await;
        *guard = None;
        self.ctx_pid.store(0, Ordering::Relaxed);
        log::info!("GooseFsCore: context reset, will re-establish on next use");
    }

    /// Build the full GooseFS path from a relative OpenDAL path.
    fn full_path(&self, path: &str) -> String {
        build_rooted_abs_path(&self.root, path)
    }

    // ── Metadata Operations ──────────────────────────────────
    //
    // All metadata RPCs reuse the Master connection inside `FileSystemContext`.
    // No TCP+SASL handshake per call.

    pub async fn create_dir(&self, path: &str) -> Result<()> {
        let full = self.full_path(path);
        let master = self.master().await?;
        master
            .create_directory(&full, true)
            .await
            .map_err(parse_error)
    }

    pub async fn get_status(&self, path: &str) -> Result<FileInfo> {
        let full = self.full_path(path);
        let master = self.master().await?;
        master.get_status(&full).await.map_err(parse_error)
    }

    pub async fn list_status(&self, path: &str) -> Result<Vec<FileInfo>> {
        let full = self.full_path(path);
        let master = self.master().await?;
        master.list_status(&full, false).await.map_err(parse_error)
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let full = self.full_path(path);
        let master = self.master().await?;
        match master.delete(&full, false).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Idempotent delete: NotFound is OK
                if matches!(e, goosefs_sdk::error::Error::NotFound { .. }) {
                    Ok(())
                } else {
                    Err(parse_error(e))
                }
            }
        }
    }

    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let src = self.full_path(from);
        let dst = self.full_path(to);
        let master = self.master().await?;
        master.rename(&src, &dst).await.map_err(parse_error)
    }

    // ── Data I/O Operations ──────────────────────────────────
    //
    // Writes go through `*_with_context` — they reuse the Master channel,
    // WorkerRouter and WorkerClientPool from `FileSystemContext`.
    //
    // All read entry points go through `FileSystemContext` as well, so the
    // Master channel, the worker-list snapshot and the `WorkerClientPool`
    // are shared with metadata and write paths — zero per-call handshake
    // on the hot read path.

    /// Read file data (full or range).
    ///
    /// # Authentication failure recovery
    ///
    /// If the read fails with an authentication error (SASL stream expired),
    /// this method resets the entire `FileSystemContext` (which drops all
    /// cached gRPC channels and SASL streams) and retries **once** with a
    /// fresh context.  This is the last line of defence — normally the
    /// goosefs-sdk layer handles auth reconnection at the `WorkerClientPool`
    /// level, but when the context itself is stale (e.g. after process fork)
    /// this outer retry ensures recovery.
    pub async fn read_file(
        &self,
        path: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> Result<bytes::Bytes> {
        let full = self.full_path(path);
        let ctx = self.ctx().await?;

        let result = match (offset, length) {
            (Some(off), Some(len)) => {
                GooseFsFileReader::read_range_with_context(ctx, &full, off, len).await
            }
            _ => GooseFsFileReader::read_file_with_context(ctx, &full).await,
        };

        match result {
            Ok(data) => Ok(data),
            Err(e) if e.is_authentication_failed() => {
                // Authentication failed — the entire context may be stale.
                // Reset it and retry once with a fresh FileSystemContext.
                log::warn!(
                    "GooseFsCore::read_file: authentication failed for {}, resetting context and retrying: {}",
                    full,
                    e
                );
                self.reset_ctx().await;
                let fresh_ctx = self.ctx().await?;
                match (offset, length) {
                    (Some(off), Some(len)) => {
                        GooseFsFileReader::read_range_with_context(fresh_ctx, &full, off, len)
                            .await
                            .map_err(parse_error)
                    }
                    _ => GooseFsFileReader::read_file_with_context(fresh_ctx, &full)
                        .await
                        .map_err(parse_error),
                }
            }
            Err(e) => Err(parse_error(e)),
        }
    }

    /// Write file data (single call).
    #[allow(dead_code)]
    pub async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
        let full = self.full_path(path);
        let ctx = self.ctx().await?;
        let mut writer = GooseFsFileWriter::create_with_context(ctx, &full, None)
            .await
            .map_err(parse_error)?;
        writer.write(data).await.map_err(parse_error)?;
        writer.close().await.map_err(parse_error)?;
        Ok(())
    }

    /// Create a streaming file writer that reuses the shared context.
    pub async fn create_writer(&self, path: &str) -> Result<GooseFsFileWriter> {
        let full = self.full_path(path);
        let ctx = self.ctx().await?;
        GooseFsFileWriter::create_with_context(ctx, &full, None)
            .await
            .map_err(parse_error)
    }

    /// Create a streaming file reader (full-file) via the shared context.
    #[allow(dead_code)]
    pub async fn open_reader(&self, path: &str) -> Result<GooseFsFileReader> {
        let full = self.full_path(path);
        let ctx = self.ctx().await?;
        GooseFsFileReader::open_with_context(ctx, &full)
            .await
            .map_err(parse_error)
    }

    /// Open a range reader via the shared context.
    #[allow(dead_code)]
    pub async fn open_range_reader(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<GooseFsFileReader> {
        let full = self.full_path(path);
        let ctx = self.ctx().await?;
        GooseFsFileReader::open_range_with_context(ctx, &full, offset, length)
            .await
            .map_err(parse_error)
    }

    // ── Metadata Conversion ──────────────────────────────────

    /// Convert goosefs FileInfo to OpenDAL Metadata.
    pub fn file_info_to_metadata(&self, info: &FileInfo) -> Metadata {
        let mut metadata = if info.folder.unwrap_or(false) {
            Metadata::new(EntryMode::DIR)
        } else {
            Metadata::new(EntryMode::FILE)
        };

        if let Some(length) = info.length {
            metadata.set_content_length(length as u64);
        }
        if let Some(mtime) = info.last_modification_time_ms {
            if let Ok(ts) = Timestamp::from_millisecond(mtime) {
                metadata.set_last_modified(ts);
            }
        }
        metadata
    }

    /// Convert goosefs FileInfo to OpenDAL Metadata (for list results),
    /// also returning the relative path.
    pub fn file_info_to_entry(&self, info: &FileInfo) -> Result<(String, Metadata)> {
        let path = info.path.clone().unwrap_or_default();
        let rel_path = if info.folder.unwrap_or(false) {
            format!("{}/", path)
        } else {
            path
        };
        let rel = build_rel_path(&self.root, &rel_path);
        Ok((rel, self.file_info_to_metadata(info)))
    }
}
