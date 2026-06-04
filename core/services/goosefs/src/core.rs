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
use goosefs_sdk::config::GoosefsConfig as ClientConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};
use goosefs_sdk::proto::grpc::file::FileInfo;
use tokio::sync::RwLock;

use super::error::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

/// GooseFS core that encapsulates all interactions with goosefs-sdk.
///
/// # Connection architecture
///
/// Unlike AlluxioCore which directly builds HTTP requests, `GoosefsCore`
/// delegates to the goosefs-sdk high-level API which handles:
/// - HA master discovery (`PollingMasterInquireClient`)
/// - Consistent hash worker routing (`WorkerRouter`)
/// - Block-level bidirectional streaming I/O (`GrpcBlockReader/Writer`)
/// - gRPC flow control and ACK management
///
/// `GoosefsCore` holds a lazily-initialised [`FileSystemContext`] shared by all
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
/// **inherit** a stale `GoosefsCore`.  The gRPC channels and SASL streams
/// from the parent are invalid in the child because they belong to a
/// different tokio runtime and OS-level TCP connections.
///
/// To handle this, `GoosefsCore` records the **PID** of the process that
/// created the `FileSystemContext`.  On each call to [`ctx()`] the current
/// PID is compared; if it differs the old context is discarded and a
/// fresh `FileSystemContext::connect` is performed.
#[derive(Clone)]
pub struct GoosefsCore {
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

impl Debug for GoosefsCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pid = self.ctx_pid.load(Ordering::Relaxed);
        f.debug_struct("GoosefsCore")
            .field("root", &self.root)
            .field("master_addr", &self.config.master_addr)
            .field("ctx_pid", &pid)
            .finish_non_exhaustive()
    }
}

impl GoosefsCore {
    /// Create a new `GoosefsCore`.
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
    /// All of this happens **once per process per `GoosefsCore`**; subsequent
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
                "GoosefsCore: PID changed {} -> {}, re-establishing FileSystemContext",
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
        log::info!("GoosefsCore: context reset, will re-establish on next use");
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

        // Mirror HDFS / Alluxio semantics so GooseFS passes OpenDAL's
        // behavior contract for rename (see `rename_test.go::testRenameNested`
        // and `testRenameOverwrite`):
        //
        //   1. Destination parent directory is created on demand
        //      (`testRenameNested` writes to `<uuid>/<uuid>/<uuid>` without
        //      mkdir'ing intermediate levels first; S3-style backends treat
        //      this as implicit, fs-style backends must emulate it).
        //   2. If the destination exists as a file, overwrite by deleting
        //      it first (`testRenameOverwrite`). GooseFS `rename` itself
        //      would otherwise return AlreadyExists.
        //   3. If the destination exists as a directory, surface
        //      IsADirectory — matches the error code the behavior tests
        //      assert in `testRenameTargetDir`.
        //
        // Source NotFound is reported by the underlying `rename` RPC
        // (`testRenameNonExistingSource` expects `NotFound`), so we don't
        // pre-stat the source.
        match master.get_status(&dst).await {
            Ok(info) => {
                if info.folder.unwrap_or(false) {
                    return Err(Error::new(
                        ErrorKind::IsADirectory,
                        "rename destination is a directory",
                    )
                    .with_context("service", super::GOOSEFS_SCHEME)
                    .with_context("from", from)
                    .with_context("to", to));
                }
                // Destination is an existing file — delete it so the
                // subsequent rename can overwrite atomically at the
                // master level.
                master.delete(&dst, false).await.map_err(parse_error)?;
            }
            Err(goosefs_sdk::error::Error::NotFound { .. }) => {
                // Ensure the destination's parent directory exists.
                // `create_directory` with recursive=true is idempotent,
                // so calling it for a parent that already exists is a
                // cheap metadata no-op on the master.
                if let Some(parent) = parent_of(&dst) {
                    master
                        .create_directory(parent, true)
                        .await
                        .map_err(parse_error)?;
                }
            }
            Err(e) => return Err(parse_error(e)),
        }

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
    /// One-shot read of a file (or a sub-range), buffering the full
    /// payload in memory.
    ///
    /// Historically this was the entry point for the reader, but the
    /// streaming `GoosefsReadStream` now pulls data block-by-block directly
    /// from [`Self::open_range_reader`] / [`Self::open_reader`]. We keep
    /// `read_file` around (muted with `#[allow(dead_code)]`) because
    /// (a) it's useful for small reads / internal diagnostics, and
    /// (b) it carries the canonical `is_authentication_failed` reset
    ///     recipe that the streaming openers mirror — keeping them
    ///     side-by-side makes the pattern easier to audit.
    #[allow(dead_code)]
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
                GoosefsFileReader::read_range_with_context(ctx, &full, off, len).await
            }
            _ => GoosefsFileReader::read_file_with_context(ctx, &full).await,
        };

        match result {
            Ok(data) => Ok(data),
            Err(e) if e.is_authentication_failed() => {
                // Authentication failed — the entire context may be stale.
                // Reset it and retry once with a fresh FileSystemContext.
                log::warn!(
                    "GoosefsCore::read_file: authentication failed for {}, resetting context and retrying: {}",
                    full,
                    e
                );
                self.reset_ctx().await;
                let fresh_ctx = self.ctx().await?;
                match (offset, length) {
                    (Some(off), Some(len)) => {
                        GoosefsFileReader::read_range_with_context(fresh_ctx, &full, off, len)
                            .await
                            .map_err(parse_error)
                    }
                    _ => GoosefsFileReader::read_file_with_context(fresh_ctx, &full)
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
        let mut writer = GoosefsFileWriter::create_with_context(ctx, &full, None)
            .await
            .map_err(parse_error)?;
        writer.write(data).await.map_err(parse_error)?;
        writer.close().await.map_err(parse_error)?;
        Ok(())
    }

    /// Create a streaming file writer that reuses the shared context.
    pub async fn create_writer(&self, path: &str) -> Result<GoosefsFileWriter> {
        let full = self.full_path(path);
        let ctx = self.ctx().await?;
        GoosefsFileWriter::create_with_context(ctx, &full, None)
            .await
            .map_err(parse_error)
    }

    /// Create a streaming file reader (full-file) via the shared context.
    ///
    /// Applies the same one-shot authentication-reset retry as
    /// [`Self::read_file`]: if opening the reader fails with a stale
    /// SASL/auth error, we drop the cached `FileSystemContext`, rebuild
    /// it, and try once more. Transient non-auth errors are propagated
    /// unchanged.
    pub async fn open_reader(&self, path: &str) -> Result<GoosefsFileReader> {
        let full = self.full_path(path);
        let ctx = self.ctx().await?;
        match GoosefsFileReader::open_with_context(ctx, &full).await {
            Ok(r) => Ok(r),
            Err(e) if e.is_authentication_failed() => {
                log::warn!(
                    "GoosefsCore::open_reader: authentication failed for {}, resetting context and retrying: {}",
                    full,
                    e
                );
                self.reset_ctx().await;
                let fresh_ctx = self.ctx().await?;
                GoosefsFileReader::open_with_context(fresh_ctx, &full)
                    .await
                    .map_err(parse_error)
            }
            Err(e) => Err(parse_error(e)),
        }
    }

    /// Open a range reader via the shared context.
    ///
    /// Same auth-reset-and-retry semantics as [`Self::open_reader`].
    pub async fn open_range_reader(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<GoosefsFileReader> {
        let full = self.full_path(path);
        let ctx = self.ctx().await?;
        match GoosefsFileReader::open_range_with_context(ctx, &full, offset, length).await {
            Ok(r) => Ok(r),
            Err(e) if e.is_authentication_failed() => {
                log::warn!(
                    "GoosefsCore::open_range_reader: authentication failed for {} [{}..+{}], resetting context and retrying: {}",
                    full,
                    offset,
                    length,
                    e
                );
                self.reset_ctx().await;
                let fresh_ctx = self.ctx().await?;
                GoosefsFileReader::open_range_with_context(fresh_ctx, &full, offset, length)
                    .await
                    .map_err(parse_error)
            }
            Err(e) => Err(parse_error(e)),
        }
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

/// Return the parent directory of an absolute GooseFS path, or `None`
/// when the path has no meaningful parent to create (i.e. it already
/// points at the filesystem root).
///
/// Input paths here are always produced by [`GoosefsCore::full_path`],
/// so they are already normalized, absolute (leading `/`) and do not
/// contain trailing slashes (OpenDAL's `build_rooted_abs_path` strips
/// them for non-directory paths). We deliberately do not depend on
/// `opendal_core::raw::get_parent` because that function's contract is
/// defined for OpenDAL *relative* paths and treats trailing slashes
/// specially; here we want fs-like semantics.
fn parent_of(path: &str) -> Option<&str> {
    // `path` is expected to be absolute. If it isn't, still behave
    // sensibly by returning None instead of panicking.
    let trimmed = path.strip_suffix('/').unwrap_or(path);
    match trimmed.rfind('/') {
        // "/foo" → parent is "/" — no need to create the root itself.
        Some(0) => None,
        // "/foo/bar" → parent is "/foo"
        Some(idx) => Some(&trimmed[..idx]),
        // No slash at all → no parent to create.
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::parent_of;

    #[test]
    fn parent_of_root_returns_none() {
        assert_eq!(parent_of("/"), None);
    }

    #[test]
    fn parent_of_top_level_entry_returns_none() {
        // "/foo" has `/` as parent; creating the root is a no-op so we
        // skip it entirely.
        assert_eq!(parent_of("/foo"), None);
        assert_eq!(parent_of("/foo/"), None);
    }

    #[test]
    fn parent_of_nested_entry_returns_parent() {
        assert_eq!(parent_of("/a/b/c"), Some("/a/b"));
        assert_eq!(parent_of("/a/b/c/"), Some("/a/b"));
        assert_eq!(parent_of("/a/b/c/d"), Some("/a/b/c"));
    }

    #[test]
    fn parent_of_relative_path_returns_none() {
        // Defensive: we never expect a relative path here, but we
        // should not panic on one.
        assert_eq!(parent_of("foo"), None);
        assert_eq!(parent_of(""), None);
    }
}
