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

use std::sync::Arc;

use goosefs_sdk::io::GooseFsFileWriter as ClientWriter;

use super::core::GooseFsCore;
use super::error::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

pub type GooseFsWriters = GooseFsWriter;

/// GooseFsWriter implements `oio::Write` using goosefs-sdk
/// high-level `GooseFsFileWriter`.
///
/// Key differences from AlluxioWriter:
/// - Alluxio uses stream_id based REST write: `create_file() → write(stream_id, data) → close(stream_id)`
/// - GooseFS uses block-level gRPC streaming: `GooseFsFileWriter::create() → write() → close()`
///   which internally handles block splitting, worker routing, and gRPC bidirectional streams.
pub struct GooseFsWriter {
    core: Arc<GooseFsCore>,
    op: OpWrite,
    path: String,
    /// Lazily initialized GooseFsFileWriter from goosefs-sdk.
    ///
    /// Created on first `write()` call, closed in `close()`.
    writer: Option<ClientWriter>,
}

impl GooseFsWriter {
    pub fn new(core: Arc<GooseFsCore>, op: OpWrite, path: String) -> Self {
        GooseFsWriter {
            core,
            op,
            path,
            writer: None,
        }
    }
}

impl oio::Write for GooseFsWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let writer = match &mut self.writer {
            Some(w) => w,
            None => {
                // Lazy init: create GooseFsFileWriter on first write.
                //
                // GooseFS's `createFile` RPC refuses to create a file
                // whose path already exists — there is no native
                // "overwrite" flag like HDFS's `CreateFlag.OVERWRITE`.
                // OpenDAL's default `write` contract, however, is
                // overwrite-by-default (see
                // `async_write::test_writer_write_with_overwrite`). We
                // emulate that semantics by pre-deleting an existing
                // target, *unless* the caller explicitly asked for
                // if-not-exists semantics.
                //
                // When `if_not_exists` is set and the file already
                // exists, `createFile` will surface `AlreadyExists`,
                // which we translate to `ConditionNotMatch` below so the
                // OpenDAL contract (see
                // `async_write::test_write_with_if_not_exists`) is
                // honoured.
                if !self.op.if_not_exists() {
                    // Best-effort pre-delete: ignore the result because
                    // (a) NotFound is the common case (fresh path), and
                    // (b) permission / transient errors will be
                    // reported by the subsequent create_writer call
                    // with a more informative context. The delete is
                    // non-recursive so we never stomp on a directory
                    // with the same name — if the path is a directory,
                    // the create_writer call will fail with a clear
                    // error from GooseFS.
                    let _ = self.core.delete(&self.path).await;
                }

                let w = self.core.create_writer(&self.path).await.map_err(|e| {
                    map_already_exists_for_if_not_exists(e, self.op.if_not_exists())
                })?;
                self.writer = Some(w);
                self.writer.as_mut().unwrap()
            }
        };

        writer.write(&bs.to_bytes()).await.map_err(parse_error)?;

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let Some(mut writer) = self.writer.take() else {
            // No data was written yet. OpenDAL's contract for
            // `write(path, "")` / `writer().close()` is to materialise
            // an empty object, so we create and immediately close a
            // zero-byte file here.
            //
            // As in the `write()` path above, we honour overwrite vs
            // if-not-exists semantics: default writes pre-delete any
            // existing target, if-not-exists writes let GooseFS surface
            // AlreadyExists which we translate to ConditionNotMatch.
            if !self.op.if_not_exists() {
                let _ = self.core.delete(&self.path).await;
            }
            let mut w =
                self.core.create_writer(&self.path).await.map_err(|e| {
                    map_already_exists_for_if_not_exists(e, self.op.if_not_exists())
                })?;
            w.close().await.map_err(parse_error)?;
            return Ok(Metadata::default());
        };

        writer.close().await.map_err(parse_error)?;

        Ok(Metadata::default())
    }

    async fn abort(&mut self) -> Result<()> {
        // GooseFS's streaming writer commits blocks to the UFS as they
        // are flushed, so by the time `abort()` is called the target
        // filename — and any blocks already staged on workers — are
        // usually already visible to the master. OpenDAL's contract
        // (see `async_write::test_writer_abort` /
        // `test_writer_abort_with_concurrent`) requires that an aborted
        // write leaves *no* file behind.
        //
        // The goosefs-sdk ships a purpose-built `GooseFsFileWriter::cancel()`
        // for exactly this case: it tears down the in-flight gRPC write
        // stream, drops the committed block list on the workers via
        // `remove_blocks`, and then issues a
        // `DeleteOptions::for_cancel()` (i.e. `unchecked=true`) against
        // the master so the INCOMPLETE inode is reaped. Using it here
        // gives us correct "no file, no blocks" semantics and avoids
        // leaking the orphaned blocks that a plain `delete(path)` on
        // the master inode would leave behind.
        //
        // If the writer was never actually started (no data written),
        // `self.writer` is `None` — there is nothing on the master for
        // this path, so there is nothing to clean up. We still fall
        // through to a best-effort delete to cover the rare case where
        // the path was pre-created by some other writer between builder
        // time and abort.
        if let Some(mut writer) = self.writer.take() {
            // `cancel()` is documented as idempotent and returns the
            // underlying master error only when the fallback delete
            // also fails — in which case we still swallow it because
            // the caller is abandoning the write and a remaining
            // orphan is less disruptive than a panic here.
            let _ = writer.cancel().await;
        }

        // Belt-and-braces: if `cancel()` above swallowed an error, or
        // if the writer was never created at all, make one more
        // best-effort attempt to remove whatever the master may be
        // holding under `self.path`. NotFound / other residual errors
        // are intentionally ignored.
        let _ = self.core.delete(&self.path).await;
        Ok(())
    }
}

/// When the caller requested `if_not_exists`, GooseFS's `AlreadyExists`
/// is exactly the condition the caller asked us to guard against — so
/// we surface it as `ConditionNotMatch` to match OpenDAL's contract
/// (see `async_write::test_write_with_if_not_exists`). For normal
/// writes, propagate the error unchanged.
fn map_already_exists_for_if_not_exists(err: Error, if_not_exists: bool) -> Error {
    if if_not_exists && err.kind() == ErrorKind::AlreadyExists {
        Error::new(
            ErrorKind::ConditionNotMatch,
            "target already exists and if_not_exists was set",
        )
        .set_source(err)
    } else {
        err
    }
}
