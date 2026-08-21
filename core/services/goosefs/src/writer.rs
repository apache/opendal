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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use goosefs_sdk::io::GoosefsFileWriter as ClientWriter;

use super::core::GoosefsCore;
use super::core::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

pub type GoosefsWriters = GoosefsWriter;

/// Process-wide monotonic counter used to disambiguate temporary file
/// names within a single process. Combined with the PID and a
/// nanosecond timestamp, this gives us a collision-free temporary
/// suffix without pulling in a UUID dependency.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Lifecycle position of a [`GoosefsWriter`], so that a re-entered
/// `close()` can tell "the caller never wrote anything" apart from "a
/// previous attempt already consumed the write". See the
/// [`GoosefsWriter`] docs for why that distinction is load-bearing.
#[derive(Debug)]
enum State {
    /// No SDK writer opened yet. `close()` here must honour OpenDAL's
    /// `write(path, "")` contract and materialise an empty object.
    Idle,
    /// Streaming into `tmp_path`.
    Streaming,
    /// A previous `close()` consumed the SDK writer and swept the staged
    /// temp. Nothing is left to finish and the write cannot be resumed.
    Failed,
    /// `close()` succeeded; the recorded metadata is replayed on re-entry.
    /// Boxed to keep the enum small — `Metadata` dwarfs the other variants.
    Closed(Box<Metadata>),
}

/// `GoosefsWriter` implements [`oio::Write`] on top of the goosefs-sdk
/// high-level streaming writer (`GoosefsFileWriter`).
///
/// # Write-via-temp protocol
///
/// To keep in-flight writes from being visible — and to make `abort()`
/// truly leave no trace — every write first streams into a sibling
/// temporary path, and is then renamed onto the caller's target in a
/// single metadata RPC on `close()`:
///
/// 1.  **open**: on the very first `write()` / `close()` call, we pick
///     a temporary path of the form
///     `"{dir}/.opendal.tmp.{pid}.{counter}.{nanos}.{basename}"`. The
///     temp name is intentionally produced in the **same parent
///     directory** as the final target so we stay on the same UFS
///     mount — this keeps the final `rename` a pure metadata op on
///     GooseFS and avoids a cross-filesystem data copy.
/// 2.  **stream**: bytes are written block-by-block into the temp path
///     via `GoosefsFileWriter`. We iterate over the incoming `Buffer`
///     chunks directly instead of flattening them with `to_bytes()`,
///     so multi-segment buffers don't force an extra concat copy.
/// 3.  **finalize**: `close()` calls `GoosefsFileWriter::close()` (which
///     commits the temp file on the master), then removes any
///     pre-existing object at the final path (overwrite) or returns
///     `ConditionNotMatch` (if-not-exists), and finally
///     `rename(temp, final)`.
/// 4.  **abort**: `abort()` cancels the in-flight SDK writer (which
///     drops any blocks already staged on workers) and removes the
///     temp inode on the master. The final target is never touched
///     in the abort path — by construction we haven't published
///     anything there yet.
///
/// This avoids the old failure mode where an interrupted write could
/// leave a half-written blob at the caller's final path, and it makes
/// overwrite vs if-not-exists decisions race only against concurrent
/// *metadata* mutations on the final path (a small, bounded window
/// around the `rename`).
///
/// # Why check if-not-exists twice
///
/// We check `if_not_exists` both up front (before opening the SDK
/// writer, so we don't waste bytes on a temp that will never be
/// finalized) and again at finalize time (because another writer may
/// have created the object while we were streaming). The second
/// check is what actually enforces the contract; the first is a
/// best-effort fast-path optimisation.
///
/// # Why `close()` must tolerate re-entry
///
/// `RetryLayer` retries a failed `close()` by calling it again on the
/// *same* writer rather than rebuilding one, and `CompleteWriter`
/// deliberately keeps its inner writer alive on that branch so the retry
/// can reach us. The zero-write branch therefore cannot be keyed off
/// "the SDK handle is gone": the first attempt consumes it, so a retry
/// would take that branch and publish an empty object over the caller's
/// target. [`State`] tracks the distinction explicitly.
pub struct GoosefsWriter {
    core: Arc<GoosefsCore>,
    op: OpWrite,
    /// Final destination path the caller asked us to write to.
    path: String,
    /// Sibling temporary path we actually stream into; materialised
    /// lazily on the first `write()` / `close()` call so that an
    /// `abort()` on a writer that never wrote anything is free.
    tmp_path: Option<String>,
    /// Lazily initialized SDK streaming writer, opened against
    /// `tmp_path`.
    writer: Option<ClientWriter>,
    state: State,
}

impl GoosefsWriter {
    pub fn new(core: Arc<GoosefsCore>, op: OpWrite, path: String) -> Self {
        GoosefsWriter {
            core,
            op,
            path,
            tmp_path: None,
            writer: None,
            state: State::Idle,
        }
    }

    /// Error used whenever a caller re-drives a writer whose write was
    /// already consumed. Persistent so `RetryLayer` stops immediately —
    /// there is no staged state left to finish.
    fn spent_error(&self, detail: &'static str) -> Error {
        Error::new(ErrorKind::Unexpected, detail)
            .with_context("service", super::GOOSEFS_SCHEME)
            .with_context("path", &self.path)
            .set_persistent()
    }

    /// Produce a sibling temporary path for `path`.
    ///
    /// The temp name is always placed in the parent directory of the
    /// final target so that the finalising `rename(temp, final)` stays
    /// on the same UFS mount and therefore resolves to a pure metadata
    /// operation on the GooseFS master (no data-plane copy across
    /// mounts). Format:
    ///
    /// ```text
    /// {dir}/.opendal.tmp.{pid}.{counter}.{nanos}.{basename}
    /// ```
    ///
    /// The `.opendal.tmp.` prefix makes leftover temps trivial to
    /// identify and sweep up; including PID + a monotonic counter +
    /// nanosecond timestamp guarantees uniqueness even under PID
    /// reuse or rapid successive writes from the same process.
    fn make_tmp_path(path: &str) -> String {
        // Split "dir/base". If there's no '/', dir is empty and base
        // is the whole path — this matches GooseFS's root-level file
        // case (e.g. `/foo` under the master's mount root).
        let (dir, base) = match path.rfind('/') {
            Some(idx) => (&path[..idx], &path[idx + 1..]),
            None => ("", path),
        };

        let pid = std::process::id();
        let counter = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);

        if dir.is_empty() {
            format!(".opendal.tmp.{pid}.{counter}.{nanos}.{base}")
        } else {
            format!("{dir}/.opendal.tmp.{pid}.{counter}.{nanos}.{base}")
        }
    }

    /// Lazily open the SDK writer against a freshly-allocated temp
    /// path, and cache both.
    async fn ensure_writer(&mut self) -> Result<&mut ClientWriter> {
        if self.writer.is_none() {
            // Fast-path only. Authority is GoosefsCore::rename(..., if_not_exists=true)
            // → Master no-replace rename (DefaultFileSystemMaster →
            // FileAlreadyExistsException → gRPC ALREADY_EXISTS → ConditionNotMatch).
            // Not this get_status.
            if self.op.if_not_exists() && self.core.get_status(&self.path).await.is_ok() {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "target already exists and if_not_exists was set",
                ));
            }

            let tmp = Self::make_tmp_path(&self.path);
            let w = self.core.create_writer(&tmp).await?;
            self.tmp_path = Some(tmp);
            self.writer = Some(w);
            self.state = State::Streaming;
        }
        Ok(self.writer.as_mut().expect("just ensured"))
    }

    /// Finish a write that streamed at least one chunk.
    ///
    /// On any failure the staged temp is swept and the writer is left in
    /// [`State::Failed`], so a retried `close()` reports the write as spent
    /// rather than silently publishing an empty object.
    async fn close_streaming(&mut self) -> Result<Metadata> {
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| self.spent_error("writer is streaming but the SDK handle is gone"))?;

        // Commit the temp inode on the master, then publish it onto the
        // caller's final path via `finalize_rename`.
        if let Err(e) = writer.close().await.map_err(parse_error) {
            // Close failed — the temp is in an indeterminate state on the
            // master; best-effort sweep to avoid leaks.
            if let Some(tmp) = self.tmp_path.take() {
                let _ = self.core.delete(&tmp).await;
            }
            return Err(e.set_persistent());
        }

        // Capture file_id before rename; Master rename keeps the same inode id.
        let mut meta = Metadata::default();
        if let Some(fid) = writer.file_info().file_id {
            meta.set_etag(&fid.to_string());
        }

        self.finalize_rename()
            .await
            .map_err(Error::set_persistent)?;

        self.state = State::Closed(Box::new(meta.clone()));
        Ok(meta)
    }

    /// Honour OpenDAL's `write(path, "")` contract for a writer that never
    /// received a `write()` call.
    ///
    /// Still goes through the temp-and-rename pipeline so overwrite /
    /// if-not-exists semantics remain identical to the streaming path.
    /// Every attempt allocates a fresh temp and leaves the target untouched
    /// until the closing rename, so this path is safe to retry as-is.
    async fn close_empty(&mut self) -> Result<Metadata> {
        if self.op.if_not_exists() && self.core.get_status(&self.path).await.is_ok() {
            return Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "target already exists and if_not_exists was set",
            ));
        }

        let tmp = Self::make_tmp_path(&self.path);
        let mut w = self.core.create_writer(&tmp).await?;
        if let Err(e) = w.close().await.map_err(parse_error) {
            let _ = self.core.delete(&tmp).await;
            return Err(e);
        }
        let mut meta = Metadata::default();
        if let Some(fid) = w.file_info().file_id {
            meta.set_etag(&fid.to_string());
        }
        self.tmp_path = Some(tmp);
        self.finalize_rename().await?;

        self.state = State::Closed(Box::new(meta.clone()));
        Ok(meta)
    }

    /// Finalize the temp file onto the caller's target path.
    ///
    /// Preconditions: `self.writer` has been `close()`d (or was never
    /// opened), and `self.tmp_path` names the committed temp inode on
    /// the master. Postconditions: on success, the temp inode has
    /// been renamed onto `self.path` and `self.tmp_path` is cleared;
    /// on failure, the temp inode is best-effort removed so we do not
    /// leak `.opendal.tmp.*` files on the UFS.
    async fn finalize_rename(&mut self) -> Result<()> {
        let Some(tmp) = self.tmp_path.take() else {
            // Nothing was ever written — `close()` handles the
            // "create an empty object" contract on its own path.
            return Ok(());
        };

        // Optional fast-path (optimization, not authority). Safe to keep:
        // a miss here is closed by rename(..., if_not_exists) below.
        if self.op.if_not_exists() && self.core.get_status(&self.path).await.is_ok() {
            // Undo our staged temp before surfacing the error.
            let _ = self.core.delete(&tmp).await;
            return Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "target already exists and if_not_exists was set",
            ));
        }

        // Authority: pass OpWrite.if_not_exists into core.rename.
        // Overwrite delete lives only inside core.rename when if_not_exists=false.
        match self
            .core
            .rename(&tmp, &self.path, self.op.if_not_exists())
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                // Rename failed — sweep the temp so we don't leak a
                // stray `.opendal.tmp.*` inode. Best-effort: if the
                // sweep fails, we still surface the original rename
                // error to the caller.
                let _ = self.core.delete(&tmp).await;
                Err(e)
            }
        }
    }
}

impl oio::Write for GoosefsWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        if matches!(self.state, State::Failed | State::Closed(_)) {
            return Err(self.spent_error("write() called on a writer that was already finished"));
        }

        let writer = self.ensure_writer().await?;

        // Iterate chunks directly instead of `bs.to_bytes()` — the
        // latter concatenates multi-segment buffers into a single
        // `Bytes`, doubling peak memory for the (common) case where
        // the caller already handed us a split buffer. `Buffer`'s
        // Iterator yields one `Bytes` per segment; the SDK writer
        // takes `&[u8]`, so each chunk flows through without a copy.
        for chunk in bs {
            writer.write(&chunk).await.map_err(parse_error)?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        // Default to `Failed` while the attempt runs: every arm below that
        // can legitimately be re-driven restores a better state explicitly,
        // so an early return can never leave the writer looking untouched.
        match std::mem::replace(&mut self.state, State::Failed) {
            // Idempotent success — replay the recorded result rather than
            // writing anything a second time.
            State::Closed(meta) => {
                self.state = State::Closed(meta.clone());
                Ok(*meta)
            }
            State::Failed => {
                Err(self
                    .spent_error("close() already failed for this writer; write cannot be resumed"))
            }
            State::Streaming => self.close_streaming().await,
            State::Idle => {
                let res = self.close_empty().await;
                if res.is_err() {
                    // Nothing was consumed: `close_empty` allocates a fresh
                    // temp per attempt and never touches the target until
                    // the closing rename, so retrying is still safe.
                    self.state = State::Idle;
                }
                res
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        // Temp-and-rename makes abort straightforward: the caller's
        // final path has *never* been touched, so there is nothing to
        // roll back there. All we need to do is tear down the staged
        // temp, both on the data plane (in-flight gRPC stream +
        // blocks on workers) and on the metadata plane (temp inode on
        // the master).
        if let Some(mut writer) = self.writer.take() {
            // `cancel()` tears down the in-flight bidi gRPC stream,
            // drops any blocks already committed to workers via
            // `remove_blocks`, and issues `DeleteOptions::for_cancel`
            // against the master (i.e. `unchecked=true`) so the
            // INCOMPLETE inode is reaped. We swallow its error here
            // because (a) the caller is abandoning the write and a
            // stray temp block is less disruptive than a panic, and
            // (b) the follow-up `delete(tmp)` below is our
            // belt-and-braces guarantee that the temp inode is gone.
            let _ = writer.cancel().await;
        }

        if let Some(tmp) = self.tmp_path.take() {
            // Best-effort sweep — NotFound means `cancel()` already
            // reaped it, which is the expected happy path.
            let _ = self.core.delete(&tmp).await;
        }

        // An aborted writer is spent. Without this, a `close()` arriving
        // after `abort()` would fall into the zero-write branch and
        // materialise an empty object the caller never asked for.
        self.state = State::Failed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GOOSEFS_SCHEME;
    use goosefs_sdk::config::GoosefsConfig as ClientConfig;
    use opendal_core::raw::oio::Write as _;

    /// Build a writer without touching the network: `GoosefsCore` defers the
    /// master connection to the first RPC, and every assertion below stays on
    /// paths that never issue one.
    fn offline_writer() -> GoosefsWriter {
        let core = GoosefsCore::new(
            ServiceInfo::new(GOOSEFS_SCHEME, "/data/", ""),
            Capability {
                write: true,
                ..Default::default()
            },
            "/data/".to_string(),
            ClientConfig::new("127.0.0.1:9200"),
        );
        GoosefsWriter::new(
            Arc::new(core),
            OpWrite::default(),
            "some-object".to_string(),
        )
    }

    /// A fresh writer must still honour `write(path, "")`.
    #[test]
    fn idle_writer_takes_the_empty_object_branch() {
        let w = offline_writer();
        assert!(matches!(w.state, State::Idle));
    }

    /// `abort()` marks the writer spent so a later `close()` cannot fall
    /// through to the zero-write branch.
    #[tokio::test]
    async fn close_after_abort_does_not_create_an_empty_object() {
        let mut w = offline_writer();

        // Idle abort is purely local: no SDK writer and no staged temp.
        w.abort().await.expect("abort on an idle writer is free");
        assert!(matches!(w.state, State::Failed));

        let err = w
            .close()
            .await
            .expect_err("close after abort must not publish an empty object");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(
            err.is_persistent(),
            "the write is unrecoverable, so RetryLayer must stop: {err}"
        );
    }

    /// Models what `RetryLayer` does after a failed `close()`: it calls
    /// `close()` again on the *same* writer instance. `CompleteWriter`
    /// deliberately keeps the inner writer alive on the failure branch so this
    /// retry can happen, so the second call must report the write as spent
    /// rather than taking the zero-write branch and publishing an empty object
    /// over the caller's target.
    #[tokio::test]
    async fn retried_close_after_failure_never_publishes_an_empty_object() {
        let mut w = offline_writer();
        // A streaming writer whose SDK handle is already gone is exactly the
        // shape a partially-failed close attempt leaves behind.
        w.state = State::Streaming;
        assert!(w.writer.is_none());

        let first = w
            .close()
            .await
            .expect_err("streaming close without an SDK handle must fail");
        assert!(first.is_persistent(), "must not be retried: {first}");
        assert!(matches!(w.state, State::Failed));

        let second = w
            .close()
            .await
            .expect_err("the retried close must not succeed");
        assert!(second.is_persistent(), "must not be retried: {second}");
        assert!(
            matches!(w.state, State::Failed),
            "the writer must stay spent no matter how often it is re-driven"
        );
    }

    /// A successful `close()` is idempotent: re-entry replays the recorded
    /// metadata rather than writing anything a second time.
    #[tokio::test]
    async fn close_replays_metadata_when_already_closed() {
        let mut w = offline_writer();
        let mut meta = Metadata::default();
        meta.set_etag("42");
        w.state = State::Closed(Box::new(meta));

        let first = w.close().await.expect("replayed close");
        assert_eq!(first.etag(), Some("42"));

        let second = w.close().await.expect("still replayed");
        assert_eq!(second.etag(), Some("42"));
    }

    /// Writing after the writer is finished must fail loudly rather than
    /// silently opening a second temp that nobody will publish.
    #[tokio::test]
    async fn write_after_finish_is_rejected() {
        let mut w = offline_writer();
        w.state = State::Failed;

        let err = w
            .write(Buffer::from("late"))
            .await
            .expect_err("write on a spent writer must fail");
        assert!(err.is_persistent(), "must not be retried: {err}");
    }
}
