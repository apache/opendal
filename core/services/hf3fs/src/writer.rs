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

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Buf;
use opendal_core::raw::*;
use opendal_core::*;

use crate::backend::Hf3fsCore;
use crate::driver::{IoDriver, RegisteredFile};

pub(crate) struct Hf3fsWriter {
    core: Arc<Hf3fsCore>,
    path: PathBuf,
    executor: Executor,
    options: OpWrite,
    inner: Option<oio::PositionWriter<Hf3fsWriteHandle>>,
}

impl Hf3fsWriter {
    pub fn new(core: Arc<Hf3fsCore>, path: PathBuf, executor: Executor, options: OpWrite) -> Self {
        Self {
            core,
            path,
            executor,
            options,
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut oio::PositionWriter<Hf3fsWriteHandle>> {
        if self.inner.is_none() {
            let path = self.path.clone();
            let api = self.core.api.clone();
            let if_not_exists = self.options.if_not_exists();
            let (file, staged) = tokio::task::spawn_blocking(move || {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(new_std_io_error)?;
                }
                // A conditional write fills a staged file that close publishes,
                // so the target never holds partial data.
                let staged = if if_not_exists {
                    Some(Arc::new(StagedFile::create(&path)?))
                } else {
                    None
                };
                let open_path = staged.as_ref().map_or(&path, |staged| &staged.path);
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .create_new(staged.is_some())
                    .truncate(true)
                    .open(open_path)
                    .map_err(new_std_io_error)?;
                let registered = RegisteredFile::new(api, file);
                if registered.is_err()
                    && let Some(staged) = &staged
                {
                    let _ = std::fs::remove_file(&staged.path);
                }
                registered.map(|file| (file, staged))
            })
            .await
            .map_err(new_task_join_error)??;
            self.inner = Some(oio::PositionWriter::new(
                self.executor.clone(),
                Hf3fsWriteHandle {
                    file,
                    driver: self.core.write.clone(),
                    staged,
                },
                self.options.concurrent(),
            ));
        }
        Ok(self.inner.as_mut().expect("writer initialized"))
    }
}

impl oio::Write for Hf3fsWriter {
    async fn write(&mut self, buffer: Buffer) -> Result<()> {
        self.inner().await?.write(buffer).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner().await?.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(inner) = self.inner.as_mut() {
            inner.abort().await?;
        }
        Ok(())
    }
}

struct Hf3fsWriteHandle {
    file: Arc<RegisteredFile>,
    driver: IoDriver,
    /// Set for `if_not_exists` writes. `file` then holds the staged file,
    /// which `close` publishes at the target.
    staged: Option<Arc<StagedFile>>,
}

impl oio::PositionWrite for Hf3fsWriteHandle {
    async fn write_all_at(&self, mut offset: u64, mut buffer: Buffer) -> Result<()> {
        while !buffer.is_empty() {
            let size = buffer.len().min(self.driver.block_size);
            let (written, _) = self
                .driver
                .submit(self.file.clone(), offset, size, Some(buffer.slice(..size)))
                .await?;
            if written == 0 {
                return Err(new_std_io_error(std::io::ErrorKind::WriteZero.into()));
            }
            offset += written as u64;
            buffer.advance(written);
        }
        Ok(())
    }

    async fn close(&self, _: u64) -> Result<Metadata> {
        let file = self.file.clone();
        let staged = self.staged.clone();
        tokio::task::spawn_blocking(move || {
            file.file.sync_all().map_err(new_std_io_error)?;
            if let Some(staged) = &staged {
                staged.publish()?;
            }
            let stat = file.file.metadata().map_err(new_std_io_error)?;
            let mut metadata = MetadataBuilder::file(stat.len());
            metadata.last_modified(Timestamp::try_from(
                stat.modified().map_err(new_std_io_error)?,
            )?);
            Ok(metadata.build())
        })
        .await
        .map_err(new_task_join_error)?
    }

    async fn abort(&self) -> Result<()> {
        let Some(staged) = &self.staged else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "3FS writes without if_not_exists cannot be rolled back",
            ));
        };
        let staged = Arc::clone(staged);
        tokio::task::spawn_blocking(move || staged.discard())
            .await
            .map_err(new_task_join_error)?
    }
}

/// The staged file of an `if_not_exists` write, which becomes visible at
/// `target` only when `close` publishes it.
///
/// 3FS rejects `renameat2` flags, so publishing links the staged file to the
/// target, which fails when the target exists, and then removes the staged
/// name. The staged file stays in the target's directory because a hard link
/// cannot cross filesystems.
struct StagedFile {
    /// staged file path
    path: PathBuf,
    /// target file path
    target: PathBuf,
    /// Held across each publish or discard attempt, so a `close` that is
    /// cancelled and retried observes the first attempt instead of linking twice.
    state: Mutex<StagedState>,
}

enum StagedState {
    /// The staged file exists and the target is untouched.
    Pending,
    /// The target names the staged file.
    Published,
    /// The staged file was removed without publishing.
    /// A later `close` reports this error again.
    Discarded(Error),
}

impl StagedFile {
    fn create(target: &Path) -> Result<Self> {
        let name = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::new(ErrorKind::IsADirectory, "target is not a file path"))?;
        // Fail fast instead of staging content that the publish must reject.
        if target.try_exists().map_err(new_std_io_error)? {
            return Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "file already exists",
            ));
        }
        Ok(Self {
            path: target.with_file_name(build_tmp_path_of(name)),
            target: target.to_path_buf(),
            state: Mutex::new(StagedState::Pending),
        })
    }

    /// Make the staged file visible at the target without replacing an
    /// existing entry.
    fn publish(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        match &*state {
            StagedState::Published => return Ok(()),
            StagedState::Discarded(err) => return Err(Error::new(err.kind(), err.message())),
            StagedState::Pending => {}
        }

        let err = match std::fs::hard_link(&self.path, &self.target) {
            Ok(()) => None,
            // Another writer published the target while this write was staged.
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Some(
                Error::new(ErrorKind::ConditionNotMatch, "file already exists").set_source(err),
            ),
            Err(err) => Some(new_std_io_error(err)),
        };
        if let Some(mut err) = err {
            // Keep the staged file when a retried close can still publish it.
            if err.is_temporary() {
                return Err(err);
            }
            *state = StagedState::Discarded(Error::new(err.kind(), err.message()));
            if let Err(cleanup_err) = std::fs::remove_file(&self.path)
                && cleanup_err.kind() != std::io::ErrorKind::NotFound
            {
                err = err.with_context("staging_cleanup", cleanup_err);
            }
            return Err(err);
        }
        *state = StagedState::Published;

        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            // The target is published, but the staged file is orphaned.
            Err(err) => {
                Err(new_std_io_error(err).with_context("service_operation", "RemoveStagedFile"))
            }
        }
    }

    /// Remove the staged file.
    fn discard(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        match &*state {
            // The write is committed, so abort only removes a staged name that
            // publish left behind.
            StagedState::Published => {
                let mut err = Error::new(
                    ErrorKind::Unexpected,
                    "staged file has already been published",
                );
                if let Err(cleanup_err) = std::fs::remove_file(&self.path)
                    && cleanup_err.kind() != std::io::ErrorKind::NotFound
                {
                    err = err.with_context("staging_cleanup", cleanup_err);
                }
                return Err(err);
            }
            StagedState::Discarded(_) => return Ok(()),
            StagedState::Pending => {}
        }

        // Keep the state pending when removal fails, so abort can be retried.
        if let Err(err) = std::fs::remove_file(&self.path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(new_std_io_error(err));
        }

        *state = StagedState::Discarded(Error::new(
            ErrorKind::Unexpected,
            "staged write was aborted",
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use oio::Write;
    use opendal_executor_tokio::TokioExecutor;

    use super::*;
    use crate::ffi::Api;

    #[tokio::test]
    async fn unopened_writer_abort_succeeds_without_creating_file() {
        let dir = tempfile::tempdir().unwrap();
        let api = Api::noop();
        let (read, _read_rx, _) = IoDriver::for_test(api.clone(), 2, 4096, true);
        let (write, _write_rx, _) = IoDriver::for_test(api.clone(), 2, 4096, false);
        let core = Arc::new(Hf3fsCore {
            root: dir.path().to_path_buf(),
            api,
            read,
            write,
        });

        let target = dir.path().join("unopened/file");
        let mut writer = Hf3fsWriter::new(
            core,
            target.clone(),
            Executor::with(TokioExecutor::default()),
            OpWrite::new(),
        );
        writer.abort().await.unwrap();
        assert!(!target.exists());
    }

    #[tokio::test]
    async fn opened_writer_abort_clears_buffer_cancels_tasks_and_reports_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let api = Api::noop();
        let (read, _read_rx, _) = IoDriver::for_test(api.clone(), 2, 4096, true);
        let (write, mut write_rx, _) = IoDriver::for_test(api.clone(), 2, 4096, false);
        let core = Arc::new(Hf3fsCore {
            root: dir.path().to_path_buf(),
            api,
            read,
            write,
        });

        let (options, _) = OpWrite::from_options(
            &Capability::default(),
            options::WriteOptions {
                concurrent: 2,
                ..Default::default()
            },
        )
        .unwrap();
        let target = dir.path().join("opened/file");
        let mut writer = Hf3fsWriter::new(
            core,
            target.clone(),
            Executor::with(TokioExecutor::default()),
            options,
        );

        // First write stays cached in PositionWriter; second write spawns an
        // async task for the first chunk and caches the second chunk.
        writer.write(Buffer::from("first")).await.unwrap();
        writer.write(Buffer::from("second")).await.unwrap();

        let mut pending_req = tokio::time::timeout(Duration::from_secs(5), write_rx.recv())
            .await
            .expect("first write should reach driver")
            .expect("driver channel open");
        assert!(!pending_req.is_cancelled());

        let err = writer.abort().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);

        // PositionWriter::abort() cancels its concurrent tasks and clears its
        // buffered chunk even though native rollback returns Unsupported.
        tokio::time::timeout(Duration::from_secs(5), pending_req.cancelled())
            .await
            .expect("in-flight concurrent write task should be cancelled on abort");

        // Closing after abort must not submit the cleared buffered chunk.
        writer.close().await.unwrap();
        assert!(write_rx.try_recv().is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"");
    }

    #[tokio::test]
    async fn conditional_writer_rejects_existing_target_without_staging() {
        let dir = tempfile::tempdir().unwrap();
        let api = Api::noop();
        let (read, _read_rx, _) = IoDriver::for_test(api.clone(), 2, 4096, true);
        let (write, _write_rx, _) = IoDriver::for_test(api.clone(), 2, 4096, false);
        let core = Arc::new(Hf3fsCore {
            root: dir.path().to_path_buf(),
            api,
            read,
            write,
        });
        let (options, _) = OpWrite::from_options(
            &Capability {
                write_with_if_not_exists: true,
                ..Default::default()
            },
            options::WriteOptions {
                if_not_exists: true,
                ..Default::default()
            },
        )
        .unwrap();
        let target = dir.path().join("file");
        std::fs::write(&target, b"old").unwrap();
        let mut writer = Hf3fsWriter::new(
            core,
            target.clone(),
            Executor::with(TokioExecutor::default()),
            options,
        );

        let err = writer.write(Buffer::from("new")).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        let err = writer.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert_eq!(std::fs::read(&target).unwrap(), b"old");
        // The target is the only entry, so no staged file was created.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn conditional_writer_abort_discards_staged_file() {
        let dir = tempfile::tempdir().unwrap();
        let api = Api::noop();
        let (read, _read_rx, _) = IoDriver::for_test(api.clone(), 2, 4096, true);
        let (write, _write_rx, _) = IoDriver::for_test(api.clone(), 2, 4096, false);
        let core = Arc::new(Hf3fsCore {
            root: dir.path().to_path_buf(),
            api,
            read,
            write,
        });
        let (options, _) = OpWrite::from_options(
            &Capability {
                write_with_if_not_exists: true,
                ..Default::default()
            },
            options::WriteOptions {
                if_not_exists: true,
                ..Default::default()
            },
        )
        .unwrap();
        let target = dir.path().join("file");
        let mut writer = Hf3fsWriter::new(
            core,
            target.clone(),
            Executor::with(TokioExecutor::default()),
            options,
        );

        // The first write only opens the staged file and caches its chunk.
        writer.write(Buffer::from("first")).await.unwrap();
        assert!(!target.exists());
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names.len(), 1);
        assert!(names[0].starts_with("file."));

        writer.abort().await.unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        // The discarded write can no longer be committed or aborted twice.
        let err = writer.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        writer.abort().await.unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn conditional_writer_abort_cannot_undo_publish() {
        let dir = tempfile::tempdir().unwrap();
        let api = Api::noop();
        let (read, _read_rx, _) = IoDriver::for_test(api.clone(), 2, 4096, true);
        let (write, _write_rx, _) = IoDriver::for_test(api.clone(), 2, 4096, false);
        let core = Arc::new(Hf3fsCore {
            root: dir.path().to_path_buf(),
            api,
            read,
            write,
        });
        let (options, _) = OpWrite::from_options(
            &Capability {
                write_with_if_not_exists: true,
                ..Default::default()
            },
            options::WriteOptions {
                if_not_exists: true,
                ..Default::default()
            },
        )
        .unwrap();
        let target = dir.path().join("file");
        let mut writer = Hf3fsWriter::new(
            core,
            target.clone(),
            Executor::with(TokioExecutor::default()),
            options,
        );

        writer.close().await.unwrap();
        let err = writer.abort().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(target.exists());
    }

    #[test]
    fn staged_file_publish_links_target_and_removes_staged_name() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("file");
        let staged = StagedFile::create(&target).unwrap();
        std::fs::write(&staged.path, b"new").unwrap();

        staged.publish().unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
        // The published target is the only entry, so the staged name is gone.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);

        // Publishing again must not treat the published target as a conflict.
        staged.publish().unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn staged_file_publish_keeps_existing_target_and_discards_staged_file() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("file");
        let staged = StagedFile::create(&target).unwrap();
        std::fs::write(&staged.path, b"new").unwrap();
        // Another writer wins the target while this write is staged.
        std::fs::write(&target, b"old").unwrap();

        let err = staged.publish().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert_eq!(std::fs::read(&target).unwrap(), b"old");
        // The winner is the only entry, so the staged file is removed.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);

        // The outcome is final: a retried close reports the same conflict.
        let err = staged.publish().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        staged.discard().unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn staged_file_discard_removes_staged_file() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("file");
        let staged = StagedFile::create(&target).unwrap();
        std::fs::write(&staged.path, b"new").unwrap();

        staged.discard().unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        // Aborting again succeeds and the discarded write cannot be committed.
        staged.discard().unwrap();
        let err = staged.publish().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
