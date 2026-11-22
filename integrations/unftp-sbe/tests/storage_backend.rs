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

use anyhow::{Context, Result};
use opendal::{Operator, services};
use tempfile::TempDir;
use unftp_core::auth::DefaultUser;
use unftp_core::storage::{ErrorKind, StorageBackend};
use unftp_sbe_opendal::OpendalStorage;

fn fs_storage() -> Result<(TempDir, OpendalStorage)> {
    let root = tempfile::tempdir()?;
    let fs = services::Fs::default().root(root.path().to_str().context("non-UTF-8 test root")?);
    Ok((root, OpendalStorage::new(Operator::new(fs)?)))
}

async fn read_file(storage: &OpendalStorage, path: &str) -> Result<String> {
    let mut reader = storage.get(&DefaultUser, path, 0).await.context("get")?;
    let mut content = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut content).await?;
    Ok(content)
}

/// `start_pos == 0` stores the input verbatim, whether the file exists or not.
#[tokio::test]
async fn put_overwrites_at_start_position_zero() -> Result<()> {
    let (_root, storage) = fs_storage()?;
    let user = DefaultUser;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put abc")?;
    storage
        .put(&user, "def".as_bytes(), "file", 0)
        .await
        .context("put def")?;

    assert_eq!(read_file(&storage, "file").await?, "def");
    Ok(())
}

/// `start_pos` equal to the current file size appends on an append-capable
/// backend, which is how libunftp implements `APPE`.
#[tokio::test]
async fn put_appends_at_end_of_file() -> Result<()> {
    let root = tempfile::tempdir()?;
    let fs = services::Fs::default().root(root.path().to_str().context("non-UTF-8 test root")?);
    let op = Operator::new(fs)?;
    assert!(op.info().capability().write_can_append);
    let storage = OpendalStorage::new(op);
    let user = DefaultUser;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put abc")?;
    storage
        .put(&user, "def".as_bytes(), "file", 3)
        .await
        .context("put def at start_pos 3")?;

    assert_eq!(read_file(&storage, "file").await?, "abcdef");
    Ok(())
}

/// A nonzero `start_pos` that does not match the file size must fail instead
/// of overwriting the file.
#[tokio::test]
async fn put_rejects_offset_past_end_of_file() -> Result<()> {
    let (_root, storage) = fs_storage()?;
    let user = DefaultUser;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put abc")?;

    for start_pos in [1, 100] {
        let err = storage
            .put(&user, "def".as_bytes(), "file", start_pos)
            .await
            .expect_err("put with mismatched start_pos must fail");
        assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);
        assert_eq!(
            read_file(&storage, "file").await?,
            "abc",
            "file must be preserved after rejected put at start_pos {start_pos}"
        );
    }
    Ok(())
}

/// Without `write_can_append` a nonzero `start_pos` is reported as
/// unimplemented and leaves the file untouched.
#[tokio::test]
async fn put_rejects_offset_on_non_append_backend() -> Result<()> {
    let op = Operator::new(services::Memory::default())?;
    assert!(!op.info().capability().write_can_append);
    let storage = OpendalStorage::new(op);
    let user = DefaultUser;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put abc")?;

    let err = storage
        .put(&user, "def".as_bytes(), "file", 3)
        .await
        .expect_err("put at start_pos 3 must fail on a non-append backend");
    assert_eq!(err.kind(), ErrorKind::CommandNotImplemented);
    assert_eq!(read_file(&storage, "file").await?, "abc");
    Ok(())
}

#[tokio::test]
async fn cwd_on_filesystem_backend() -> Result<()> {
    let (_root, storage) = fs_storage()?;
    let user = DefaultUser;

    storage.cwd(&user, "/").await.context("cwd /")?;
    storage.cwd(&user, "").await.context("cwd empty path")?;

    storage.mkd(&user, "dir").await.context("mkd dir")?;
    storage.cwd(&user, "dir").await.context("cwd dir")?;
    storage.cwd(&user, "dir/").await.context("cwd dir/")?;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put file")?;
    let err = storage
        .cwd(&user, "file")
        .await
        .expect_err("cwd file must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);

    let err = storage
        .cwd(&user, "nonexistent")
        .await
        .expect_err("cwd nonexistent must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);
    Ok(())
}

/// Memory keeps no directory entries of its own, so a directory only exists
/// implicitly through its children. `cwd` must still accept it while
/// rejecting files and absent paths, like on object stores.
#[tokio::test]
async fn cwd_on_memory_backend() -> Result<()> {
    let storage = OpendalStorage::new(Operator::new(services::Memory::default())?);
    let user = DefaultUser;

    storage.cwd(&user, "/").await.context("cwd /")?;
    storage.cwd(&user, "").await.context("cwd empty path")?;

    storage
        .put(&user, "abc".as_bytes(), "dir/file", 0)
        .await
        .context("put dir/file")?;
    storage
        .cwd(&user, "dir")
        .await
        .context("cwd into implicit dir")?;
    storage
        .cwd(&user, "dir/")
        .await
        .context("cwd into implicit dir/")?;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put file")?;
    let err = storage
        .cwd(&user, "file")
        .await
        .expect_err("cwd file must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);

    let err = storage
        .cwd(&user, "nonexistent")
        .await
        .expect_err("cwd nonexistent must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);
    Ok(())
}

/// Backends without the list capability cannot probe implicit prefix
/// directories; `cwd` must still report `PermanentDirectoryNotAvailable`
/// instead of leaking an internal `LocalError`.
#[tokio::test]
async fn cwd_on_backend_without_list_capability() -> Result<()> {
    let root = tempfile::tempdir()?;
    let cacache =
        services::Cacache::default().datadir(root.path().to_str().context("non-UTF-8 test root")?);
    let op = Operator::new(cacache)?;
    assert!(!op.info().capability().list);
    let storage = OpendalStorage::new(op);
    let user = DefaultUser;

    storage.cwd(&user, "/").await.context("cwd /")?;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put file")?;
    let err = storage
        .cwd(&user, "file")
        .await
        .expect_err("cwd file must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);

    let err = storage
        .cwd(&user, "nonexistent")
        .await
        .expect_err("cwd nonexistent must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);
    Ok(())
}
