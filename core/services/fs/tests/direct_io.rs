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

use opendal_core::{ErrorKind, Operator};
use opendal_service_fs::Fs;

#[cfg(not(target_os = "linux"))]
#[test]
fn direct_io_is_rejected_before_creating_directories() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let staging = temp.path().join("staging");
    let result = Operator::new(
        Fs::default()
            .root(root.to_str().unwrap())
            .atomic_write_dir(staging.to_str().unwrap())
            .direct_io(true),
    );
    assert_eq!(result.err().unwrap().kind(), ErrorKind::ConfigInvalid);
    assert!(!root.exists());
    assert!(!staging.exists());
}

#[cfg(target_os = "linux")]
mod linux {
    use std::fs::{self, File};
    use std::path::Path;

    use bytes::Bytes;
    use opendal_core::options::WriteOptions;
    use opendal_core::raw::oio::{Read, ReadStreamDyn};
    use opendal_core::raw::{OpRead, OpWrite, Service};
    use opendal_core::{Buffer, Builder, BytesRange, OperationContext};
    use rustix::fs::fstatfs;

    use super::*;

    #[tokio::test]
    async fn direct_io_preserves_unaligned_ranges_and_logical_write_length() {
        let dir = tempfile::tempdir().unwrap();
        let fs_type = fstatfs(File::open(dir.path()).unwrap()).unwrap().f_type as u32;
        // Other filesystems may reject O_DIRECT or lack STATX_DIOALIGN.
        if !matches!(fs_type, 0xef53 | 0x5846_5342) {
            return;
        }

        let root = dir.path().join("root");
        let staging = dir.path().join("staging");
        let pattern: Bytes = (0..(1024 * 1024 + 17))
            .map(|i| ((i / 251 + i % 251) % 256) as u8)
            .collect();
        for atomic in [false, true] {
            let mut builder = Fs::default().root(root.to_str().unwrap()).direct_io(true);
            if atomic {
                builder = builder.atomic_write_dir(staging.to_str().unwrap());
            }
            let op = Operator::new(builder).unwrap();
            for size in [4096, 3, 0, pattern.len()] {
                let data = pattern.slice(..size);
                let segments: Buffer = data.chunks(4093).map(Bytes::copy_from_slice).collect();
                let metadata = op.write("data", segments).await.unwrap();
                assert_eq!(metadata.content_length(), size as u64);
                assert_eq!(fs::read(root.join("data")).unwrap(), data);
            }

            let data = pattern.slice(..16_519);
            let mut writer = op
                .writer_with("data")
                .chunk(127)
                .concurrent(4)
                .await
                .unwrap();
            for chunk in data.chunks(193) {
                let (first, rest) = chunk.split_at(chunk.len() / 2);
                let segments = Buffer::from(vec![
                    Bytes::copy_from_slice(first),
                    Bytes::copy_from_slice(rest),
                ]);
                writer.write(segments).await.unwrap();
            }
            let metadata = writer.close().await.unwrap();
            assert_eq!(metadata.content_length(), data.len() as u64);
            assert_eq!(fs::read(root.join("data")).unwrap(), data);

            let size = data.len() as u64;
            for range in [1..2, 4094..4101, size - 3..size] {
                let result = op.read_with("data").range(range.clone()).await.unwrap();
                assert_eq!(
                    result.to_bytes(),
                    data.slice(range.start as usize..range.end as usize)
                );
            }

            let service = Fs::default()
                .root(root.to_str().unwrap())
                .direct_io(true)
                .build()
                .unwrap();
            let reader = service
                .read(&OperationContext::new(), "data", OpRead::default())
                .unwrap();
            let (_, mut stream) = reader.open(BytesRange::new(size + 1, None)).await.unwrap();
            assert!(stream.read_dyn().await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn direct_io_rejects_append_for_operator_and_raw_service() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data");
        fs::write(&path, "original").unwrap();
        let builder = || Fs::default().root(root.path().to_str().unwrap());
        let op = Operator::new(builder().direct_io(true)).unwrap();
        assert!(!op.info().capability().write_can_append);
        let err = op
            .write_with("data", "append")
            .append(true)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);

        let service = builder().direct_io(true).build().unwrap();
        let options = WriteOptions {
            append: true,
            ..Default::default()
        };
        let (args, _) = OpWrite::from_options(&service.capability(), options).unwrap();
        let result = service.write(&OperationContext::new(), "data", args);
        assert_eq!(result.err().unwrap().kind(), ErrorKind::Unsupported);
        assert_eq!(fs::read(&path).unwrap(), b"original");

        let op = Operator::new(builder()).unwrap();
        assert!(op.info().capability().write_can_append);
        op.write_with("data", " append").append(true).await.unwrap();
        assert_eq!(fs::read(path).unwrap(), b"original append");
    }

    #[tokio::test]
    async fn tmpfs_failed_direct_io_create_new_leaves_no_file() {
        let shm = Path::new("/dev/shm");
        let Ok(dir) = File::open(shm) else { return };
        if fstatfs(dir).unwrap().f_type as u32 != 0x0102_1994 {
            return;
        }
        let dir = tempfile::tempdir_in(shm).unwrap();
        let full_path = dir.path().join("data");
        let op = Operator::new(
            Fs::default()
                .root(dir.path().to_str().unwrap())
                .direct_io(true),
        )
        .unwrap();

        let err = op
            .write_with("data", "content")
            .if_not_exists(true)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        assert!(!full_path.exists(), "failed write left a file behind");
    }

    #[tokio::test]
    async fn tmpfs_rejects_direct_io_without_changing_existing_data() {
        let shm = Path::new("/dev/shm");
        let Ok(dir) = File::open(shm) else { return };
        if fstatfs(dir).unwrap().f_type as u32 != 0x0102_1994 {
            return;
        }
        let dir = tempfile::tempdir_in(shm).unwrap();
        let staging = dir.path().join("staging");
        let path = dir.path().join("data");
        fs::write(&path, "original").unwrap();
        let builder = || {
            Fs::default()
                .root(dir.path().to_str().unwrap())
                .direct_io(true)
        };

        let op = Operator::new(builder()).unwrap();
        assert_eq!(
            op.read("data").await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            op.write("data", "overwrite").await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(fs::read(&path).unwrap(), b"original");

        let op = Operator::new(builder().atomic_write_dir(staging.to_str().unwrap())).unwrap();
        assert_eq!(
            op.write("data", "overwrite").await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(fs::read(path).unwrap(), b"original");
        assert_eq!(fs::read_dir(staging).unwrap().count(), 0);
    }
}
