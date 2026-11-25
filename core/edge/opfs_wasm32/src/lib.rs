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

#![cfg(target_arch = "wasm32")]
#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use opendal::Buffer;
    use opendal::Builder;
    use opendal::BytesRange;
    use opendal::EntryMode;
    use opendal::ErrorKind;
    use opendal::OperationContext;
    use opendal::Operator;
    use opendal::raw::{
        OpList, OpRead, OpStat, OpWrite, Service,
        oio::{List, Read, ReadStream, Write},
    };
    use opendal::services::Opfs;
    use opendal::services::OpfsConfig;
    use wasm_bindgen_test::wasm_bindgen_test;
    use wasm_bindgen_test::wasm_bindgen_test_configure; // Required for Next()

    macro_rules! console_log {
        ($($arg:tt)*) => {
            web_sys::console::log_1(&format!($($arg)*).into())
        };
    }

    #[cfg(not(feature = "worker"))]
    wasm_bindgen_test_configure!(run_in_browser);
    #[cfg(feature = "worker")]
    wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn new_operator() -> Operator {
        Operator::from_config(OpfsConfig::default()).expect("failed to create opfs operator")
    }

    #[wasm_bindgen_test]
    async fn test_create_directory_handle() {
        let op = new_operator();
        op.create_dir("/dir/").await.expect("directory");
        op.create_dir("/dir///").await.expect("directory");
        op.create_dir("/dir:/").await.expect("directory");
        op.create_dir("/dir<>/").await.expect("directory");
        assert_eq!(
            op.create_dir("/a/b/../x/y/z/").await.unwrap_err().kind(),
            ErrorKind::Unexpected
        );
        // this works on Chrome, but fails on macOS
        // assert_eq!(op.create_dir("/dir\0/").await.unwrap_err().kind(), ErrorKind::Unexpected);
    }

    #[wasm_bindgen_test]
    async fn test_create_directory_handle_with_root_and_list() {
        {
            // create files and dirs
            let op_rooted =
                Operator::new(Opfs::default().root("/myapp/subdir1/subdir2/")).expect("config");
            op_rooted
                .write("subdir3/somefile", "content")
                .await
                .expect("write under root");

            let stat_rooted = op_rooted.stat("subdir3/somefile").await.expect("stat");

            let op = new_operator();
            let stat = op
                .stat("/myapp/subdir1/subdir2/subdir3/somefile")
                .await
                .expect("stat");
            assert_eq!(stat_rooted, stat);
            let stat = op
                .stat("myapp/subdir1/subdir2/subdir3/somefile")
                .await
                .expect("stat");
            assert_eq!(stat_rooted, stat);
        }

        {
            // simple list
            let op = new_operator();
            let mut entries = op.lister("").await.expect("list");
            while let Some(entry) = entries.try_next().await.expect("next") {
                console_log!("entry: {} {:?}", entry.path(), entry.metadata().mode());
            }
        }

        // list test added here so we are sure there are dirs and files to list
        {
            // simple list
            let op = new_operator();
            let mut entries = op.lister("myapp/").await.expect("list");
            while let Some(entry) = entries.try_next().await.expect("next") {
                console_log!("entry: {} {:?}", entry.path(), entry.metadata().mode());
            }
        }

        {
            // recursive list
            let op = new_operator();
            let mut entries = op
                .lister_with("")
                .recursive(true)
                .await
                .expect("recursive list");
            while let Some(entry) = entries.try_next().await.expect("next") {
                console_log!("rec entry: {} {:?}", entry.path(), entry.metadata().mode());
            }
        }
    }

    #[wasm_bindgen_test]
    async fn test_write() {
        let op = new_operator();

        // this does not even go to OPFS backend, short-circuited
        assert_eq!(
            op.write("/", "should_not_work").await.unwrap_err().kind(),
            ErrorKind::IsADirectory
        );
    }

    #[wasm_bindgen_test]
    async fn test_write_read_simple() {
        let path = "/test_file";
        let content = "Content of the file to write";
        {
            let op = new_operator();
            let meta = op.write(path, content).await.expect("write");
            console_log!("{:?}", meta);
            assert_eq!(meta.content_length(), content.len() as u64);

            // This is None - we have to use stat
            assert!(meta.last_modified().is_none());

            let stat = op.stat(path).await.expect("stat");
            console_log!("stat = {:?}", stat);
            assert_eq!(stat.mode(), EntryMode::FILE);
            assert_eq!(stat.content_length(), content.len() as u64);
            assert!(stat.last_modified().is_some());
        }

        {
            // read back and compare
            let op = new_operator();
            let buffer = op.read(path).await.expect("read");
            console_log!("read = {:?}", buffer);
            assert_eq!(buffer.to_bytes(), content.as_bytes());
            op.delete(path).await.expect("delete");
        }
    }

    #[wasm_bindgen_test]
    async fn test_write_write_twice_same_file() {
        let op = new_operator();
        let content = "Content of the file to write";
        let path = "/test_file";
        op.write(path, content).await.expect("write");
        let meta = op.write(path, content).await.expect("write");
        assert_eq!(meta.content_length(), content.len() as u64);
        assert!(meta.last_modified().is_none());
        op.delete(path).await.expect("delete");
    }

    #[wasm_bindgen_test]
    async fn test_write_like_append_three_times() {
        let op = new_operator();
        let content = "Content of the file to write";
        let path = "/test_file_write_multiple";
        let mut w = op.writer(path).await.expect("writer");
        w.write(content).await.expect("write");
        w.write(content).await.expect("write");
        w.write(content).await.expect("write");
        let meta = w.close().await.expect("close");
        let expected_file_size = (content.len() as u64) * 3;
        assert_eq!(meta.content_length(), expected_file_size);
        assert!(meta.last_modified().is_none());
        let stat = op.stat(path).await.expect("stat");
        assert_eq!(stat.content_length(), expected_file_size);
        op.delete(path).await.expect("delete");
    }

    #[wasm_bindgen_test]
    async fn test_write_large_file_quota() {
        // you can simulate a lower disk space in Chrome
        let op = new_operator();
        let path = "big_file";
        let mut w = op.writer(path).await.expect("writer");
        let chunk = vec![0u8; 1024 * 1024]; // 1MB
        for _ in 0..1024 {
            let res = w.write(chunk.clone()).await;
            match res {
                Ok(()) => (),
                Err(e) => {
                    // OPFS filled up (you can simulate this in Chrome by setting a lower limit)
                    // parse_js_error: JsValue(TypeError: Cannot close a ERRORED writable stream
                    // TypeError: Cannot close a ERRORED writable stream
                    console_log!("got {e:?}");
                    console_log!("message = {}", e.message());
                    assert_eq!(e.kind(), ErrorKind::Unexpected);
                    return;
                }
            }
        }

        let expected_file_size = 1024 * 1024 * 1024; // 1GB
        let meta = w.close().await.expect("close");
        assert_eq!(meta.content_length(), expected_file_size);
        let stat = op.stat(path).await.expect("stat");
        assert_eq!(stat.content_length(), expected_file_size);

        {
            // read and compare
            let op = new_operator();
            let buffer = op.read(path).await.expect("read");
            assert_eq!(buffer.to_bytes().len(), expected_file_size as usize);
        }
        op.delete(path).await.expect("delete");
    }

    #[wasm_bindgen_test]
    async fn test_delete_missing_parent() {
        let op = new_operator();
        op.create_dir("delete-missing-parent/")
            .await
            .expect("create ancestor");
        op.delete("delete-missing-parent/missing/deeper/file")
            .await
            .expect("delete below missing parent");
    }

    #[wasm_bindgen_test]
    async fn test_delete_recursive_directory() {
        let op = Operator::new(Opfs::default().root("/delete-recursive-directory/"))
            .expect("create rooted operator");
        op.write("dir-sibling/file", "keep")
            .await
            .expect("write sibling file");

        for path in ["dir", "dir/"] {
            op.write("dir/nested/file", "delete")
                .await
                .expect("write nested file");
            assert!(op.delete(path).await.is_err());
            assert!(
                op.exists("dir/nested/file")
                    .await
                    .expect("check nested file")
            );

            op.delete_with(path)
                .recursive(true)
                .await
                .expect("delete non-empty directory");
            assert!(!op.exists("dir/").await.expect("check deleted directory"));
            assert!(op.exists("dir-sibling/file").await.expect("check sibling"));
        }
    }

    #[wasm_bindgen_test]
    async fn test_delete_recursive_file_and_missing_paths() {
        let op = new_operator();
        let path = "delete-recursive-file/file";
        op.write(path, "delete").await.expect("write file");
        op.delete_with(path)
            .recursive(true)
            .await
            .expect("delete file");
        assert!(!op.exists(path).await.expect("check deleted file"));

        for path in [
            path,
            "delete-recursive-file/missing/",
            "delete-recursive-file/missing/file",
        ] {
            op.delete_with(path)
                .recursive(true)
                .await
                .expect("delete missing path");
        }
    }

    #[wasm_bindgen_test]
    async fn test_delete_recursive_origin_root() {
        let op = new_operator();
        op.write("delete-root/nested/file", "delete")
            .await
            .expect("write nested file");
        op.write("delete-root-file", "delete")
            .await
            .expect("write top-level file");

        op.delete_with("/")
            .recursive(true)
            .await
            .expect("delete origin root contents");
        assert!(op.list("/").await.expect("list origin root").is_empty());

        op.delete_with("/")
            .recursive(true)
            .await
            .expect("delete empty origin root contents");
    }

    #[wasm_bindgen_test]
    async fn test_write_and_read_with_range() {
        let op = new_operator();
        let path = "numbers.txt";
        let content = "0123456789";
        op.write(path, content).await.expect("write");
        let buffer = op.read_with(path).range(3..5).await.expect("read");
        assert_eq!(buffer.to_bytes(), "34".as_bytes());
        op.delete(path).await.expect("delete");
    }

    #[wasm_bindgen_test]
    async fn test_writer_double_close_and_abort_lifecycle() {
        let op = new_operator();
        let srv = Opfs::default().build().expect("build opfs service");
        let ctx = OperationContext::default();
        let path = "writer-lifecycle.txt";
        let content = "keep-this-content";

        // Test OPFS-level double-close directly below CompleteLayer.
        let mut raw_w = srv
            .write(&ctx, path, OpWrite::default())
            .expect("raw writer");
        raw_w.write(Buffer::from(content)).await.expect("raw write");
        let meta = raw_w.close().await.expect("first raw close");
        assert_eq!(meta.content_length(), content.len() as u64);

        let err = raw_w.close().await.expect_err("second raw close must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let err = raw_w
            .write(Buffer::from("extra"))
            .await
            .expect_err("raw write after close must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let err = raw_w
            .abort()
            .await
            .expect_err("raw abort after close must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let buffer = op.read(path).await.expect("read back after double close");
        assert_eq!(buffer.to_bytes(), content.as_bytes());

        // Test OPFS-level double-abort directly below CompleteLayer.
        let mut raw_w = srv
            .write(&ctx, path, OpWrite::default())
            .expect("second raw writer");
        raw_w
            .write(Buffer::from("discard"))
            .await
            .expect("staged raw write");
        raw_w.abort().await.expect("first raw abort");
        raw_w
            .abort()
            .await
            .expect("second raw abort must be idempotent");
        let err = raw_w
            .close()
            .await
            .expect_err("raw close after abort must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let buffer = op.read(path).await.expect("read back after raw abort");
        assert_eq!(buffer.to_bytes(), content.as_bytes());

        // Also verify Operator-level close and abort preserve file contents.
        let mut w = op.writer(path).await.expect("operator writer");
        w.write("operator-discard")
            .await
            .expect("operator staged write");
        w.abort().await.expect("operator abort");

        let buffer = op.read(path).await.expect("read back after operator abort");
        assert_eq!(buffer.to_bytes(), content.as_bytes());

        let op_content = "operator-committed";
        let mut w = op.writer(path).await.expect("operator commit writer");
        w.write(op_content).await.expect("operator write");
        let meta = w.close().await.expect("operator close");
        assert_eq!(meta.content_length(), op_content.len() as u64);

        let buffer = op.read(path).await.expect("read back after operator close");
        assert_eq!(buffer.to_bytes(), op_content.as_bytes());

        op.delete(path).await.expect("delete");
    }

    #[wasm_bindgen_test]
    async fn test_writer_without_writes() {
        let op = new_operator();
        let srv = Opfs::default().build().expect("build opfs service");
        let ctx = OperationContext::default();
        let path = "writer-without-writes.txt";

        let mut writer = srv
            .write(&ctx, path, OpWrite::default())
            .expect("raw writer");
        writer.abort().await.expect("abort unused writer");
        writer.abort().await.expect("abort unused writer again");
        let err = writer.close().await.expect_err("close after abort");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!op.exists(path).await.expect("check aborted file"));

        op.write(path, "replace with empty").await.expect("write");
        let mut writer = srv
            .write(&ctx, path, OpWrite::default())
            .expect("raw writer");
        let meta = writer.close().await.expect("close unused writer");
        assert_eq!(meta.content_length(), 0);
        assert!(op.read(path).await.expect("read empty file").is_empty());
        op.delete(path).await.expect("delete");
    }

    #[wasm_bindgen_test]
    async fn test_stat_and_non_recursive_delete_origin_root() {
        let srv = Opfs::default().build().expect("build opfs service");
        let ctx = OperationContext::default();
        let rp = srv
            .stat(&ctx, "/", OpStat::default())
            .await
            .expect("direct Service::stat on origin root must succeed");
        assert_eq!(rp.into_metadata().mode(), EntryMode::DIR);

        let op = new_operator();
        op.write("origin-root-keep", "keep")
            .await
            .expect("write file under origin root");

        let err = op
            .delete("/")
            .await
            .expect_err("non-recursive delete on non-empty origin root must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(op.exists("origin-root-keep").await.expect("file survives"));

        op.delete_with("/")
            .recursive(true)
            .await
            .expect("clear origin root");
        op.delete("/")
            .await
            .expect("non-recursive delete on empty origin root must succeed");
    }

    #[wasm_bindgen_test]
    async fn test_list_missing_dir_and_non_trailing_slash() {
        let op = new_operator();
        let srv = Opfs::default().build().expect("build opfs service");
        let ctx = OperationContext::default();

        for path in ["list-prefix-dir/", "list-prefix-dir/nested/"] {
            assert!(op.list(path).await.expect("list missing dir").is_empty());
        }
        let mut missing = srv
            .list(&ctx, "list-prefix-dir/", OpList::default())
            .expect("raw lister for missing dir");
        assert!(missing.next().await.expect("next on missing dir").is_none());

        op.write("list-prefix-dir/file.txt", "hello")
            .await
            .expect("write file");
        op.create_dir("list-prefix-dir/child/")
            .await
            .expect("create child dir");
        assert!(
            missing
                .next()
                .await
                .expect("next after completion")
                .is_none()
        );

        let mut lister = srv
            .list(&ctx, "list-prefix-dir", OpList::default())
            .expect("list without trailing slash");
        let mut paths = Vec::new();
        while let Some(entry) = lister.next().await.expect("next entry") {
            paths.push(entry.path().to_string());
        }
        paths.sort();
        assert_eq!(
            paths,
            vec!["list-prefix-dir/child/", "list-prefix-dir/file.txt"]
        );

        op.delete_with("list-prefix-dir/")
            .recursive(true)
            .await
            .expect("cleanup");
    }

    #[wasm_bindgen_test]
    async fn test_read_with_out_of_bounds_and_edge_ranges() {
        let op = new_operator();
        let path = "numbers-range-bounds.txt";
        op.write(path, "0123456789").await.expect("write");

        let err = op
            .read_with(path)
            .range(100..105)
            .await
            .expect_err("out-of-bounds range must fail");
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);

        // Also exercise OPFS range validation directly below CompleteLayer.
        let srv = Opfs::default().build().expect("build opfs service");
        let ctx = OperationContext::default();
        let reader = srv
            .read(&ctx, path, OpRead::default())
            .expect("raw read handle");
        let err = reader
            .read(BytesRange::new(100, Some(5)))
            .await
            .expect_err("raw out-of-bounds range must fail");
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);

        let err = op
            .read_with(path)
            .range(10..11)
            .await
            .expect_err("range at EOF must fail");
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);

        let err = op
            .read_with(path)
            .range(8..20)
            .await
            .expect_err("strict bounded read must fail on a short result");
        assert!(matches!(
            err.kind(),
            ErrorKind::Unexpected | ErrorKind::RangeNotSatisfied
        ));

        let (_, mut stream) = reader
            .open(BytesRange::new(8, Some(12)))
            .await
            .expect("open raw stream crossing EOF");
        let tail = stream.read_all().await.expect("read available bytes");
        assert_eq!(tail.to_bytes(), b"89".as_slice());

        let tail = op
            .read_with(path)
            .range(8..)
            .limit(12)
            .await
            .expect("limited read may end at EOF");
        assert_eq!(tail.to_bytes(), b"89".as_slice());

        let suffix = BytesRange::suffix(3);
        let (_, tail) = reader.read(suffix).await.expect("raw suffix read");
        assert_eq!(tail.to_bytes(), b"789".as_slice());
        let tail = op.read_with(path).range(suffix).await.expect("suffix read");
        assert_eq!(tail.to_bytes(), b"789".as_slice());

        let empty = op
            .read_with(path)
            .range(10..)
            .await
            .expect("open-ended range at EOF");
        assert!(empty.is_empty());

        op.delete(path).await.expect("delete");
    }
}
