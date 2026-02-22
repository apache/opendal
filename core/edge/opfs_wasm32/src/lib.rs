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

#[cfg(test)]
mod tests {
    use opendal::ErrorKind;
    use opendal::Operator;
    use opendal::services::OpfsConfig;
    use wasm_bindgen_test::wasm_bindgen_test;
    use wasm_bindgen_test::wasm_bindgen_test_configure;

    macro_rules! console_log {
        ($($arg:tt)*) => {
            web_sys::console::log_1(&format!($($arg)*).into())
        };
    }

    wasm_bindgen_test_configure!(run_in_browser);

    fn new_operator() -> Operator {
        Operator::from_config(OpfsConfig::default())
            .expect("failed to create opfs operator")
            .finish()
    }

    #[wasm_bindgen_test]
    async fn test_get_directory_handle() {
        let op = new_operator();
        op.create_dir("/dir/").await.expect("directory");
        op.create_dir("/dir///").await.expect("directory");
        op.create_dir("/dir:/").await.expect("directory");
        op.create_dir("/dir<>/").await.expect("directory");
        assert_eq!(op.create_dir("/a/b/../x/y/z/").await.unwrap_err().kind(), ErrorKind::Unexpected);
        // this works on Chrome, but fails on macOS
        // assert_eq!(op.create_dir("/dir\0/").await.unwrap_err().kind(), ErrorKind::Unexpected);
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
    async fn test_write_simple() {
        let op = new_operator();
        let content = "Content of the file to write";
        let meta = op.write("/test_file", content).await.expect("write");
        console_log!("{:?}", meta);
        assert_eq!(meta.content_length(), content.len() as u64);

        // This is None - we have to use stat
        assert!(meta.last_modified().is_none());
    }

    #[wasm_bindgen_test]
    async fn test_write_write_twice_same_file() {
        let op = new_operator();
        let content = "Content of the file to write";
        let meta = op.write("/test_file", content).await.expect("write");
        let meta = op.write("/test_file", content).await.expect("write");
        assert_eq!(meta.content_length(), content.len() as u64);
        assert!(meta.last_modified().is_none());
    }

    #[wasm_bindgen_test]
    async fn test_write_like_append_three_times() {
        let op = new_operator();
        let content = "Content of the file to write";
        let mut w = op.writer("/test_file_writte_twice").await.expect("writer");
        w.write(content).await.expect("write");
        w.write(content).await.expect("write");
        w.write(content).await.expect("write");
        let meta = w.close().await.expect("close");
        assert_eq!(meta.content_length(), (content.len() as u64) * 3);
        assert!(meta.last_modified().is_none());
    }

    #[wasm_bindgen_test]
    async fn test_write_large_file_quota() {
        // you can simulate a lower disk space in Chrome
        let op = new_operator();
        let mut w = op.writer("big_file").await.expect("writer");
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
        let meta = w.close().await.expect("close");
        assert_eq!(meta.content_length(), 1024 * 1024 * 1024); // 1GB
    }
}
