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
}
