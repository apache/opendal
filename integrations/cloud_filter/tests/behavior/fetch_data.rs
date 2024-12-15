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

use libtest_mimic::Failed;

use crate::{
    utils::{file_content, file_length},
    ROOT_PATH,
};

pub fn test_fetch_data() -> Result<(), Failed> {
    let files = [
        (
            "normal_file.txt",
            include_str!("..\\..\\..\\..\\fixtures/data/normal_file.txt"),
        ),
        (
            "special_file  !@#$%^&()_+-=;',.txt",
            include_str!("..\\..\\..\\..\\fixtures/data/special_file  !@#$%^&()_+-=;',.txt"),
        ),
    ];
    for (file, expected_content) in files {
        let path = format!("{ROOT_PATH}\\{file}");
        assert_eq!(
            expected_content.len(),
            file_length(&path).expect("file length"),
            "file length",
        );

        assert_eq!(
            expected_content,
            file_content(&path).expect("file content"),
            "file content",
        )
    }
    Ok(())
}
