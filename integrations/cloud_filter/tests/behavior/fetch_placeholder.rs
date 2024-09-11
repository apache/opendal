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

use crate::{utils::list, ROOT_PATH};

pub fn test_fetch_placeholder() -> Result<(), Failed> {
    let files = ["normal_file.txt", "special_file  !@#$%^&()_+-=;',.txt"];
    let dirs = ["normal_dir", "special_dir  !@#$%^&()_+-=;',"];

    assert_eq!(
        list(ROOT_PATH, "File").expect("list files"),
        files,
        "list files"
    );
    assert_eq!(
        list(ROOT_PATH, "Directory").expect("list dirs"),
        dirs,
        "list dirs"
    );

    Ok(())
}
