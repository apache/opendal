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

use crate::utils::{build_trail, new_file_path};
use anyhow::{Result, ensure};
use bytes::Bytes;
use futures::TryStreamExt;
use libtest_mimic::Trial;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use object_store_opendal::OpendalStore;

pub fn tests(store: &OpendalStore, tests: &mut Vec<Trial>) {
    tests.push(build_trail(
        "test_list_with_offset",
        store,
        test_list_with_offset,
    ));
}

async fn test_list_with_offset(store: OpendalStore) -> Result<()> {
    let prefix = Path::from(new_file_path("data"));
    let paths = ["a#b", "a$b", "a%b", "a~b", "a-b", "z"].map(|name| prefix.clone().join(name));
    for path in &paths {
        store.put(path, Bytes::from_static(b"data").into()).await?;
    }

    let result = async {
        let mut full: Vec<_> = store
            .list(Some(&prefix))
            .map_ok(|meta| meta.location)
            .try_collect()
            .await?;
        full.sort_unstable();
        let mut expected_paths = paths.clone();
        expected_paths.sort_unstable();
        ensure!(full == expected_paths, "unexpected listing: {full:?}");

        for name in ["a$b", "a%b", "a~b", "a-b"] {
            let offset = prefix.clone().join(name);
            let expected: Vec<_> = full
                .iter()
                .filter(|path| *path > &offset)
                .cloned()
                .collect();
            let mut actual: Vec<_> = store
                .list_with_offset(Some(&prefix), &offset)
                .map_ok(|meta| meta.location)
                .try_collect()
                .await?;
            actual.sort_unstable();
            ensure!(
                actual == expected,
                "listing after {offset}: expected {expected:?}, got {actual:?}"
            );
        }
        Ok(())
    }
    .await;

    for path in &paths {
        store.delete(path).await?;
    }
    result
}
