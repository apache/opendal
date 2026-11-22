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
use object_store::{ListResult, ObjectStore, ObjectStoreExt};
use object_store_opendal::OpendalStore;

pub fn tests(store: &OpendalStore, tests: &mut Vec<Trial>) {
    tests.push(build_trail(
        "test_list_with_offset",
        store,
        test_list_with_offset,
    ));
    tests.push(build_trail(
        "test_list_with_delimiter",
        store,
        test_list_with_delimiter,
    ));
    tests.push(build_trail(
        "test_list_with_delimiter_special_chars",
        store,
        test_list_with_delimiter_special_chars,
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

/// Compare `objects` and `common_prefixes` exactly, ignoring order. Exact
/// equality also catches duplicate prefixes and a prefix reporting itself.
fn ensure_list_result(
    result: &ListResult,
    expected_objects: &[Path],
    expected_prefixes: &[Path],
) -> Result<()> {
    let mut objects: Vec<_> = result
        .objects
        .iter()
        .map(|meta| meta.location.clone())
        .collect();
    objects.sort_unstable();
    let mut prefixes = result.common_prefixes.clone();
    prefixes.sort_unstable();
    let mut expected_objects = expected_objects.to_vec();
    expected_objects.sort_unstable();
    let mut expected_prefixes = expected_prefixes.to_vec();
    expected_prefixes.sort_unstable();

    ensure!(
        objects == expected_objects,
        "list_with_delimiter objects: expected {expected_objects:?}, got {objects:?}"
    );
    ensure!(
        prefixes == expected_prefixes,
        "list_with_delimiter common_prefixes: expected {expected_prefixes:?}, got {prefixes:?}"
    );
    Ok(())
}

async fn put_objects(store: &OpendalStore, paths: &[Path]) -> Result<()> {
    for path in paths {
        store.put(path, Bytes::from_static(b"data").into()).await?;
    }
    Ok(())
}

async fn test_list_with_delimiter(store: OpendalStore) -> Result<()> {
    // A unique single-segment root keeps this test isolated from other tests
    // and runs sharing the same operator root.
    let base = Path::from(format!("delimiter-{}", uuid::Uuid::new_v4()));
    let a = base.clone().join("a");
    let ab = base.clone().join("ab");
    let paths = [
        a.clone().join("file1"),
        a.clone().join("b").join("file2"),
        a.clone().join("b").join("c").join("file3"),
        a.clone().join("d").join("file4"),
        ab.clone().join("file5"),
        base.clone().join("top.txt"),
    ];
    put_objects(&store, &paths).await?;

    let result = async {
        // A non-recursive listing returns only direct objects plus each
        // immediate subdirectory prefix exactly once. The queried prefix
        // itself must not appear in either list.
        let result = store.list_with_delimiter(Some(&a)).await?;
        ensure_list_result(
            &result,
            &[a.clone().join("file1")],
            &[a.clone().join("b"), a.clone().join("d")],
        )?;

        let result = store
            .list_with_delimiter(Some(&a.clone().join("b")))
            .await?;
        ensure_list_result(
            &result,
            &[a.clone().join("b").join("file2")],
            &[a.clone().join("b").join("c")],
        )?;

        let result = store
            .list_with_delimiter(Some(&a.clone().join("b").join("c")))
            .await?;
        ensure_list_result(&result, &[a.clone().join("b").join("c").join("file3")], &[])?;

        // The sibling prefix `ab/` must not leak into `a/`'s listing, and
        // `ab/` only contains a direct object.
        let result = store.list_with_delimiter(Some(&ab)).await?;
        ensure_list_result(&result, &[ab.clone().join("file5")], &[])?;

        // The base contains both a direct object and two subdirectories.
        let result = store.list_with_delimiter(Some(&base)).await?;
        ensure_list_result(
            &result,
            &[base.clone().join("top.txt")],
            &[a.clone(), ab.clone()],
        )?;

        // A prefix without objects yields empty results instead of an error.
        let result = store
            .list_with_delimiter(Some(&base.clone().join("nonexistent")))
            .await?;
        ensure_list_result(&result, &[], &[])?;

        // Listing the root reports the base prefix exactly once as a
        // one-segment common prefix and never reports objects below it.
        // Other tests may own further prefixes under the shared root, so
        // only membership is asserted here.
        let result = store.list_with_delimiter(None).await?;
        ensure!(
            result
                .common_prefixes
                .iter()
                .filter(|prefix| *prefix == &base)
                .count()
                == 1,
            "expected {base} once in common_prefixes: {:?}",
            result.common_prefixes
        );
        for prefix in &result.common_prefixes {
            ensure!(
                !prefix.as_ref().contains('/'),
                "common prefix {prefix} is not an immediate child of the root"
            );
        }
        ensure!(
            !result
                .objects
                .iter()
                .any(|meta| meta.location.prefix_matches(&base)),
            "root listing reported objects below {base}: {:?}",
            result.objects
        );
        Ok(())
    }
    .await;

    for path in &paths {
        store.delete(path).await?;
    }
    result
}

async fn test_list_with_delimiter_special_chars(store: OpendalStore) -> Result<()> {
    let base = Path::from(format!("delimiter-{}", uuid::Uuid::new_v4()));
    // `Path::join` percent-encodes segment characters that object_store
    // treats as unsafe, while OpenDAL stores the decoded name.
    let dir_names = [
        "dir with space",
        "dir#hash",
        "dir%percent",
        "dir+plus",
        "dir~tilde",
        "dir-中文",
    ];
    let mut paths: Vec<Path> = dir_names
        .iter()
        .map(|name| base.clone().join(*name).join("nested file.txt"))
        .collect();
    let direct_file = base.clone().join("file name#1.txt");
    paths.push(direct_file.clone());
    put_objects(&store, &paths).await?;

    let result = async {
        let result = store.list_with_delimiter(Some(&base)).await?;
        let expected_prefixes: Vec<Path> = dir_names
            .iter()
            .map(|name| base.clone().join(*name))
            .collect();
        ensure_list_result(
            &result,
            std::slice::from_ref(&direct_file),
            &expected_prefixes,
        )?;

        // Listing inside a special-char directory resolves the decoded
        // OpenDAL path and returns its objects.
        let nested = base.clone().join("dir%percent");
        let result = store.list_with_delimiter(Some(&nested)).await?;
        ensure_list_result(&result, &[nested.clone().join("nested file.txt")], &[])?;

        let nested = base.clone().join("dir with space");
        let result = store.list_with_delimiter(Some(&nested)).await?;
        ensure_list_result(&result, &[nested.clone().join("nested file.txt")], &[])?;
        Ok(())
    }
    .await;

    for path in &paths {
        store.delete(path).await?;
    }
    result
}
