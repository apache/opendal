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
use anyhow::Result;
use bytes::Bytes;
use libtest_mimic::Trial;
use object_store::ObjectStore;
use object_store_opendal::OpendalStore;

pub fn tests(store: &OpendalStore, tests: &mut Vec<Trial>) {
    tests.push(build_trail("test_basic_get", store, test_basic_get));
    tests.push(build_trail("test_head", store, test_head));
    tests.push(build_trail("test_get_range", store, test_get_range));
}

pub async fn test_basic_get(store: OpendalStore) -> Result<()> {
    let location = new_file_path("data").into();
    let value = Bytes::from_static(b"Hello, world!");

    store.put(&location, value.clone().into()).await?;

    let ret = store.get(&location).await?;

    assert_eq!(0..value.len(), ret.range);
    let data = ret.bytes().await?;
    assert_eq!(value, data);

    let ret = store.get(&"not_exist".into()).await;
    assert!(ret.is_err());
    assert!(matches!(
        ret.err().unwrap(),
        object_store::Error::NotFound { .. }
    ));

    store.delete(&location).await?;
    Ok(())
}

pub async fn test_head(store: OpendalStore) -> Result<()> {
    let location = new_file_path("data").into();
    let value = Bytes::from_static(b"Hello, world!");

    store.put(&location, value.clone().into()).await?;

    let meta = store.head(&location).await?;

    assert_eq!(meta.size, value.len());
    assert_eq!(meta.location, location);

    store.delete(&location).await?;
    Ok(())
}

pub async fn test_get_range(store: OpendalStore) -> Result<()> {
    let location = new_file_path("data").into();
    let value = Bytes::from_static(b"Hello, world!");

    store.put(&location, value.clone().into()).await?;

    let ret = store.get_range(&location, 0..5).await?;
    assert_eq!(Bytes::from_static(b"Hello"), ret);

    store.delete(&location).await?;
    Ok(())
}
