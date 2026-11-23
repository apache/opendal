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

use std::sync::Arc;
use std::vec::IntoIter;

use opendal_core::raw::*;
use opendal_core::{Capability, ErrorKind, MetadataBuilder, OperationContext, Operator, Result};
use opendal_layer_sanity_check::SanityCheckLayer;

#[derive(Debug)]
struct ListService(Vec<oio::Entry>);

impl Service for ListService {
    type Reader = ();
    type Writer = ();
    type Lister = Entries;
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("test")
    }

    fn capability(&self) -> Capability {
        Capability {
            list: true,
            list_with_versions: true,
            list_with_deleted: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        unreachable!()
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        unreachable!()
    }

    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
        unreachable!()
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        unreachable!()
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        unreachable!()
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        Ok(Entries(self.0.clone().into_iter()))
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        unreachable!()
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        unreachable!()
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        unreachable!()
    }
}

struct Entries(IntoIter<oio::Entry>);

impl oio::List for Entries {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        Ok(self.0.next())
    }
}

fn listing(entries: Vec<oio::Entry>) -> Operator {
    Operator::from_parts(OperationContext::default(), Arc::new(ListService(entries)))
        .layer(SanityCheckLayer)
}

fn versioned_entry(path: &str, version: &str) -> oio::Entry {
    let mut metadata = MetadataBuilder::file(0);
    metadata.version(version);
    oio::Entry::new(path, metadata.build())
}

#[tokio::test]
async fn normal_listing_rejects_duplicate_paths() {
    let op = listing(vec![
        versioned_entry("file", "v1"),
        versioned_entry("other", "v1"),
        versioned_entry("file", "v2"),
    ]);

    let error = op.list("/").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert_eq!(error.message(), "duplicate entry in list response");
}

#[tokio::test]
async fn versioned_listing_allows_distinct_path_version_pairs() -> Result<()> {
    let op = listing(vec![
        versioned_entry("file", "v1"),
        versioned_entry("file", "v2"),
        versioned_entry("other", "v1"),
        oio::Entry::new("dir/", MetadataBuilder::dir().build()),
    ]);

    let entries = op.list_with("/").versions(true).await?;
    let identities: Vec<_> = entries
        .iter()
        .map(|entry| (entry.path(), entry.metadata().version()))
        .collect();
    assert_eq!(
        identities,
        [
            ("file", Some("v1")),
            ("file", Some("v2")),
            ("other", Some("v1")),
            ("dir/", None),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn versioned_listing_rejects_duplicate_path_version_pairs() {
    let op = listing(vec![
        versioned_entry("file", "v1"),
        versioned_entry("file", "v2"),
        versioned_entry("file", "v1"),
    ]);

    let error = op.list_with("/").versions(true).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert_eq!(error.message(), "duplicate entry in list response");
}

#[tokio::test]
async fn listing_deleted_entries_allows_multiple_versions_of_a_path() -> Result<()> {
    let mut deleted = MetadataBuilder::file(0);
    deleted.version("v2").is_deleted(true);
    let op = listing(vec![
        versioned_entry("file", "v1"),
        oio::Entry::new("file", deleted.build()),
    ]);

    let entries = op.list_with("/").deleted(true).await?;
    assert_eq!(entries.len(), 2);
    assert!(entries[1].metadata().is_deleted());
    Ok(())
}

#[tokio::test]
async fn normal_listings_allow_distinct_paths_and_track_them_independently() -> Result<()> {
    let op = Operator::new(opendal_core::services::Memory::default())?.layer(SanityCheckLayer);
    op.write("first", "first").await?;
    op.write("second", "second").await?;

    for _ in 0..2 {
        let entries = op.list("/").await?;
        let mut paths: Vec<_> = entries.iter().map(|entry| entry.path()).collect();
        paths.sort_unstable();
        assert_eq!(paths, ["first", "second"]);
    }
    Ok(())
}
