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

use std::collections::VecDeque;
use std::sync::Mutex;

use super::*;
use crate::services::Memory;

#[tokio::test]
async fn default_glob_can_be_disabled_and_reenabled() -> Result<()> {
    let op = Operator::new_without_glob_simulation(Memory::default())?;
    assert!(!op.info().capability().list_with_glob);
    op.write("a.jpg", "jpg").await?;
    op.write("b.png", "png").await?;
    let err = op.list_with("/").glob("*.jpg").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.message().contains("glob"));
    assert_eq!(op.list("/").await?.len(), 2);

    let op = op.layer(GlobLayer);
    assert!(op.info().capability().list_with_glob);
    let entries = op.list_with("/").glob("*.jpg").await?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path(), "a.jpg");
    Ok(())
}

#[tokio::test]
async fn guided_listing_with_overlapping_recursive_segments() -> Result<()> {
    let op = Operator::new(Memory::default())?;
    for path in ["a/b/c", "a/b/b/c", "a/b/x/c", "a/x/b/c", "a/c", "b/b/c"] {
        op.write(path, "data").await?;
    }
    let entries = op.list_with("/").glob("a/**/**/b/**/c").await?;
    let mut paths: Vec<_> = entries.iter().map(Entry::path).collect();
    paths.sort_unstable();
    assert_eq!(paths, ["a/b/b/c", "a/b/c", "a/b/x/c", "a/x/b/c"]);
    Ok(())
}

#[tokio::test]
async fn literal_patterns_preserve_list_prefix_semantics() -> Result<()> {
    let op = Operator::new(Memory::default())?;
    op.write("a/b/é.jpg", "data").await?;
    assert_eq!(
        op.list_with("/").glob("a/b/é.jpg").await?[0].path(),
        "a/b/é.jpg"
    );
    assert!(op.list_with("/").glob("a/b/missing.jpg").await?.is_empty());
    assert_eq!(
        op.list_with("a/b/é").glob("?.jpg").await?[0].path(),
        "a/b/é.jpg"
    );
    assert_eq!(op.list_with("a/").glob("b/").await?[0].path(), "a/b/");
    assert!(op.list_with("/").glob("a/b/é.jpg/").await?.is_empty());
    let entries = op.list_with("/").glob("a/b/**").await?;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["a/b/", "a/b/é.jpg"]
    );
    for pattern in [String::new(), "*".repeat(usize::from(u16::MAX))] {
        let error = op.list_with("/").glob(&pattern).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
    }
    Ok(())
}

#[tokio::test]
async fn guided_listing_skips_literal_parents_and_keeps_page_size() -> Result<()> {
    let inner = Arc::new(ListFixture::new(&[
        "a/b/猫.jpg",
        "a/b/bad.png",
        "a/b/é.jpg",
    ]));
    let op = Operator::from_parts(OperationContext::default(), inner.clone()).layer(GlobLayer);
    let entries = op.list_with("/").glob("a/b/?.jpg").limit(1).await?;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["a/b/猫.jpg", "a/b/é.jpg"]
    );
    let requests = inner.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let (path, args) = &requests[0];
    assert_eq!(path, "a/b/");
    assert_eq!(args.limit(), Some(1));
    assert_eq!(args.glob(), None);
    Ok(())
}

#[tokio::test]
async fn guided_listing_skips_unreadable_discovered_directories() -> Result<()> {
    let inner = Arc::new(ListFixture {
        entries: vec![
            oio::Entry::new("locked/", MetadataBuilder::dir().build()),
            oio::Entry::new("open/", MetadataBuilder::dir().build()),
            oio::Entry::new("open/a.jpg", MetadataBuilder::file(0).build()),
        ],
        denied: Some("locked/"),
        ..ListFixture::new(&[])
    });
    let op = Operator::from_parts(OperationContext::default(), inner.clone()).layer(GlobLayer);
    let entries = op.list_with("/").glob("*/*.jpg").await?;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["open/a.jpg"]
    );
    // The directory the caller asked for still reports the error.
    let err = op.list_with("locked/").glob("*.jpg").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    Ok(())
}

#[tokio::test]
async fn filtering_preserves_versions_and_delete_markers_at_the_same_path() -> Result<()> {
    let entries = [
        ("a/b.jpg", "new", false),
        ("a/b.jpg", "old", false),
        ("a/b.jpg", "deleted", true),
        ("a/c.png", "other", false),
    ]
    .map(|(path, version, deleted)| {
        let mut metadata = MetadataBuilder::file(0);
        metadata.version(version).is_deleted(deleted);
        oio::Entry::new(path, metadata.build())
    });
    let inner = Arc::new(ListFixture {
        entries: entries.to_vec(),
        ..ListFixture::new(&[])
    });
    let op = Operator::from_parts(OperationContext::default(), inner.clone()).layer(GlobLayer);
    let got = op
        .list_with("/")
        .glob("a/**/*.jpg")
        .versions(true)
        .deleted(true)
        .await?;
    assert_eq!(
        got.iter()
            .map(|entry| (
                entry.path(),
                entry.metadata().version(),
                entry.metadata().is_deleted()
            ))
            .collect::<Vec<_>>(),
        [
            ("a/b.jpg", Some("new"), false),
            ("a/b.jpg", Some("old"), false),
            ("a/b.jpg", Some("deleted"), true),
        ]
    );
    let requests = inner.requests.lock().unwrap();
    let (_, args) = &requests[0];
    assert!(args.recursive() && args.versions() && args.deleted());
    assert_eq!(args.glob(), None);
    Ok(())
}

#[tokio::test]
async fn native_glob_receives_the_original_list_options() -> Result<()> {
    let inner = Arc::new(ListFixture {
        native_glob: true,
        ..ListFixture::new(&[])
    });
    let op = Operator::from_parts(OperationContext::default(), inner.clone())
        .layer(crate::layers::SimulateLayer::default())
        .layer(GlobLayer);
    op.list_with("prefix")
        .glob("**/*.jpg")
        .recursive(true)
        .limit(1)
        .start_after("prefix.jpg")
        .await?;
    let requests = inner.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let (path, args) = &requests[0];
    assert_eq!(path, "prefix");
    assert_eq!(args.glob(), Some("**/*.jpg"));
    assert!(args.recursive());
    assert_eq!(args.limit(), Some(1));
    assert_eq!(args.start_after(), Some("prefix.jpg"));
    Ok(())
}

#[tokio::test]
async fn recursive_services_filter_one_listing() -> Result<()> {
    let inner = Arc::new(ListFixture {
        recursive: true,
        ..ListFixture::new(&[
            "t/p=1/a.parquet",
            "t/p=1/b.json",
            "t/p=2/x/c.parquet",
            "t/q=3/d.parquet",
        ])
    });
    let op = Operator::from_parts(OperationContext::default(), inner.clone()).layer(GlobLayer);

    let entries = op.list_with("/").glob("t/*/**/*.parquet").limit(2).await?;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["t/p=1/a.parquet", "t/p=2/x/c.parquet", "t/q=3/d.parquet"]
    );
    let entries = op.list_with("/").glob("*/**/").await?;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["t/", "t/p=1/", "t/p=2/", "t/p=2/x/", "t/q=3/"]
    );
    let entries = op.list_with("t/").glob("**").await?;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        [
            "t/p=1/",
            "t/p=1/a.parquet",
            "t/p=1/b.json",
            "t/p=2/",
            "t/p=2/x/",
            "t/p=2/x/c.parquet",
            "t/q=3/",
            "t/q=3/d.parquet",
        ]
    );

    let requests = inner.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for ((path, args), expected) in requests.iter().zip(["t/", "/", "t/"]) {
        assert_eq!(path, expected);
        assert!(args.recursive());
        assert_eq!(args.glob(), None);
    }
    assert_eq!(requests[0].1.limit(), Some(2));
    Ok(())
}

#[tokio::test]
async fn recursive_listing_returns_each_directory_once() -> Result<()> {
    let inner = Arc::new(ListFixture {
        recursive: true,
        entries: vec![
            oio::Entry::new("t/a/x", MetadataBuilder::file(0).build()),
            oio::Entry::new("t/a/", MetadataBuilder::dir().build()),
            oio::Entry::new("t/", MetadataBuilder::dir().build()),
        ],
        ..ListFixture::new(&[])
    });
    let op = Operator::from_parts(OperationContext::default(), inner).layer(GlobLayer);
    let entries = op.list_with("/").glob("**/").await?;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["t/", "t/a/"]
    );
    Ok(())
}

#[tokio::test]
async fn services_without_recursive_listing_keep_the_walk() -> Result<()> {
    let inner = Arc::new(ListFixture {
        recursive: true,
        entries: vec![
            oio::Entry::new("t/", MetadataBuilder::dir().build()),
            oio::Entry::new("t/a.parquet", MetadataBuilder::file(0).build()),
        ],
        ..ListFixture::new(&[])
    });
    let no_recursion = crate::layers::CapabilityOverrideLayer::new(|mut capability| {
        capability.list_with_recursive = false;
        capability
    });
    let op = Operator::from_parts(OperationContext::default(), inner.clone())
        .layer(no_recursion)
        .layer(GlobLayer);
    op.list_with("/").glob("t/**/*.parquet").await?;
    op.list_with("/").glob("t/**").await?;
    let requests = inner.requests.lock().unwrap();
    assert!(requests.iter().all(|(_, args)| !args.recursive()));
    Ok(())
}

#[tokio::test]
async fn walk_filters_one_recursive_listing_below_a_matched_directory() -> Result<()> {
    let inner = Arc::new(ListFixture {
        recursive: true,
        entries: vec![
            oio::Entry::new("logs/", MetadataBuilder::dir().build()),
            oio::Entry::new("logs/a/x.json", MetadataBuilder::file(0).build()),
            oio::Entry::new("t/", MetadataBuilder::dir().build()),
        ],
        ..ListFixture::new(&[])
    });
    let op = Operator::from_parts(OperationContext::default(), inner.clone()).layer(GlobLayer);
    let entries = op.list_with("/").glob("logs/**").await?;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["logs/", "logs/a/", "logs/a/x.json"]
    );
    let requests = inner.requests.lock().unwrap();
    let requests: Vec<_> = requests
        .iter()
        .map(|(path, args)| (path.as_str(), args.recursive()))
        .collect();
    assert_eq!(requests, [("/", false), ("logs/", true)]);
    Ok(())
}

#[tokio::test]
async fn recursive_services_walk_patterns_that_skip_directories() -> Result<()> {
    let inner = Arc::new(ListFixture {
        recursive: true,
        ..ListFixture::new(&["t/a.parquet", "t/b.json"])
    });
    let op = Operator::from_parts(OperationContext::default(), inner.clone()).layer(GlobLayer);
    let entries = op.list_with("t/").glob("*.parquet").await?;
    assert_eq!(entries[0].path(), "t/a.parquet");
    for pattern in ["t/*", "t/*/*.parquet", "t/p=*/**", "t/*/x/**"] {
        op.list_with("/").glob(pattern).await?;
    }
    let requests = inner.requests.lock().unwrap();
    assert_eq!(requests.len(), 5);
    assert!(
        requests
            .iter()
            .all(|(path, args)| path == "t/" && !args.recursive())
    );
    Ok(())
}

#[tokio::test]
async fn recursive_listing_keeps_the_entry_named_by_a_prefix() -> Result<()> {
    let op = Operator::new(Memory::default())?;
    for path in ["t/a", "t/ab/c", "v/a/b"] {
        op.write(path, "data").await?;
    }
    let entries = op.list_with("t/a").glob("**").await?;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["t/a", "t/ab/", "t/ab/c"]
    );
    let entries = op.list_with("v/a/b").glob("**").await?;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["v/a/b"]
    );
    Ok(())
}

#[tokio::test]
async fn start_after_preserves_implied_directories_for_recursive_globs() -> Result<()> {
    let op = Operator::new(Memory::default())?;
    for path in [
        "t/a/1.txt",
        "t/a/x/1.txt",
        "t/a/y/1.txt",
        "t/b/2.txt",
        "t/c/3.txt",
    ] {
        op.write(path, "data").await?;
    }

    let full_all: Vec<String> = op
        .list_with("t/")
        .glob("**")
        .await?
        .into_iter()
        .map(|entry| entry.path().to_owned())
        .collect();
    assert_eq!(
        full_all,
        [
            "t/a/",
            "t/a/1.txt",
            "t/a/x/",
            "t/a/x/1.txt",
            "t/a/y/",
            "t/a/y/1.txt",
            "t/b/",
            "t/b/2.txt",
            "t/c/",
            "t/c/3.txt",
        ]
    );

    for marker in ["t/a/", "t/a/1.txt", "t/a/x/", "t/a/x/1.txt", "t/b/"] {
        let got: Vec<String> = op
            .list_with("t/")
            .glob("**")
            .start_after(marker)
            .await?
            .into_iter()
            .map(|entry| entry.path().to_owned())
            .collect();
        let expected: Vec<String> = full_all
            .iter()
            .filter(|path| path.as_str() > marker)
            .cloned()
            .collect();
        assert_eq!(got, expected, "glob ** with start_after={marker:?}");
    }

    let got_dirs: Vec<String> = op
        .list_with("t/")
        .glob("**/")
        .start_after("t/a/x/1.txt")
        .await?
        .into_iter()
        .map(|entry| entry.path().to_owned())
        .collect();
    assert_eq!(got_dirs, ["t/a/y/", "t/b/", "t/c/"]);

    let got_one_level_dirs: Vec<String> = op
        .list_with("/")
        .glob("t/*/")
        .start_after("t/a/")
        .await?
        .into_iter()
        .map(|entry| entry.path().to_owned())
        .collect();
    assert_eq!(got_one_level_dirs, ["t/b/", "t/c/"]);

    // Non-trailing-slash prefix with start_after also emits implied directories after the cursor.
    op.write("p/a", "data").await?;
    op.write("p/ab/c", "data").await?;
    let got_prefix: Vec<String> = op
        .list_with("p/a")
        .glob("**")
        .start_after("p/a")
        .await?
        .into_iter()
        .map(|entry| entry.path().to_owned())
        .collect();
    assert_eq!(got_prefix, ["p/ab/", "p/ab/c"]);

    Ok(())
}

#[tokio::test]
async fn start_after_with_explicit_directory_markers_returns_each_directory_once() -> Result<()> {
    let inner = Arc::new(ListFixture {
        recursive: true,
        entries: vec![
            oio::Entry::new("t/b/", MetadataBuilder::dir().build()),
            oio::Entry::new("t/b/1.txt", MetadataBuilder::file(0).build()),
            oio::Entry::new("t/c/2.txt", MetadataBuilder::file(0).build()),
            oio::Entry::new("t/c/", MetadataBuilder::dir().build()),
        ],
        ..ListFixture::new(&[])
    });
    let op = Operator::from_parts(OperationContext::default(), inner).layer(GlobLayer);
    let entries = op.list_with("/").glob("**/").start_after("t/a/").await?;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["t/b/", "t/c/"]
    );
    Ok(())
}

#[tokio::test]
async fn start_after_with_versions_does_not_synthesize_implied_directories() -> Result<()> {
    let mut meta = MetadataBuilder::file(0);
    meta.version("v1");
    let inner = Arc::new(ListFixture {
        recursive: true,
        entries: vec![oio::Entry::new("t/b/1.txt", meta.build())],
        ..ListFixture::new(&[])
    });
    let op = Operator::from_parts(OperationContext::default(), inner).layer(GlobLayer);
    let entries = op
        .list_with("/")
        .glob("**")
        .start_after("t/a/")
        .versions(true)
        .await?;
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.path(), entry.metadata().version()))
            .collect::<Vec<_>>(),
        [("t/b/1.txt", Some("v1"))]
    );
    Ok(())
}

#[derive(Debug)]
struct ListFixture {
    requests: Mutex<Vec<(String, OpList)>>,
    entries: Vec<oio::Entry>,
    native_glob: bool,
    denied: Option<&'static str>,
    recursive: bool,
}

impl ListFixture {
    fn new(paths: &[&str]) -> Self {
        Self {
            requests: Mutex::default(),
            entries: paths
                .iter()
                .map(|path| oio::Entry::new(path, MetadataBuilder::file(0).build()))
                .collect(),
            native_glob: false,
            denied: None,
            recursive: false,
        }
    }
}

impl Service for ListFixture {
    type Reader = ();
    type Writer = ();
    type Lister = TestLister;
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("test")
    }

    fn capability(&self) -> Capability {
        Capability {
            list: true,
            list_with_glob: self.native_glob,
            list_with_recursive: self.recursive,
            list_with_limit: true,
            list_with_start_after: true,
            list_with_versions: true,
            list_with_deleted: true,
            ..Default::default()
        }
    }

    fn list(&self, _: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.requests.lock().unwrap().push((path.to_owned(), args));
        if self.denied == Some(path) {
            return Err(Error::new(ErrorKind::PermissionDenied, "denied"));
        }
        Ok(TestLister(self.entries.clone().into()))
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

struct TestLister(VecDeque<oio::Entry>);

impl List for TestLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        Ok(self.0.pop_front())
    }
}
