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

use crate::raw::*;
use crate::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::Capability;

use crate::Metadata;
use crate::Operator;
use crate::raw::oio;

#[derive(Debug, Default)]
struct MockService {
    capability: Capability,
    calls: Arc<AtomicUsize>,
    checksum_hints: Arc<Mutex<Vec<(Operation, bool)>>>,
}

impl Builder for MockService {
    type Config = ();

    fn build(self) -> Result<impl Service> {
        Ok(self)
    }
}

impl Service for MockService {
    type Reader = MockReader;
    type Writer = MockWriter;
    type Lister = ();
    type Deleter = MockDeleter;
    type Copier = MockCopier;
    type Composer = MockComposer;

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("memory")
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, _: &OperationContext, _: &str, args: OpStat) -> Result<RpStat> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.checksum_hints
            .lock()
            .unwrap()
            .push((Operation::Stat, args.checksum()));
        Ok(RpStat::new(MetadataBuilder::file(0).build()))
    }

    fn read(&self, _ctx: &OperationContext, _: &str, args: OpRead) -> Result<Self::Reader> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.checksum_hints
            .lock()
            .unwrap()
            .push((Operation::Read, args.checksum()));
        Ok(MockReader)
    }

    fn write(&self, _ctx: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        Ok(MockWriter)
    }

    fn list(&self, _ctx: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        Ok(MockDeleter)
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(MockCopier)
    }

    fn compose(&self, _: &OperationContext, _: &str, _: OpCompose) -> Result<Self::Composer> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(MockComposer {
            accepted: 0,
            calls: self.calls.clone(),
        })
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(RpRename::default())
    }

    async fn restore(&self, _: &OperationContext, _: &str, _: OpRestore) -> Result<RpRestore> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(RpRestore::default())
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

struct MockReader;

impl oio::Read for MockReader {
    async fn open(&self, _: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        Ok((
            RpRead::new({
                let metadata = MetadataBuilder::file(0);
                metadata.build()
            }),
            Box::new(Buffer::new()) as Box<dyn oio::ReadStreamDyn>,
        ))
    }

    async fn read(&self, _: BytesRange) -> Result<(RpRead, Buffer)> {
        Ok((
            RpRead::new({
                let metadata = MetadataBuilder::file(0);
                metadata.build()
            }),
            Buffer::new(),
        ))
    }
}

struct MockWriter;

impl oio::Write for MockWriter {
    async fn write(&mut self, _: Buffer) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        Ok(MetadataBuilder::unknown().build())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

struct MockDeleter;

impl oio::Delete for MockDeleter {
    async fn delete(&mut self, _: &str, _: OpDelete) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

struct MockCopier;

impl oio::Copy for MockCopier {
    async fn next(&mut self) -> Result<Option<usize>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<Metadata> {
        Ok(MetadataBuilder::file(0).build())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

struct MockComposer {
    accepted: usize,
    calls: Arc<AtomicUsize>,
}

impl oio::Compose for MockComposer {
    async fn compose(&mut self, _: &str, _: OpRead) -> Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.accepted += 1;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        if self.accepted == 0 {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "compose requires at least one source object",
            ));
        }
        Ok(MetadataBuilder::file(0).build())
    }
}

fn new_test_operator(capability: Capability) -> Operator {
    let srv = MockService {
        capability,
        ..Default::default()
    };

    Operator::from_parts(OperationContext::default(), Arc::new(srv))
}

async fn assert_override_rejects(
    capability: Capability,
    overrides: &str,
    operation: impl AsyncFn(&Operator) -> Result<()>,
) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let op = Operator::new(MockService {
        capability,
        calls: calls.clone(),
        ..Default::default()
    })?;
    let overridden = op
        .clone()
        .layer(crate::layers::CapabilityOverrideLayer::from_overrides(
            overrides,
        )?);

    let result = operation(&overridden).await;
    assert_eq!(calls.load(Ordering::Relaxed), 0, "{overrides}");
    let err = result.expect_err(overrides);
    assert_eq!(err.kind(), ErrorKind::Unsupported, "{overrides}: {err}");

    operation(&op).await?;
    assert_eq!(calls.load(Ordering::Relaxed), 1, "{overrides}");
    Ok(())
}

#[tokio::test]
async fn test_stat_honors_capability_override() -> Result<()> {
    assert_override_rejects(
        Capability {
            stat: true,
            stat_with_if_match: true,
            ..Default::default()
        },
        "stat_with_if_match=false",
        async |op| op.stat_with("path").if_match("etag").await.map(drop),
    )
    .await?;

    let timestamp = Timestamp::from_second(1)?;
    assert_override_rejects(
        Capability {
            stat: true,
            stat_with_if_unmodified_since: true,
            ..Default::default()
        },
        "stat_with_if_unmodified_since=false",
        async |op| {
            op.stat_with("path")
                .if_unmodified_since(timestamp)
                .await
                .map(drop)
        },
    )
    .await
}

#[tokio::test]
async fn test_read_and_reader_honor_capability_override() -> Result<()> {
    let capability = Capability {
        read: true,
        read_with_if_match: true,
        read_with_version: true,
        ..Default::default()
    };
    assert_override_rejects(capability, "read_with_if_match=false", async |op| {
        op.read_with("path").if_match("etag").await.map(drop)
    })
    .await?;
    assert_override_rejects(capability, "read_with_version=false", async |op| {
        op.reader_with("path").version("version").await.map(drop)
    })
    .await
}

#[tokio::test]
async fn test_checksum_metadata_hint_reaches_read_and_stat_services() -> Result<()> {
    let hints = Arc::new(Mutex::new(Vec::new()));
    let op = Operator::new(MockService {
        capability: Capability {
            stat: true,
            read: true,
            ..Default::default()
        },
        checksum_hints: hints.clone(),
        ..Default::default()
    })?;

    let meta = op.stat_with("path").checksum(true).await?;
    assert_eq!(meta.checksum(), None);
    op.stat("path").await?;
    op.read_with("path").checksum(true).await?;
    op.read("path").await?;
    let reader = op.reader_with("path").checksum(true).await?;
    reader.read(..).await?;
    op.reader("path").await?;
    op.reader_with("path")
        .checksum(true)
        .checksum(false)
        .await?;

    assert_eq!(
        *hints.lock().unwrap(),
        [
            (Operation::Stat, true),
            (Operation::Stat, false),
            (Operation::Read, true),
            (Operation::Read, false),
            (Operation::Read, true),
            (Operation::Read, false),
            (Operation::Read, false),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn test_copy_and_copier_honor_capability_override() -> Result<()> {
    let capability = Capability {
        copy: true,
        copy_with_if_match: true,
        copy_with_if_not_exists: true,
        ..Default::default()
    };
    assert_override_rejects(capability, "copy_with_if_match=false", async |op| {
        op.copy_with("from", "to").if_match("etag").await.map(drop)
    })
    .await?;
    assert_override_rejects(capability, "copy_with_if_not_exists=false", async |op| {
        op.copier_with("from", "to")
            .if_not_exists(true)
            .await
            .map(drop)
    })
    .await
}

#[tokio::test]
async fn test_list_and_lister_honor_capability_override() -> Result<()> {
    let capability = Capability {
        list: true,
        list_with_glob: true,
        list_with_start_after: true,
        list_with_versions: true,
        list_with_deleted: true,
        ..Default::default()
    };
    assert_override_rejects(capability, "list_with_glob=false", async |op| {
        op.list_with("path/").glob("*.jpg").await.map(drop)
    })
    .await?;
    assert_override_rejects(capability, "list_with_start_after=false", async |op| {
        op.lister_with("path/")
            .start_after("path/key")
            .await
            .map(drop)
    })
    .await?;
    assert_override_rejects(capability, "list_with_versions=false", async |op| {
        op.list_with("path/").versions(true).await.map(drop)
    })
    .await?;
    assert_override_rejects(capability, "list_with_deleted=false", async |op| {
        op.list_with("path/").deleted(true).await.map(drop)
    })
    .await
}

#[tokio::test]
async fn test_rename_honors_capability_override() -> Result<()> {
    assert_override_rejects(
        Capability {
            rename: true,
            rename_with_if_not_exists: true,
            ..Default::default()
        },
        "rename_with_if_not_exists=false",
        async |op| op.rename_with("from", "to").if_not_exists(true).await,
    )
    .await
}

#[tokio::test]
async fn test_restore_honors_capability_override() -> Result<()> {
    assert_override_rejects(
        Capability {
            restore: true,
            restore_with_version: true,
            ..Default::default()
        },
        "restore_with_version=false",
        async |op| op.restore_with("path").version("version").await,
    )
    .await
}

#[tokio::test]
async fn test_compose_destination_honors_capability_override() -> Result<()> {
    assert_override_rejects(
        Capability {
            compose: true,
            compose_with_if_match: true,
            ..Default::default()
        },
        "compose_with_if_match=false",
        async |op| op.composer_with("to").if_match("etag").await.map(drop),
    )
    .await
}

#[tokio::test]
async fn test_compose_source_honors_capability_override() -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let op = Operator::new(MockService {
        capability: Capability {
            compose: true,
            compose_with_source_version: true,
            ..Default::default()
        },
        calls: calls.clone(),
        ..Default::default()
    })?;
    let overridden = op
        .clone()
        .layer(crate::layers::CapabilityOverrideLayer::from_overrides(
            "compose_with_source_version=false",
        )?);
    let mut composer = overridden.composer("to").await?;
    let before_source = calls.load(Ordering::Relaxed);
    let err = composer
        .compose_with("from")
        .version("version")
        .await
        .expect_err("overridden source version must be rejected");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert_eq!(calls.load(Ordering::Relaxed), before_source);

    let mut composer = op.composer("to").await?;
    let before_source = calls.load(Ordering::Relaxed);
    composer.compose_with("from").version("version").await?;
    assert_eq!(calls.load(Ordering::Relaxed), before_source + 1);
    Ok(())
}

#[tokio::test]
async fn test_read() {
    let op = new_test_operator(Capability {
        read: true,
        ..Default::default()
    });
    let res = op.read_with("path").version("version").await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

    let op = new_test_operator(Capability {
        read: true,
        read_with_version: true,
        ..Default::default()
    });
    let res = op.read_with("path").version("version").await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_stat() {
    let op = new_test_operator(Capability {
        stat: true,
        ..Default::default()
    });
    let res = op.stat_with("path").version("version").await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

    let op = new_test_operator(Capability {
        stat: true,
        stat_with_version: true,
        ..Default::default()
    });
    let res = op.stat_with("path").version("version").await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_write_with() {
    let op = new_test_operator(Capability {
        write: true,
        write_with_if_not_exists: true,
        ..Default::default()
    });
    let res = op.write_with("path", "".as_bytes()).append(true).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

    let res = op
        .write_with("path", "".as_bytes())
        .if_none_match("etag")
        .await;
    assert!(res.is_err());
    assert_eq!(
        res.unwrap_err().to_string(),
        "Unsupported (permanent) at write, context: { service: memory } => The service does not support the operation write with the arguments if_none_match. Please verify if the relevant flags have been enabled, or submit an issue if you believe this is incorrect."
    );

    // Now try a wildcard if-none-match
    let res = op
        .write_with("path", "".as_bytes())
        .if_none_match("*")
        .await;
    assert!(res.is_err());
    assert_eq!(
        res.unwrap_err().to_string(),
        "Unsupported (permanent) at write, context: { hint: use if_not_exists instead, service: memory } => The service does not support the operation write with the arguments if_none_match. Please verify if the relevant flags have been enabled, or submit an issue if you believe this is incorrect."
    );

    let res = op
        .write_with("path", "".as_bytes())
        .if_not_exists(true)
        .await;
    assert!(res.is_ok());

    let op = new_test_operator(Capability {
        write: true,
        write_can_append: true,
        write_with_if_not_exists: true,
        write_with_if_none_match: true,
        ..Default::default()
    });
    let res = op.writer_with("path").append(true).await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_write_with_unsupported_expiration() {
    let op = Operator::new(crate::services::Memory::default()).unwrap();
    let expires = Duration::from_secs(60);
    let err = op
        .write_with("path", "data")
        .expires(expires)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);

    let err = op
        .writer_with("path")
        .expires(expires)
        .await
        .expect_err("writer with expiration must fail");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(!op.exists("path").await.unwrap());
}

#[tokio::test]
async fn test_delete() {
    let op = new_test_operator(Capability {
        delete: true,
        ..Default::default()
    });
    let res = op.delete_with("path").version("version").await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

    let op = new_test_operator(Capability {
        delete: true,
        delete_with_version: true,
        ..Default::default()
    });
    let res = op.delete_with("path").version("version").await;
    assert!(res.is_ok())
}

#[tokio::test]
async fn test_list() {
    let op = new_test_operator(Capability {
        list: true,
        ..Default::default()
    });
    for recursive in [false, true] {
        let err = op
            .list_with("path/")
            .recursive(recursive)
            .start_after("path/key")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        assert!(err.message().contains("start_after"));
    }
    let err = op.list_with("path/").versions(true).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.message().contains("versions"));
    let err = op.list_with("path/").deleted(true).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.message().contains("deleted"));
    let err = op.list_with("path/").glob("*.jpg").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.message().contains("glob"));
    assert!(
        op.list_with("path/")
            .versions(false)
            .deleted(false)
            .await
            .is_ok()
    );

    let op = new_test_operator(Capability {
        list: true,
        list_with_start_after: true,
        list_with_versions: true,
        list_with_deleted: true,
        ..Default::default()
    });
    assert!(op.list_with("path/").start_after("path/key").await.is_ok());
    assert!(op.list_with("path/").versions(true).await.is_ok());
    assert!(op.list_with("path/").deleted(true).await.is_ok());
}

/// `limit` is a backend hint and `recursive` is simulated by `SimulateLayer`,
/// so neither is gated on a capability.
#[tokio::test]
async fn test_list_limit_and_recursive_need_no_capability() {
    let op = new_test_operator(Capability {
        list: true,
        ..Default::default()
    });
    assert!(op.list_with("path/").limit(1).await.is_ok());
    assert!(op.list_with("path/").recursive(true).await.is_ok());
}

#[tokio::test]
async fn test_compose() {
    let op = new_test_operator(Capability {
        compose: true,
        ..Default::default()
    });

    let err = op
        .compose(
            [(
                "from",
                options::ComposeSourceOptions {
                    version: Some("version".to_string()),
                    ..Default::default()
                },
            )],
            "to",
        )
        .await
        .expect_err("source version must require a capability");
    assert_eq!(err.kind(), ErrorKind::Unsupported);

    let mut composer = op
        .composer("etag-target")
        .await
        .expect("compose capability must create a composer");
    let err = composer
        .compose_with("from")
        .if_match("etag")
        .await
        .expect_err("source ETag must require a capability");
    assert_eq!(err.kind(), ErrorKind::Unsupported);

    let err = op
        .compose_with(["from"], "to")
        .content_type("text/plain")
        .await
        .expect_err("destination content type must require a capability");
    assert_eq!(err.kind(), ErrorKind::Unsupported);

    let err = op
        .compose(Vec::<String>::new(), "to")
        .await
        .expect_err("empty composition must fail");
    assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

    let err = op
        .compose(["same"], "same")
        .await
        .expect_err("a destination cannot be its own source");
    assert_eq!(err.kind(), ErrorKind::IsSameFile);

    let op = new_test_operator(Capability {
        compose: true,
        compose_with_source_version: true,
        compose_with_content_type: true,
        ..Default::default()
    });
    op.compose_with(
        [(
            "from",
            options::ComposeSourceOptions {
                version: Some("version".to_string()),
                ..Default::default()
            },
        )],
        "to",
    )
    .content_type("text/plain")
    .await
    .expect("supported compose options must be forwarded");
}

#[tokio::test]
async fn test_copy_with_metadata_replacement() {
    let op = new_test_operator(Capability {
        copy: true,
        ..Default::default()
    });

    let err = op
        .copy_with("from", "to")
        .user_metadata([("mtime".to_owned(), "42".to_owned())])
        .await
        .expect_err("user metadata must require a capability");
    assert_eq!(err.kind(), ErrorKind::Unsupported);

    let err = op
        .copy_with("from", "to")
        .content_type("text/plain")
        .await
        .expect_err("content type must require a capability");
    assert_eq!(err.kind(), ErrorKind::Unsupported);

    let err = op
        .copy_with("from", "to")
        .content_language("en")
        .await
        .expect_err("content language must require a capability");
    assert_eq!(err.kind(), ErrorKind::Unsupported);

    let op = new_test_operator(Capability {
        copy: true,
        copy_with_user_metadata: true,
        copy_with_content_type: true,
        copy_with_content_language: true,
        ..Default::default()
    });

    let err = op
        .copy("same", "same")
        .await
        .expect_err("a plain copy onto itself must fail");
    assert_eq!(err.kind(), ErrorKind::IsSameFile);

    op.copy_with("same", "same")
        .user_metadata([("mtime".to_owned(), "42".to_owned())])
        .content_type("text/plain")
        .await
        .expect("replacing metadata in place must be forwarded");

    op.copy_with("same", "same")
        .content_language("en")
        .await
        .expect("replacing content language in place must be forwarded");
}

#[tokio::test]
async fn test_version_preconditions_are_forwarded() -> Result<()> {
    let op = new_test_operator(Capability {
        stat: true,
        stat_with_version: true,
        stat_with_if_version_match: true,
        read: true,
        read_with_if_version_match: true,
        write: true,
        write_with_if_match: true,
        write_with_if_version_match: true,
        delete: true,
        delete_with_if_version_match: true,
        delete_with_if_version_not_match: true,
        copy: true,
        copy_with_if_version_match: true,
        ..Default::default()
    });

    op.stat_with("path").if_version_match("version").await?;
    op.read_with("path").if_version_match("version").await?;
    op.write_with("path", "")
        .if_version_match("version")
        .await?;
    op.delete_with("path").if_version_match("version").await?;
    op.copy_with("from", "to")
        .if_version_match("version")
        .await?;

    op.write_with("path", "")
        .if_match("etag")
        .if_version_match("version")
        .await?;

    op.stat_with("path")
        .version("selected")
        .if_version_match("current")
        .await?;

    op.delete_with("path")
        .if_version_match("matched")
        .if_version_not_match("not-matched")
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_if_not_changed_lowers_before_capability_check() -> Result<()> {
    let op = new_test_operator(Capability {
        write: true,
        write_with_if_match: true,
        write_with_if_version_match: true,
        delete: true,
        delete_with_if_match: true,
        copy: true,
        copy_with_if_match: true,
        ..Default::default()
    });
    let metadata = {
        let mut metadata = MetadataBuilder::unknown();
        metadata.etag("etag").version("version");
        metadata.build()
    };

    let err = op
        .write_with("path", "")
        .if_not_changed(&MetadataBuilder::unknown().build())
        .await
        .expect_err("metadata without an identity must fail during lowering");
    assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

    op.write_with("path", "")
        .if_match("other-etag")
        .if_version_match("version")
        .if_not_changed(&metadata)
        .await?;

    let err = op
        .write_with("path", "")
        .if_version_match("other-version")
        .if_not_changed(&metadata)
        .await
        .expect_err("different selected version must fail");
    assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

    op.copy_with("from", "to")
        .if_match("etag")
        .if_not_changed(&metadata)
        .await?;
    let err = op
        .copy_with("from", "to")
        .if_match("other-etag")
        .if_not_changed(&metadata)
        .await
        .expect_err("different selected etag must fail");
    assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

    op.delete_with("path")
        .if_match("etag")
        .if_not_changed(&metadata)
        .await?;
    let err = op
        .delete_with("path")
        .if_match("other-etag")
        .if_not_changed(&metadata)
        .await
        .expect_err("different selected etag must fail");
    assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

    let op = new_test_operator(Capability {
        write: true,
        write_with_if_match: true,
        delete: true,
        delete_with_if_match: true,
        copy: true,
        copy_with_if_match: true,
        compose: true,
        compose_with_if_match: true,
        compose_with_source_if_match: true,
        ..Default::default()
    });

    op.write_with("path", "")
        .if_match("etag")
        .if_not_changed(&metadata)
        .await?;

    op.copy_with("from", "to")
        .if_match("etag")
        .if_not_changed(&metadata)
        .await?;

    op.delete_with("path")
        .if_match("etag")
        .if_not_changed(&metadata)
        .await?;

    op.compose_with(["from"], "to")
        .if_match("etag")
        .if_not_changed(&metadata)
        .await?;

    op.compose(
        [(
            "from",
            options::ComposeSourceOptions {
                if_not_changed: Some(metadata.clone()),
                ..Default::default()
            },
        )],
        "to",
    )
    .await?;

    let err = op
        .compose(
            [(
                "from",
                options::ComposeSourceOptions {
                    if_not_changed: Some(MetadataBuilder::unknown().build()),
                    ..Default::default()
                },
            )],
            "to",
        )
        .await
        .expect_err("source metadata without an identity must fail during lowering");
    assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

    let op = new_test_operator(Capability {
        write: true,
        delete: true,
        copy: true,
        compose: true,
        ..Default::default()
    });

    let err = op
        .write_with("path", "")
        .if_not_changed(&metadata)
        .await
        .expect_err("the derived write condition must be capability checked");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("if_match"));

    let err = op
        .copy_with("from", "to")
        .if_not_changed(&metadata)
        .await
        .expect_err("the derived copy condition must be capability checked");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("if_match"));

    let err = op
        .delete_with("path")
        .if_not_changed(&metadata)
        .await
        .expect_err("the derived delete condition must be capability checked");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("if_match"));

    let err = op
        .compose_with(["from"], "to")
        .if_not_changed(&metadata)
        .await
        .expect_err("the derived destination condition must be capability checked");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("if_match"));

    let err = op
        .compose(
            [(
                "from",
                options::ComposeSourceOptions {
                    if_not_changed: Some(metadata),
                    ..Default::default()
                },
            )],
            "to",
        )
        .await
        .expect_err("the derived source condition must be capability checked");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("source_if_match"));

    Ok(())
}

#[tokio::test]
async fn test_presign_conditions_validate_against_capabilities() {
    let op = new_test_operator(Capability::default());

    let err = op
        .presign_stat_options(
            "path",
            std::time::Duration::from_secs(60),
            options::StatOptions {
                if_version_match: Some("version".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("presign_stat rejects unsupported stat conditions");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("if_version_match"));

    let err = op
        .presign_read_options(
            "path",
            std::time::Duration::from_secs(60),
            options::ReadOptions {
                if_match: Some("etag".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("presign_read rejects unsupported read conditions");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("if_match"));

    let err = op
        .presign_delete_options(
            "path",
            std::time::Duration::from_secs(60),
            options::DeleteOptions {
                if_none_match: Some("etag".to_string()),
                if_version_not_match: Some("version".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("presign_delete rejects unsupported delete conditions");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("if_none_match"));

    let op = new_test_operator(Capability {
        stat_with_if_version_match: true,
        read_with_if_match: true,
        ..Default::default()
    });
    let err = op
        .presign_stat_options(
            "path",
            std::time::Duration::from_secs(60),
            options::StatOptions {
                if_version_match: Some("version".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("mock service rejects presign");
    assert!(err.to_string().contains("operation is not supported"));

    let err = op
        .presign_read_options(
            "path",
            std::time::Duration::from_secs(60),
            options::ReadOptions {
                if_match: Some("etag".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("mock service rejects presign");
    assert!(err.to_string().contains("operation is not supported"));

    let op = new_test_operator(Capability {
        write_with_if_match: true,
        ..Default::default()
    });
    let err = op
        .presign_write_options(
            "path",
            std::time::Duration::from_secs(60),
            options::WriteOptions {
                if_not_changed: Some({
                    let mut metadata = MetadataBuilder::unknown();
                    metadata.etag("etag");
                    metadata.build()
                }),
                ..Default::default()
            },
        )
        .await
        .expect_err("mock service rejects presign");
    assert!(err.to_string().contains("operation is not supported"));

    let metadata = {
        let mut metadata = MetadataBuilder::unknown();
        metadata.version("version").etag("etag");
        metadata.build()
    };
    let err = op
        .presign_write_options(
            "path",
            std::time::Duration::from_secs(60),
            options::WriteOptions {
                if_not_changed: Some(metadata.clone()),
                ..Default::default()
            },
        )
        .await
        .expect_err("mock service rejects presign");
    assert!(err.to_string().contains("operation is not supported"));

    let op = new_test_operator(Capability {
        delete_with_if_match: true,
        ..Default::default()
    });
    let err = op
        .presign_delete_options(
            "path",
            std::time::Duration::from_secs(60),
            options::DeleteOptions {
                if_not_changed: Some(metadata),
                ..Default::default()
            },
        )
        .await
        .expect_err("mock service rejects presign");
    assert!(err.to_string().contains("operation is not supported"));
}

#[tokio::test]
async fn test_rename_with_if_not_exists() {
    let op = new_test_operator(Capability {
        rename: true,
        ..Default::default()
    });
    let res = op.rename_with("from", "to").if_not_exists(true).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

    let op = new_test_operator(Capability {
        rename: true,
        rename_with_if_not_exists: true,
        ..Default::default()
    });
    let res = op.rename_with("from", "to").if_not_exists(true).await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_conditional_write_honors_capability_override() {
    // #8640: a conditional write disabled by an outer capability override
    // must fail instead of reaching the service.
    let op = Operator::new(crate::services::Memory::default())
        .unwrap()
        .layer(
            crate::layers::CapabilityOverrideLayer::from_overrides(
                "write_with_if_not_exists=false",
            )
            .unwrap(),
        );
    assert!(!op.info().capability().write_with_if_not_exists);

    let err = op
        .write_with("new", "data")
        .if_not_exists(true)
        .await
        .expect_err("if_not_exists write must fail when overridden off");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(!op.exists("new").await.unwrap());

    let err = op
        .writer_with("new")
        .if_not_exists(true)
        .await
        .expect_err("if_not_exists writer must fail when overridden off");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(!op.exists("new").await.unwrap());

    // The same conditional write still works without the override.
    let op = Operator::new(crate::services::Memory::default()).unwrap();
    assert!(op.info().capability().write_with_if_not_exists);
    op.write_with("new", "data")
        .if_not_exists(true)
        .await
        .expect("if_not_exists write must succeed when supported");
    assert!(op.exists("new").await.unwrap());
}

#[tokio::test]
async fn test_conditional_delete_honors_capability_override() {
    // #8640: the same bypass for deletes; the deleter handle must check too.
    let op = Operator::new(crate::services::Memory::default())
        .unwrap()
        .layer(
            crate::layers::CapabilityOverrideLayer::from_overrides("delete_with_recursive=false")
                .unwrap(),
        );
    assert!(!op.info().capability().delete_with_recursive);

    op.write("dir/file", "data").await.unwrap();
    let err = op
        .delete_with("dir/")
        .recursive(true)
        .await
        .expect_err("recursive delete must fail when overridden off");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(op.exists("dir/file").await.unwrap());

    let mut deleter = op.deleter().await.unwrap();
    let err = deleter
        .delete((
            "dir/".to_string(),
            options::DeleteOptions {
                recursive: true,
                ..Default::default()
            },
        ))
        .await
        .expect_err("recursive delete via handle must fail when overridden off");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(op.exists("dir/file").await.unwrap());

    // Without the override, the recursive delete succeeds.
    let op = Operator::new(crate::services::Memory::default()).unwrap();
    op.write("dir/file", "data").await.unwrap();
    op.delete_with("dir/").recursive(true).await.unwrap();
    assert!(!op.exists("dir/file").await.unwrap());
}

#[tokio::test]
async fn test_presign_write_honors_capability_override() {
    // #8640 follow-up: presigned writes must validate against the
    // effective capability, not just the inner service's.
    let op = new_test_operator(Capability {
        presign_write: true,
        write_with_if_match: true,
        ..Default::default()
    })
    .layer(
        crate::layers::CapabilityOverrideLayer::from_overrides("write_with_if_match=false")
            .unwrap(),
    );
    assert!(!op.info().capability().write_with_if_match);

    let err = op
        .presign_write_options(
            "path",
            std::time::Duration::from_secs(60),
            options::WriteOptions {
                if_match: Some("etag".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("presigned write with if_match must fail when overridden off");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("if_match"));

    // Without the override, the presign reaches the service.
    let op = new_test_operator(Capability {
        presign_write: true,
        write_with_if_match: true,
        ..Default::default()
    });
    let err = op
        .presign_write_options(
            "path",
            std::time::Duration::from_secs(60),
            options::WriteOptions {
                if_match: Some("etag".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("mock service rejects presign");
    assert!(err.to_string().contains("operation is not supported"));
}

#[tokio::test]
async fn test_presign_delete_honors_capability_override() {
    // #8640 follow-up: presigned deletes and conditional deletes must
    // validate against the effective capability.
    let op = new_test_operator(Capability {
        delete: true,
        presign_delete: true,
        delete_with_if_match: true,
        ..Default::default()
    })
    .layer(
        crate::layers::CapabilityOverrideLayer::from_overrides("delete_with_if_match=false")
            .unwrap(),
    );
    assert!(!op.info().capability().delete_with_if_match);

    let err = op
        .presign_delete_options(
            "path",
            std::time::Duration::from_secs(60),
            options::DeleteOptions {
                if_match: Some("etag".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("presigned delete with if_match must fail when overridden off");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("if_match"));

    let err = op
        .delete_with("path")
        .if_match("etag")
        .await
        .expect_err("conditional delete must fail when overridden off");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("if_match"));

    // Without the override, both reach the service.
    let op = new_test_operator(Capability {
        delete: true,
        presign_delete: true,
        delete_with_if_match: true,
        ..Default::default()
    });
    let err = op
        .presign_delete_options(
            "path",
            std::time::Duration::from_secs(60),
            options::DeleteOptions {
                if_match: Some("etag".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("mock service rejects presign");
    assert!(err.to_string().contains("operation is not supported"));
    op.delete_with("path").if_match("etag").await.unwrap();
}

#[tokio::test]
async fn test_presign_stat_and_read_validate_all_options_and_honor_override() {
    let since = Timestamp::from_second(1_700_000_000).unwrap();

    // Unsupported options fail fast before reaching the service.
    let op = new_test_operator(Capability {
        presign_stat: true,
        presign_read: true,
        ..Default::default()
    });
    for (name, stat_opts) in [
        (
            "version",
            options::StatOptions {
                version: Some("v1".to_string()),
                ..Default::default()
            },
        ),
        (
            "if_match",
            options::StatOptions {
                if_match: Some("etag".to_string()),
                ..Default::default()
            },
        ),
        (
            "if_none_match",
            options::StatOptions {
                if_none_match: Some("etag".to_string()),
                ..Default::default()
            },
        ),
        (
            "if_version_match",
            options::StatOptions {
                if_version_match: Some("v1".to_string()),
                ..Default::default()
            },
        ),
        (
            "if_version_not_match",
            options::StatOptions {
                if_version_not_match: Some("v1".to_string()),
                ..Default::default()
            },
        ),
        (
            "if_modified_since",
            options::StatOptions {
                if_modified_since: Some(since),
                ..Default::default()
            },
        ),
        (
            "if_unmodified_since",
            options::StatOptions {
                if_unmodified_since: Some(since),
                ..Default::default()
            },
        ),
    ] {
        let err = op
            .presign_stat_options("path", std::time::Duration::from_secs(60), stat_opts)
            .await
            .expect_err(name);
        assert_eq!(err.kind(), ErrorKind::Unsupported, "{name}: {err}");
        assert!(err.to_string().contains(name), "{name}: {err}");
    }

    for (name, read_opts) in [
        (
            "version",
            options::ReadOptions {
                version: Some("v1".to_string()),
                ..Default::default()
            },
        ),
        (
            "if_match",
            options::ReadOptions {
                if_match: Some("etag".to_string()),
                ..Default::default()
            },
        ),
        (
            "if_none_match",
            options::ReadOptions {
                if_none_match: Some("etag".to_string()),
                ..Default::default()
            },
        ),
        (
            "if_version_match",
            options::ReadOptions {
                if_version_match: Some("v1".to_string()),
                ..Default::default()
            },
        ),
        (
            "if_version_not_match",
            options::ReadOptions {
                if_version_not_match: Some("v1".to_string()),
                ..Default::default()
            },
        ),
        (
            "if_modified_since",
            options::ReadOptions {
                if_modified_since: Some(since),
                ..Default::default()
            },
        ),
        (
            "if_unmodified_since",
            options::ReadOptions {
                if_unmodified_since: Some(since),
                ..Default::default()
            },
        ),
    ] {
        let err = op
            .presign_read_options("path", std::time::Duration::from_secs(60), read_opts)
            .await
            .expect_err(name);
        assert_eq!(err.kind(), ErrorKind::Unsupported, "{name}: {err}");
        assert!(err.to_string().contains(name), "{name}: {err}");
    }

    // CapabilityOverrideLayer disabling stat_with_version / read_with_version
    // rejects presign_stat / presign_read before reaching the service.
    let op = new_test_operator(Capability {
        presign_stat: true,
        presign_read: true,
        stat_with_version: true,
        read_with_version: true,
        ..Default::default()
    })
    .layer(
        crate::layers::CapabilityOverrideLayer::from_overrides(
            "stat_with_version=false,read_with_version=false",
        )
        .unwrap(),
    );

    let err = op
        .presign_stat_options(
            "path",
            std::time::Duration::from_secs(60),
            options::StatOptions {
                version: Some("v1".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("presign_stat must honor CapabilityOverrideLayer");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("version"));

    let err = op
        .presign_read_options(
            "path",
            std::time::Duration::from_secs(60),
            options::ReadOptions {
                version: Some("v1".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("presign_read must honor CapabilityOverrideLayer");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("version"));
}
