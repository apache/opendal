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

use std::sync::Mutex;

use super::*;

#[derive(Debug, Default)]
struct MockService {
    calls: Arc<Mutex<Vec<String>>>,
}

impl Service for MockService {
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = MockCopier;
    type Composer = MockComposer;

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("mock", "/", "mock")
    }

    fn capability(&self) -> Capability {
        Capability {
            copy: true,
            compose: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        unimplemented!()
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        unimplemented!()
    }

    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
        unimplemented!()
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        unimplemented!()
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        unimplemented!()
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        unimplemented!()
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        unimplemented!()
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        unimplemented!()
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("copy:{from}->{to}"));
        Ok(MockCopier {
            calls: self.calls.clone(),
            done: false,
        })
    }

    fn compose(
        &self,
        _ctx: &OperationContext,
        to: &str,
        _args: OpCompose,
    ) -> Result<Self::Composer> {
        self.calls.lock().unwrap().push(format!("compose:{to}"));
        Ok(MockComposer {
            calls: self.calls.clone(),
            count: 0,
        })
    }
}

struct MockCopier {
    calls: Arc<Mutex<Vec<String>>>,
    done: bool,
}

impl oio::Copy for MockCopier {
    async fn next(&mut self) -> Result<Option<usize>> {
        self.calls.lock().unwrap().push("copier.next".to_string());
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            Ok(Some(5))
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.calls.lock().unwrap().push("copier.close".to_string());
        Ok(MetadataBuilder::file(5).build())
    }

    async fn abort(&mut self) -> Result<()> {
        self.calls.lock().unwrap().push("copier.abort".to_string());
        Ok(())
    }
}

struct MockComposer {
    calls: Arc<Mutex<Vec<String>>>,
    count: u64,
}

impl oio::Compose for MockComposer {
    async fn compose(&mut self, path: &str, _args: OpRead) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("composer.compose:{path}"));
        self.count += 1;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.calls
            .lock()
            .unwrap()
            .push("composer.close".to_string());
        Ok(MetadataBuilder::file(self.count).build())
    }
}

#[tokio::test]
async fn wraps_copier_and_composer_operations() {
    let mock = Arc::new(MockService::default());
    let calls = mock.calls.clone();
    let service = HotpathLayer::new().apply_service(mock);
    let ctx = OperationContext::default();

    let mut copier = service.copy(&ctx, "src", "dst", OpCopy::default()).unwrap();
    assert_eq!(oio::Copy::next(&mut copier).await.unwrap(), Some(5));
    assert_eq!(oio::Copy::next(&mut copier).await.unwrap(), None);
    assert_eq!(
        oio::Copy::close(&mut copier)
            .await
            .unwrap()
            .content_length(),
        5
    );

    let mut aborted = service
        .copy(&ctx, "src", "dst2", OpCopy::default())
        .unwrap();
    oio::Copy::abort(&mut aborted).await.unwrap();

    let mut composer = service
        .compose(&ctx, "composed", OpCompose::default())
        .unwrap();
    oio::Compose::compose(&mut composer, "part1", OpRead::default())
        .await
        .unwrap();
    oio::Compose::compose(&mut composer, "part2", OpRead::default())
        .await
        .unwrap();
    assert_eq!(
        oio::Compose::close(&mut composer)
            .await
            .unwrap()
            .content_length(),
        2
    );

    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [
            "copy:src->dst",
            "copier.next",
            "copier.next",
            "copier.close",
            "copy:src->dst2",
            "copier.abort",
            "compose:composed",
            "composer.compose:part1",
            "composer.compose:part2",
            "composer.close",
        ]
    );
}
