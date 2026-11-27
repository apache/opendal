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

#![cfg(all(
    feature = "executors-tokio",
    feature = "layers-concurrent-limit",
    feature = "layers-retry",
    feature = "layers-timeout",
    not(target_arch = "wasm32")
))]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use opendal::executors::TokioExecutor;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer, SimulateLayer, TimeoutLayer};
use opendal::raw::{oio, *};
use opendal::*;

#[derive(Debug)]
struct Fixture;

impl Service for Fixture {
    type Reader = ();
    type Writer = ();
    type Lister = oio::PageLister<Pages>;
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("test")
    }

    fn capability(&self) -> Capability {
        Capability {
            list: true,
            ..Default::default()
        }
    }

    fn list(&self, ctx: &OperationContext, path: &str, _: OpList) -> Result<Self::Lister> {
        Ok(oio::PageLister::new(Pages {
            path: path.to_owned(),
            transport: ctx.http_transport().clone(),
        }))
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
    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<()> {
        unreachable!()
    }
    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<()> {
        unreachable!()
    }
    fn delete(&self, _: &OperationContext) -> Result<()> {
        unreachable!()
    }
    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<()> {
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

struct Pages {
    path: String,
    transport: HttpTransporter,
}

impl oio::PageList for Pages {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let (request, entry) = match (self.path.as_str(), ctx.token.is_empty()) {
            ("/", true) => ("root-one", "a/"),
            ("/", false) => ("root-two", "b/"),
            ("a/", _) => ("a", "a/first.jpg"),
            ("b/", _) => ("b", "b/second.jpg"),
            (path, _) => panic!("unexpected listing for {path}"),
        };
        let uri = format!("http://fixture.example/{request}");
        self.transport
            .fetch(http::Request::get(uri).body(Buffer::new()).unwrap())
            .await?;

        ctx.done = request != "root-one";
        if !ctx.done {
            ctx.token = "second-page".into();
        }
        let metadata = if entry.ends_with('/') {
            MetadataBuilder::dir()
        } else {
            MetadataBuilder::file(0)
        };
        ctx.entries
            .push_back(oio::Entry::new(entry, metadata.build()));
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum SecondPage {
    Delayed,
    TimeoutOnFirstAttempt,
}

type Requests = Arc<Mutex<Vec<String>>>;

struct Transport {
    second_page: SecondPage,
    requests: Requests,
}

impl HttpTransport for Transport {
    async fn fetch(&self, request: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let path = request.uri().path();
        let first_attempt = {
            let mut requests = self.requests.lock().unwrap();
            let first_attempt = !requests.iter().any(|recorded| recorded == path);
            requests.push(path.to_owned());
            first_attempt
        };
        if path == "/root-two" {
            match self.second_page {
                SecondPage::Delayed => tokio::time::sleep(Duration::from_millis(10)).await,
                SecondPage::TimeoutOnFirstAttempt if first_attempt => std::future::pending().await,
                SecondPage::TimeoutOnFirstAttempt => {}
            }
        }
        Ok(http::Response::new(HttpBody::new(
            futures::stream::empty(),
            Some(0),
        )))
    }
}

fn operator(second_page: SecondPage) -> (Operator, Requests) {
    let requests = Requests::default();
    let transport = Transport {
        second_page,
        requests: requests.clone(),
    };
    let ctx = OperationContext::default()
        .with_http_transport(HttpTransporter::new(transport))
        .with_executor(Executor::with(TokioExecutor::default()));
    let op = Operator::from_parts(ctx, Arc::new(Fixture)).layer(SimulateLayer::default());
    (op, requests)
}

#[tokio::test]
async fn guided_glob_progresses_with_one_http_permit() -> anyhow::Result<()> {
    let (op, requests) = operator(SecondPage::Delayed);
    let op = op.layer(ConcurrentLimitLayer::new(16).with_http_concurrent_limit(1));
    let entries =
        tokio::time::timeout(Duration::from_secs(5), op.list_with("/").glob("*/*.jpg")).await??;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["a/first.jpg", "b/second.jpg"]
    );
    assert_eq!(
        *requests.lock().unwrap(),
        ["/root-one", "/a", "/root-two", "/b"]
    );
    Ok(())
}

#[tokio::test]
async fn guided_glob_reissues_a_timed_out_parent_page() -> anyhow::Result<()> {
    let (op, requests) = operator(SecondPage::TimeoutOnFirstAttempt);
    let op = op
        .layer(TimeoutLayer::new().with_io_timeout(Duration::from_millis(100)))
        .layer(
            RetryLayer::new()
                .with_min_delay(Duration::from_millis(1))
                .with_max_times(2),
        );
    let entries =
        tokio::time::timeout(Duration::from_secs(5), op.list_with("/").glob("*/*.jpg")).await??;
    assert_eq!(
        entries.iter().map(Entry::path).collect::<Vec<_>>(),
        ["a/first.jpg", "b/second.jpg"]
    );
    assert_eq!(
        *requests.lock().unwrap(),
        ["/root-one", "/a", "/root-two", "/root-two", "/b"]
    );
    Ok(())
}
