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

use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use futures::StreamExt;
use futures::channel::{mpsc, oneshot};
use tokio::time::timeout;

use super::*;

type Request = (String, oneshot::Sender<()>);

#[derive(Debug, Default)]
struct Activity {
    active: AtomicUsize,
    peak: AtomicUsize,
}

struct InFlight<'a>(&'a Activity);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug)]
pub(super) struct FixturePages {
    requests: mpsc::UnboundedSender<Request>,
    activity: Arc<Activity>,
    split: Option<(&'static str, usize)>,
    hold_children: bool,
}

struct Pages {
    fixture: FixturePages,
    path: String,
    entries: Vec<oio::Entry>,
}

impl FixturePages {
    pub(super) fn lister(&self, path: &str, entries: Vec<oio::Entry>) -> oio::Lister {
        Box::new(oio::PageLister::new(Pages {
            fixture: self.clone(),
            path: path.to_owned(),
            entries,
        }))
    }
}

impl oio::PageList for Pages {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let active = self.fixture.activity.active.fetch_add(1, Ordering::Relaxed) + 1;
        let _request = InFlight(&self.fixture.activity);
        self.fixture
            .activity
            .peak
            .fetch_max(active, Ordering::Relaxed);

        let split = self.fixture.split.filter(|(path, _)| *path == self.path);
        if let Some((_, count)) = split
            && ctx.token.is_empty()
        {
            ctx.entries.extend(self.entries[..count].iter().cloned());
            ctx.token = "page-two".into();
            return Ok(());
        }
        if split.is_some() || (self.fixture.hold_children && self.path != "/") {
            let (release, wait) = oneshot::channel();
            self.fixture
                .requests
                .unbounded_send((self.path.clone(), release))
                .unwrap();
            wait.await.unwrap();
        }
        let offset = split.map_or(0, |(_, count)| count);
        ctx.entries.extend(self.entries[offset..].iter().cloned());
        ctx.done = true;
        Ok(())
    }
}

struct TokioExecutor;

impl Execute for TokioExecutor {
    fn execute(&self, task: BoxedStaticFuture<()>) {
        tokio::spawn(task);
    }
}

fn operator(
    paths: &[&str],
    split: Option<(&'static str, usize)>,
    hold_children: bool,
    executor: impl Execute,
) -> (Operator, mpsc::UnboundedReceiver<Request>, Arc<Activity>) {
    let (requests, receiver) = mpsc::unbounded();
    let activity = Arc::new(Activity::default());
    let fixture = ListFixture {
        pages: Some(FixturePages {
            requests,
            activity: activity.clone(),
            split,
            hold_children,
        }),
        ..ListFixture::tree(paths)
    };
    let ctx = OperationContext::default().with_executor(Executor::with(executor));
    let op = Operator::from_parts(ctx, Arc::new(fixture)).layer(SimulateLayer::default());
    (op, receiver, activity)
}

#[tokio::test]
async fn glob_bounds_nested_page_requests_and_progresses_with_siblings_held() -> Result<()> {
    let paths = [
        "a/",
        "b/",
        "c/",
        "d/",
        "e/",
        "f/",
        "g/",
        "h/",
        "a/1/",
        "a/2/",
        "a/3/",
        "a/4/",
        "a/5/",
        "a/6/",
        "a/7/",
        "a/8/",
        "a/1/file.jpg",
        "a/8/file.jpg",
    ];
    let (op, mut requests, activity) = operator(&paths, None, true, TokioExecutor);
    let list = tokio::spawn(async move { op.list_with("/").glob("*/*/*.jpg").await });
    timeout(Duration::from_secs(10), async {
        let mut pending = BTreeMap::new();
        for _ in 0..8 {
            let (path, release) = requests.next().await.unwrap();
            assert!(pending.insert(path, release).is_none());
        }
        assert_eq!(
            pending.keys().map(String::as_str).collect::<Vec<_>>(),
            paths[..8]
        );
        pending.remove("a/").unwrap().send(()).unwrap();

        // All seven siblings stay in flight while the foreground walk descends.
        for expected in &paths[8..16] {
            let (path, release) = requests.next().await.unwrap();
            assert_eq!(path, *expected);
            assert_eq!(activity.active.load(Ordering::Relaxed), 8);
            release.send(()).unwrap();
        }
        for release in pending.into_values() {
            release.send(()).unwrap();
        }
        let entries = list.await.unwrap()?;
        assert_eq!(
            entries.iter().map(Entry::path).collect::<Vec<_>>(),
            ["a/1/file.jpg", "a/8/file.jpg"]
        );
        assert_eq!(activity.peak.load(Ordering::Relaxed), 8);
        Ok(())
    })
    .await
    .expect("glob walk must finish while sibling requests are independently held")
}

#[tokio::test]
async fn glob_yields_first_match_before_requesting_next_page() -> Result<()> {
    for (paths, directory, pattern) in [
        (&["first.jpg", "second.jpg"][..], "/", "*.jpg"),
        (
            &["a/", "b/", "b/first.jpg", "b/second.jpg"][..],
            "b/",
            "*/*.jpg",
        ),
    ] {
        let (op, mut requests, _) = operator(paths, Some((directory, 1)), false, EagerExecutor);
        let mut lister = op.lister_with("/").glob(pattern).await?;
        let first = lister
            .try_next()
            .now_or_never()
            .expect("first match is ready")?;
        assert_eq!(first.unwrap().path(), paths[paths.len() - 2]);
        assert!(
            requests.try_recv().is_err(),
            "second page was requested before the first match"
        );

        let mut next = pin!(lister.try_next());
        assert!(futures::poll!(next.as_mut()).is_pending());
        let (path, release) = requests.try_recv().unwrap();
        assert_eq!(path, directory);
        release.send(()).unwrap();
        assert_eq!(next.await?.unwrap().path(), paths[paths.len() - 1]);
    }
    Ok(())
}

#[tokio::test]
async fn glob_descends_while_the_parent_page_is_pending() -> Result<()> {
    for finish in [true, false] {
        let (op, mut requests, activity) = operator(
            &["a/", "b/", "c/", "a/file.jpg"],
            Some(("/", 2)),
            true,
            TokioExecutor,
        );
        let first = tokio::spawn(async move {
            let mut lister = op.lister_with("/").glob("*/*.jpg").await?;
            let entry = lister.try_next().await?.unwrap();
            Ok::<_, Error>((lister, entry))
        });
        timeout(Duration::from_secs(10), async {
            let mut pending = BTreeMap::new();
            for _ in 0..3 {
                let (path, release) = requests.next().await.unwrap();
                assert!(pending.insert(path, release).is_none());
            }
            pending.remove("a/").unwrap().send(()).unwrap();
            let (mut lister, entry) = first.await.unwrap()?;
            assert_eq!(entry.path(), "a/file.jpg");
            if finish {
                // The original parent request resumes and discovers c/.
                for release in pending.into_values() {
                    release.send(()).unwrap();
                }
                let rest = tokio::spawn(async move { lister.try_next().await });
                let (path, release) = requests.next().await.unwrap();
                assert_eq!(path, "c/");
                release.send(()).unwrap();
                assert!(rest.await.unwrap()?.is_none());
            } else {
                drop(lister);
                for mut release in pending.into_values() {
                    release.cancellation().await;
                }
            }
            assert_eq!(activity.active.load(Ordering::Relaxed), 0);
            Ok::<(), Error>(())
        })
        .await
        .expect("the pending parent page must resume or be cancelled")?;
    }
    Ok(())
}
