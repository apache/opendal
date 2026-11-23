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

use crate::raw::*;
use crate::*;

// Keep the lister in `next_dir` until its first entry arrives so cancelling
// `FlatLister::next` cannot detach the lister from its directory.
enum NextDir<L> {
    Directory(oio::Entry),
    Listing(oio::Entry, L),
}

/// FlatLister will walk dir in bottom up way:
///
/// - List nested dir first
/// - Go back into parent dirs one by one
///
/// Given the following file tree:
///
/// ```txt
/// .
/// ├── dir_x/
/// │   ├── dir_y/
/// │   │   ├── dir_z/
/// │   │   └── file_c
/// │   └── file_b
/// └── file_a
/// ```
///
/// ToFlatLister will output entries like:
///
/// ```txt
/// dir_x/dir_y/dir_z/file_c
/// dir_x/dir_y/dir_z/
/// dir_x/dir_y/file_b
/// dir_x/dir_y/
/// dir_x/file_a
/// dir_x/
/// ```
///
/// # Note
///
/// There is no guarantee about the order between files and dirs at the same level.
/// We only make sure the nested dirs will show up before parent dirs.
///
/// Especially, for storage services that can't return dirs first, ToFlatLister
/// may output parent dirs' files before nested dirs, this is expected because files
/// always output directly while listing.
pub struct FlatLister<S: Service> {
    service: Arc<S>,
    ctx: OperationContext,

    next_dir: Option<NextDir<S::Lister>>,
    active_lister: Vec<(oio::Entry, S::Lister)>,
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this FlatLister.
unsafe impl<S: Service> Send for FlatLister<S> {}
/// # Safety
///
/// We will only take `&mut Self` reference for FsLister.
unsafe impl<S: Service> Sync for FlatLister<S> {}

impl<S: Service> FlatLister<S> {
    /// Create a new flat lister
    pub fn new(service: Arc<S>, ctx: OperationContext, path: &str) -> FlatLister<S> {
        FlatLister {
            service,
            ctx,
            next_dir: Some(NextDir::Directory(oio::Entry::new(
                path,
                MetadataBuilder::dir().build(),
            ))),
            active_lister: vec![],
        }
    }
}

impl<S: Service> oio::List for FlatLister<S> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            if matches!(self.next_dir.as_ref(), Some(NextDir::Directory(_))) {
                let Some(NextDir::Directory(de)) = self.next_dir.take() else {
                    unreachable!("next_dir must contain a directory")
                };
                let lister = match self
                    .service
                    .as_ref()
                    .list(&self.ctx, de.path(), OpList::new())
                {
                    Ok(v) => v,
                    Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                        // Skip directories that we don't have permission to access
                        // and continue with the rest of the listing.
                        log::warn!(
                            "FlatLister skipping directory due to permission denied: {}",
                            de.path()
                        );
                        continue;
                    }
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        // Skip directories that are deleted while listing.
                        log::warn!(
                            "FlatLister skipping directory due to not found during listing: {}",
                            de.path()
                        );
                        continue;
                    }
                    Err(e) => {
                        self.next_dir = Some(NextDir::Directory(de));
                        return Err(e);
                    }
                };
                self.next_dir = Some(NextDir::Listing(de, lister));
            }

            if matches!(self.next_dir.as_ref(), Some(NextDir::Listing(..))) {
                let first = {
                    let Some(NextDir::Listing(de, lister)) = self.next_dir.as_mut() else {
                        unreachable!("next_dir must contain a lister")
                    };
                    loop {
                        match lister.next().await {
                            Ok(v) => break Ok(v),
                            Err(e) if e.kind() == ErrorKind::NotFound => {
                                // Skip entries that are deleted during listing.
                                log::warn!(
                                    "FlatLister skipping entry due to not found during listing: {}",
                                    de.path()
                                );
                            }
                            Err(e) => break Err(e),
                        }
                    }
                };

                let Some(NextDir::Listing(de, lister)) = self.next_dir.take() else {
                    unreachable!("next_dir must contain a lister")
                };
                let first = match first {
                    Ok(v) => v,
                    Err(e) => {
                        // No entry has been yielded from this lister, so restart
                        // the directory when the caller retries after an error.
                        self.next_dir = Some(NextDir::Directory(de));
                        return Err(e);
                    }
                };

                if let Some(v) = first {
                    self.active_lister.push((de.clone(), lister));

                    if v.mode().is_dir() {
                        // should not loop itself again
                        if v.path() != de.path() {
                            self.next_dir = Some(NextDir::Directory(v));
                            continue;
                        }
                    } else {
                        return Ok(Some(v));
                    }
                }
            }

            let (de, lister) = match self.active_lister.last_mut() {
                Some((de, lister)) => (de, lister),
                None => return Ok(None),
            };

            match lister.next().await {
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    log::warn!(
                        "FlatLister skipping entry due to not found during recursive listing: {}",
                        de.path()
                    );
                    continue;
                }
                Err(e) => return Err(e),
                Ok(Some(v)) if v.mode().is_dir() => {
                    // should not loop itself again
                    if v.path() != de.path() {
                        self.next_dir = Some(NextDir::Directory(v));
                        continue;
                    }
                }
                Ok(Some(v)) => return Ok(Some(v)),
                Ok(None) => {
                    let (de, _) = self
                        .active_lister
                        .pop()
                        .expect("the exhausted lister must still be active");
                    return Ok(Some(de));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::{Future, poll_fn};
    use std::sync::Mutex;
    use std::task::Poll;
    use std::vec::IntoIter;

    use super::*;

    #[derive(Debug, Default)]
    struct TreeState {
        tree: HashMap<String, Vec<(String, bool)>>,
        pending_once_on: Option<(String, usize)>,
        fail_first_list_on: Option<String>,
        fail_first_next_on: Option<String>,
        pending_injected: Mutex<bool>,
        list_injected: Mutex<bool>,
        next_injected: Mutex<bool>,
        listed: Mutex<Vec<String>>,
    }

    impl TreeState {
        fn take_fault(&self, armed: Option<&str>, used: &Mutex<bool>, path: &str) -> bool {
            if armed != Some(path) {
                return false;
            }
            let mut used = used.lock().expect("mutex must not poison");
            let first_time = !*used;
            *used = true;
            first_time
        }
    }

    #[derive(Debug)]
    struct MockService {
        state: Arc<TreeState>,
    }

    struct TreeLister {
        state: Arc<TreeState>,
        path: String,
        entries: IntoIter<(String, bool)>,
        calls: usize,
        fail_first: bool,
    }

    impl TreeLister {
        fn new(state: Arc<TreeState>, path: &str) -> Self {
            let entries = state
                .tree
                .get(path)
                .cloned()
                .unwrap_or_default()
                .into_iter();
            let fail_first = state.take_fault(
                state.fail_first_next_on.as_deref(),
                &state.next_injected,
                path,
            );
            Self {
                state,
                path: path.to_string(),
                entries,
                calls: 0,
                fail_first,
            }
        }

        fn next_ready(&mut self) -> Result<Option<oio::Entry>> {
            if self.fail_first {
                self.fail_first = false;
                return Err(
                    Error::new(ErrorKind::Unexpected, "injected listing failure").set_temporary(),
                );
            }

            Ok(self.entries.next().map(|(path, is_dir)| {
                let metadata = if is_dir {
                    MetadataBuilder::dir().build()
                } else {
                    MetadataBuilder::file(0).build()
                };
                oio::Entry::new(&path, metadata)
            }))
        }
    }

    impl oio::List for TreeLister {
        async fn next(&mut self) -> Result<Option<oio::Entry>> {
            self.calls += 1;
            let mut pending = match &self.state.pending_once_on {
                Some((path, call)) if path == &self.path && *call == self.calls => self
                    .state
                    .take_fault(Some(path), &self.state.pending_injected, &self.path),
                _ => false,
            };

            poll_fn(move |cx| {
                if pending {
                    pending = false;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(self.next_ready())
                }
            })
            .await
        }
    }

    impl Service for MockService {
        type Reader = ();
        type Writer = ();
        type Lister = TreeLister;
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

        fn list(&self, _: &OperationContext, path: &str, _: OpList) -> Result<Self::Lister> {
            self.state
                .listed
                .lock()
                .expect("mutex must not poison")
                .push(path.to_string());
            if self.state.take_fault(
                self.state.fail_first_list_on.as_deref(),
                &self.state.list_injected,
                path,
            ) {
                return Err(
                    Error::new(ErrorKind::Unexpected, "injected list failure").set_temporary()
                );
            }
            Ok(TreeLister::new(self.state.clone(), path))
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

    fn test_tree() -> HashMap<String, Vec<(String, bool)>> {
        HashMap::from([
            ("/".to_string(), vec![("a/".to_string(), true)]),
            (
                "a/".to_string(),
                vec![("a/file".to_string(), false), ("a/sub/".to_string(), true)],
            ),
            (
                "a/sub/".to_string(),
                vec![("a/sub/file2".to_string(), false)],
            ),
        ])
    }

    fn new_lister(state: &Arc<TreeState>) -> FlatLister<MockService> {
        FlatLister::new(
            Arc::new(MockService {
                state: state.clone(),
            }),
            OperationContext::new(),
            "/",
        )
    }

    async fn cancel_next(lister: &mut FlatLister<MockService>) {
        let mut next = Box::pin(oio::List::next(lister));
        poll_fn(|cx| {
            assert!(
                next.as_mut().poll(cx).is_pending(),
                "the injected pending point must be reached"
            );
            Poll::Ready(())
        })
        .await;
        drop(next);
    }

    async fn drain(lister: &mut FlatLister<MockService>) -> Vec<String> {
        let mut paths = Vec::new();
        while let Some(entry) = oio::List::next(lister).await.expect("listing must succeed") {
            paths.push(entry.path().to_string());
        }
        paths
    }

    async fn drain_with_retry(lister: &mut FlatLister<MockService>) -> (Vec<String>, usize) {
        let mut paths = Vec::new();
        let mut errors = 0;
        loop {
            match oio::List::next(lister).await {
                Ok(Some(entry)) => paths.push(entry.path().to_string()),
                Ok(None) => break,
                Err(err) => {
                    assert!(err.is_temporary(), "injected error is temporary: {err}");
                    errors += 1;
                    assert_eq!(errors, 1, "the one-shot fault must not repeat");
                }
            }
        }
        (paths, errors)
    }

    fn assert_complete_tree(paths: Vec<String>) {
        assert_eq!(
            paths,
            ["a/file", "a/sub/file2", "a/sub/", "a/", "/"],
            "cancellation or retry must preserve every entry and the bottom-up order"
        );
    }

    #[tokio::test]
    async fn flat_lister_preserves_a_new_directory_when_cancelled() {
        let state = Arc::new(TreeState {
            tree: test_tree(),
            pending_once_on: Some(("a/".to_string(), 1)),
            ..Default::default()
        });
        let mut lister = new_lister(&state);

        cancel_next(&mut lister).await;
        assert_complete_tree(drain(&mut lister).await);
    }

    #[tokio::test]
    async fn flat_lister_preserves_an_active_directory_when_cancelled() {
        let state = Arc::new(TreeState {
            tree: test_tree(),
            pending_once_on: Some(("a/sub/".to_string(), 2)),
            ..Default::default()
        });
        let mut lister = new_lister(&state);
        let mut paths = Vec::new();
        while paths.last().map(String::as_str) != Some("a/sub/file2") {
            let entry = oio::List::next(&mut lister)
                .await
                .expect("listing must succeed")
                .expect("the nested file must be reached");
            paths.push(entry.path().to_string());
        }

        cancel_next(&mut lister).await;
        paths.extend(drain(&mut lister).await);
        assert_complete_tree(paths);
    }

    async fn assert_temporary_fault_is_retried(state: Arc<TreeState>) {
        let mut lister = new_lister(&state);
        let (paths, errors) = drain_with_retry(&mut lister).await;

        assert_eq!(errors, 1);
        assert_complete_tree(paths);
        let listed = state.listed.lock().expect("mutex must not poison");
        assert_eq!(
            listed.iter().filter(|path| path.as_str() == "a/").count(),
            2,
            "a temporary failure before the first entry must restart the directory: {listed:?}"
        );
    }

    #[tokio::test]
    async fn flat_lister_retries_a_temporary_list_error() {
        assert_temporary_fault_is_retried(Arc::new(TreeState {
            tree: test_tree(),
            fail_first_list_on: Some("a/".to_string()),
            ..Default::default()
        }))
        .await;
    }

    #[tokio::test]
    async fn flat_lister_retries_a_temporary_first_next_error() {
        assert_temporary_fault_is_retried(Arc::new(TreeState {
            tree: test_tree(),
            fail_first_next_on: Some("a/".to_string()),
            ..Default::default()
        }))
        .await;
    }
}
