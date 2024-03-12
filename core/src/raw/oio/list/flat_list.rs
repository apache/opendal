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

use std::task::ready;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;

use crate::raw::*;
use crate::*;

/// ListFuture is the future returned while calling async list.
type ListFuture<A, L> = BoxedStaticFuture<(A, oio::Entry, Result<(RpList, L)>)>;

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
pub struct FlatLister<A: Accessor, L> {
    acc: Option<A>,
    root: String,

    next_dir: Option<oio::Entry>,
    active_lister: Vec<(Option<oio::Entry>, L)>,
    list_future: Option<ListFuture<A, L>>,
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this FlatLister.
unsafe impl<A: Accessor, L> Send for FlatLister<A, L> {}
/// # Safety
///
/// We will only take `&mut Self` reference for FsLister.
unsafe impl<A: Accessor, L> Sync for FlatLister<A, L> {}

impl<A, L> FlatLister<A, L>
where
    A: Accessor,
{
    /// Create a new flat lister
    pub fn new(acc: A, path: &str) -> FlatLister<A, L> {
        FlatLister {
            acc: Some(acc),
            root: path.to_string(),
            next_dir: Some(oio::Entry::new(path, Metadata::new(EntryMode::DIR))),
            active_lister: vec![],
            list_future: None,
        }
    }
}

impl<A, L> oio::List for FlatLister<A, L>
where
    A: Accessor<Lister = L>,
    L: oio::List,
{
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<oio::Entry>>> {
        loop {
            if let Some(fut) = self.list_future.as_mut() {
                let (acc, de, res) = ready!(fut.poll_unpin(cx));
                self.acc = Some(acc);
                self.list_future = None;

                let (_, l) = res?;
                self.active_lister.push((Some(de), l))
            }

            if let Some(de) = self.next_dir.take() {
                let acc = self.acc.take().expect("Accessor must be valid");
                let fut = async move {
                    let res = acc.list(de.path(), OpList::new()).await;
                    (acc, de, res)
                };
                self.list_future = Some(Box::pin(fut));
                continue;
            }

            let (de, lister) = match self.active_lister.last_mut() {
                Some((de, lister)) => (de, lister),
                None => return Poll::Ready(Ok(None)),
            };

            match ready!(lister.poll_next(cx))? {
                Some(v) if v.mode().is_dir() => {
                    self.next_dir = Some(v);
                    continue;
                }
                Some(v) => return Poll::Ready(Ok(Some(v))),
                None => {
                    match de.take() {
                        Some(de) => {
                            // Only push entry if it's not root dir
                            if de.path() != self.root {
                                return Poll::Ready(Ok(Some(de)));
                            }
                            continue;
                        }
                        None => {
                            let _ = self.active_lister.pop();
                            continue;
                        }
                    }
                }
            }
        }
    }
}

impl<A, P> oio::BlockingList for FlatLister<A, P>
where
    A: Accessor<BlockingLister = P>,
    P: oio::BlockingList,
{
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            if let Some(de) = self.next_dir.take() {
                let acc = self.acc.take().expect("Accessor must be valid");
                let (_, l) = acc.blocking_list(de.path(), OpList::new())?;

                self.acc = Some(acc);
                self.active_lister.push((Some(de), l))
            }

            let (de, lister) = match self.active_lister.last_mut() {
                Some((de, lister)) => (de, lister),
                None => return Ok(None),
            };

            match lister.next()? {
                Some(v) if v.mode().is_dir() => {
                    self.next_dir = Some(v);
                    continue;
                }
                Some(v) => return Ok(Some(v)),
                None => {
                    match de.take() {
                        Some(de) => {
                            // Only push entry if it's not root dir
                            if de.path() != self.root {
                                return Ok(Some(de));
                            }
                            continue;
                        }
                        None => {
                            let _ = self.active_lister.pop();
                            continue;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::vec;
    use std::vec::IntoIter;

    use async_trait::async_trait;
    use log::debug;
    use oio::BlockingList;

    use super::*;

    #[derive(Debug)]
    struct MockService {
        map: HashMap<&'static str, Vec<&'static str>>,
    }

    impl MockService {
        fn new() -> Self {
            let mut map = HashMap::default();
            map.insert("x/", vec!["x/x/"]);
            map.insert("x/x/", vec!["x/x/x/"]);
            map.insert("x/x/x/", vec!["x/x/x/x"]);

            Self { map }
        }

        fn get(&self, path: &str) -> MockLister {
            let inner = self.map.get(path).expect("must have value").to_vec();

            MockLister {
                inner: inner.into_iter(),
            }
        }
    }

    #[async_trait]
    impl Accessor for MockService {
        type Reader = ();
        type BlockingReader = ();
        type Writer = ();
        type BlockingWriter = ();
        type Lister = ();
        type BlockingLister = MockLister;

        fn info(&self) -> AccessorInfo {
            let mut am = AccessorInfo::default();
            am.full_capability_mut().list = true;

            am
        }

        fn blocking_list(&self, path: &str, _: OpList) -> Result<(RpList, Self::BlockingLister)> {
            debug!("visit path: {path}");
            Ok((RpList::default(), self.get(path)))
        }
    }

    struct MockLister {
        inner: IntoIter<&'static str>,
    }

    impl BlockingList for MockLister {
        fn next(&mut self) -> Result<Option<oio::Entry>> {
            Ok(self.inner.next().map(|path| {
                if path.ends_with('/') {
                    oio::Entry::new(path, Metadata::new(EntryMode::DIR))
                } else {
                    oio::Entry::new(path, Metadata::new(EntryMode::FILE))
                }
            }))
        }
    }

    #[test]
    fn test_blocking_list() -> Result<()> {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let acc = MockService::new();
        let mut lister = FlatLister::new(acc, "x/");

        let mut entries = Vec::default();

        while let Some(e) = lister.next()? {
            entries.push(e)
        }

        assert_eq!(
            entries[0],
            oio::Entry::new("x/x/x/x", Metadata::new(EntryMode::FILE))
        );

        Ok(())
    }
}
