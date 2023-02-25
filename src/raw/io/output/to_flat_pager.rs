// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::mem;

use async_trait::async_trait;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// to_flat_pager is used to make a hierarchy pager flat.
pub fn to_flat_pager<A: Accessor, P>(acc: A, path: &str, size: usize) -> ToFlatPager<A, P> {
    #[cfg(debug_assertions)]
    {
        let meta = acc.metadata();
        debug_assert!(
            !meta.capabilities().contains(AccessorCapability::Scan),
            "service already supports scan, call to_flat_pager must be a mistake"
        );
        debug_assert!(
            meta.capabilities().contains(AccessorCapability::List),
            "service doesn't support list hierarchy, it must be a bug"
        );
    }

    ToFlatPager {
        acc,
        size,
        dirs: VecDeque::from([output::Entry::new(
            path,
            ObjectMetadata::new(ObjectMode::DIR),
        )]),
        pagers: vec![],
        res: Vec::with_capacity(size),
    }
}

/// ToFlatPager will walk dir in bottom up way:
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
/// ToFlatPager will output entries like:
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
/// Especially, for storage services that can't return dirs first, ToFlatPager
/// may output parent dirs' files before nested dirs, this is expected because files
/// always output directly while listing.
pub struct ToFlatPager<A: Accessor, P> {
    acc: A,
    size: usize,
    dirs: VecDeque<output::Entry>,
    pagers: Vec<(P, output::Entry, Vec<output::Entry>)>,
    res: Vec<output::Entry>,
}

#[async_trait]
impl<A, P> output::Page for ToFlatPager<A, P>
where
    A: Accessor<Pager = P>,
    P: output::Page,
{
    async fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        loop {
            if let Some(de) = self.dirs.pop_back() {
                let (_, op) = self.acc.list(de.path(), OpList::new()).await?;
                self.pagers.push((op, de, vec![]))
            }

            let (mut pager, de, mut buf) = match self.pagers.pop() {
                Some((pager, de, buf)) => (pager, de, buf),
                None => {
                    if !self.res.is_empty() {
                        return Ok(Some(mem::take(&mut self.res)));
                    }
                    return Ok(None);
                }
            };

            if buf.is_empty() {
                match pager.next_page().await? {
                    Some(v) => {
                        buf = v;
                    }
                    None => {
                        self.res.push(de);
                        continue;
                    }
                }
            }

            let mut buf = VecDeque::from(buf);
            loop {
                if let Some(oe) = buf.pop_front() {
                    if oe.mode().is_dir() {
                        self.dirs.push_back(oe);
                        self.pagers.push((pager, de, buf.into()));
                        break;
                    } else {
                        self.res.push(oe)
                    }
                } else {
                    self.pagers.push((pager, de, vec![]));
                    break;
                }
            }

            if self.res.len() >= self.size {
                return Ok(Some(mem::take(&mut self.res)));
            }
        }
    }
}

impl<A, P> output::BlockingPage for ToFlatPager<A, P>
where
    A: Accessor<BlockingPager = P>,
    P: output::BlockingPage,
{
    fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        loop {
            if let Some(de) = self.dirs.pop_back() {
                let (_, op) = self.acc.blocking_list(de.path(), OpList::new())?;
                self.pagers.push((op, de, vec![]))
            }

            let (mut pager, de, mut buf) = match self.pagers.pop() {
                Some((pager, de, buf)) => (pager, de, buf),
                None => {
                    if !self.res.is_empty() {
                        return Ok(Some(mem::take(&mut self.res)));
                    }
                    return Ok(None);
                }
            };

            if buf.is_empty() {
                match pager.next_page()? {
                    Some(v) => {
                        buf = v;
                    }
                    None => {
                        self.res.push(de);
                        continue;
                    }
                }
            }

            let mut buf = VecDeque::from(buf);
            loop {
                if let Some(oe) = buf.pop_front() {
                    if oe.mode().is_dir() {
                        self.dirs.push_back(oe);
                        self.pagers.push((pager, de, buf.into()));
                        break;
                    } else {
                        self.res.push(oe)
                    }
                } else {
                    self.pagers.push((pager, de, vec![]));
                    break;
                }
            }

            if self.res.len() >= self.size {
                return Ok(Some(mem::take(&mut self.res)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::vec;

    use io::output::BlockingPage;
    use log::debug;

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

        fn get(&self, path: &str) -> MockPager {
            let inner = self.map.get(path).expect("must have value").to_vec();

            MockPager { inner, done: false }
        }
    }

    #[async_trait]
    impl Accessor for MockService {
        type Reader = ();
        type BlockingReader = ();
        type Pager = ();
        type BlockingPager = MockPager;

        fn metadata(&self) -> AccessorMetadata {
            let mut am = AccessorMetadata::default();
            am.set_capabilities(AccessorCapability::List);

            am
        }

        fn blocking_list(&self, path: &str, _: OpList) -> Result<(RpList, Self::BlockingPager)> {
            debug!("visit path: {path}");
            Ok((RpList::default(), self.get(path)))
        }
    }

    struct MockPager {
        inner: Vec<&'static str>,
        done: bool,
    }

    impl BlockingPage for MockPager {
        fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
            if self.done {
                return Ok(None);
            }
            self.done = true;

            let entries = self
                .inner
                .iter()
                .map(|path| {
                    if path.ends_with('/') {
                        output::Entry::new(path, ObjectMetadata::new(ObjectMode::DIR))
                    } else {
                        output::Entry::new(path, ObjectMetadata::new(ObjectMode::FILE))
                    }
                })
                .collect();

            Ok(Some(entries))
        }
    }

    #[test]
    fn test_blocking_list() -> Result<()> {
        let _ = env_logger::try_init();

        let acc = MockService::new();
        let mut pager = to_flat_pager(acc, "x/", 10);

        let mut entries = Vec::default();

        while let Some(e) = pager.next_page()? {
            entries.extend_from_slice(&e)
        }

        assert_eq!(
            entries[0],
            output::Entry::new("x/x/x/x", ObjectMetadata::new(ObjectMode::FILE))
        );

        Ok(())
    }
}
