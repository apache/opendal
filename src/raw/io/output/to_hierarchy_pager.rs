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

use std::collections::HashSet;

use async_trait::async_trait;

use crate::raw::*;
use crate::*;

/// to_hierarchy_pager is used to make a hierarchy pager flat.
pub fn to_hierarchy_pager<P>(pager: P, path: &str) -> ToHierarchyPager<P> {
    let path = if path == "/" {
        "".to_string()
    } else {
        path.to_string()
    };

    ToHierarchyPager {
        pager,
        path,
        visited: HashSet::default(),
    }
}

/// ToHierarchyPager will convert a flat page to hierarchy by filter
/// not needed entries.
///
/// # Notes
///
/// ToHierarchyPager filter entries after fecth entries. So it's possible
/// to return an empty vec. It doesn't mean the all pages have been
/// returned.
///
/// Please keep calling next_page until we returned `Ok(None)`
pub struct ToHierarchyPager<P> {
    pager: P,
    path: String,
    visited: HashSet<String>,
}

impl<P> ToHierarchyPager<P> {
    /// TODO: use retain_mut instead after we bump MSRV to 1.61.
    fn filter_entries(&mut self, entries: Vec<output::Entry>) -> Vec<output::Entry> {
        entries
            .into_iter()
            .filter_map(|mut e| {
                // If path is not started with prefix, drop it.
                //
                // Idealy, it should never happen. But we just tolerate
                // this state.
                if !e.path().starts_with(&self.path) {
                    return None;
                }

                // Dir itself should not be returned in hierarchy page.
                if e.path() == self.path {
                    return None;
                }

                let prefix_len = self.path.len();

                let idx = if let Some(idx) = e.path()[prefix_len..].find('/') {
                    idx + prefix_len + 1
                } else {
                    // If there is no `/` in path, it's a normal file, we
                    // can return it directly.
                    return Some(e);
                };

                // idx == path.len() means it's contain only one `/` at the
                // end of path.
                if idx == e.path().len() {
                    if !self.visited.contains(e.path()) {
                        self.visited.insert(e.path().to_string());
                    }
                    return Some(e);
                }

                // If idx < path.len() means that are more levels to come.
                // We should check the first dir path.
                let has = {
                    let path = &e.path()[..idx];
                    self.visited.contains(path)
                };
                if !has {
                    let path = {
                        let path = &e.path()[..idx];
                        path.to_string()
                    };

                    e.set_path(&path);
                    e.set_mode(ObjectMode::DIR);
                    self.visited.insert(path);

                    return Some(e);
                }

                None
            })
            .collect()
    }
}

#[async_trait]
impl<P: output::Page> output::Page for ToHierarchyPager<P> {
    async fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        let page = self.pager.next_page().await?;

        let entries = if let Some(entries) = page {
            entries
        } else {
            return Ok(None);
        };

        let entries = self.filter_entries(entries);

        Ok(Some(entries))
    }
}

impl<P: output::BlockingPage> output::BlockingPage for ToHierarchyPager<P> {
    fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        let page = self.pager.next_page()?;

        let entries = if let Some(entries) = page {
            entries
        } else {
            return Ok(None);
        };

        let entries = self.filter_entries(entries);

        Ok(Some(entries))
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use io::output::BlockingPage;
    use log::debug;

    use super::*;

    struct MockPager {
        inner: Vec<&'static str>,
        done: bool,
    }

    impl MockPager {
        fn new(inner: &[&'static str]) -> Self {
            Self {
                inner: inner.to_vec(),
                done: false,
            }
        }
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

        let pager = MockPager::new(&["x/x/", "x/y/", "y/", "x/x/x", "y/y", "xy/", "z", "y/a"]);
        let mut pager = to_hierarchy_pager(pager, "");

        let mut entries = Vec::default();

        let mut set = HashSet::new();
        while let Some(e) = pager.next_page()? {
            for i in &e {
                debug!("got path {}", i.path());
                assert!(
                    set.insert(i.path().to_string()),
                    "duplicated value: {}",
                    i.path()
                );
            }
            entries.extend_from_slice(&e)
        }

        assert_eq!(
            entries[0],
            output::Entry::new("x/", ObjectMetadata::new(ObjectMode::DIR))
        );
        assert_eq!(
            entries[1],
            output::Entry::new("y/", ObjectMetadata::new(ObjectMode::DIR))
        );
        assert_eq!(
            entries[2],
            output::Entry::new("xy/", ObjectMetadata::new(ObjectMode::DIR))
        );
        assert_eq!(
            entries[3],
            output::Entry::new("z", ObjectMetadata::new(ObjectMode::FILE))
        );

        Ok(())
    }
}
