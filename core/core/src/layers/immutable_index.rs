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

use std::collections::HashSet;
use std::fmt::Debug;
use std::vec::IntoIter;

use crate::raw::*;
use crate::*;

/// Add an immutable in-memory index for underlying storage services.
///
/// Especially useful for services without list capability like HTTP.
///
/// # Examples
///
/// ```rust, no_run
/// # use std::collections::HashMap;
///
/// # use opendal_core::layers::ImmutableIndexLayer;
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
///
/// # fn main() -> Result<()> {
/// let mut iil = ImmutableIndexLayer::default();
///
/// for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
///     iil.insert(i.to_string())
/// }
///
/// let op = Operator::from_iter::<services::Memory>(HashMap::<_, _>::default())?
///     .layer(iil)
///     .finish();
/// Ok(())
/// # }
/// ```
#[derive(Default, Debug, Clone)]
pub struct ImmutableIndexLayer {
    vec: Vec<String>,
}

impl ImmutableIndexLayer {
    /// Insert a key into index.
    pub fn insert(&mut self, key: String) {
        self.vec.push(key);
    }

    /// Insert keys from iter.
    pub fn extend_iter<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = String>,
    {
        self.vec.extend(iter);
    }
}

impl<A: Access> Layer<A> for ImmutableIndexLayer {
    type LayeredAccess = ImmutableIndexAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();
        info.update_full_capability(|mut cap| {
            cap.list = true;
            cap.list_with_recursive = true;
            cap
        });

        ImmutableIndexAccessor {
            vec: self.vec.clone(),
            inner,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ImmutableIndexAccessor<A: Access> {
    inner: A,
    vec: Vec<String>,
}

impl<A: Access> ImmutableIndexAccessor<A> {
    fn children_flat(&self, path: &str) -> Vec<String> {
        self.vec
            .iter()
            .filter(|v| v.starts_with(path) && v.as_str() != path)
            .cloned()
            .collect()
    }

    fn children_hierarchy(&self, path: &str) -> Vec<String> {
        let mut res = HashSet::new();

        for i in self.vec.iter() {
            // `/xyz` should not belong to `/abc`
            if !i.starts_with(path) {
                continue;
            }

            // remove `/abc` if self
            if i == path {
                continue;
            }

            match i[path.len()..].find('/') {
                // File `/abc/def.csv` must belong to `/abc`
                None => {
                    res.insert(i.to_string());
                }
                Some(idx) => {
                    // The index of first `/` after `/abc`.
                    let dir_idx = idx + 1 + path.len();

                    if dir_idx == i.len() {
                        // Dir `/abc/def/` belongs to `/abc/`
                        res.insert(i.to_string());
                    } else {
                        // File/Dir `/abc/def/xyz` doesn't belong to `/abc`.
                        // But we need to list `/abc/def` out so that we can walk down.
                        res.insert(i[..dir_idx].to_string());
                    }
                }
            }
        }

        res.into_iter().collect()
    }
}

impl<A: Access> LayeredAccess for ImmutableIndexAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = ImmutableDir;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let mut path = path;
        if path == "/" {
            path = ""
        }

        let idx = if args.recursive() {
            self.children_flat(path)
        } else {
            self.children_hierarchy(path)
        };

        Ok((RpList::default(), ImmutableDir::new(idx)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }
}

pub struct ImmutableDir {
    idx: IntoIter<String>,
}

impl ImmutableDir {
    fn new(idx: Vec<String>) -> Self {
        Self {
            idx: idx.into_iter(),
        }
    }

    fn inner_next(&mut self) -> Option<oio::Entry> {
        self.idx.next().map(|v| {
            let mode = if v.ends_with('/') {
                EntryMode::DIR
            } else {
                EntryMode::FILE
            };
            let meta = Metadata::new(mode);
            oio::Entry::with(v, meta)
        })
    }
}

impl oio::List for ImmutableDir {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        Ok(self.inner_next())
    }
}

#[cfg(test)]
// Note: services-http feature is now in opendal facade crate, not opendal-core
// These tests are disabled and marked with #[ignore]
// TODO: Re-enable when we can access http service through facade in core tests
#[allow(unused_imports)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use anyhow::Result;
    use futures::TryStreamExt;
    use log::debug;

    use super::*;
    use crate::EntryMode;
    use crate::Operator;
    use crate::layers::LoggingLayer;
    // HttpConfig is now in opendal-service-http crate, but tests in core
    // can't access it directly. These tests are disabled until we can
    // access http service through facade or move tests to appropriate location.
    // use crate::services::HttpConfig;

    #[tokio::test]
    #[ignore = "HttpConfig is now in opendal-service-http crate, needs facade access"]
    async fn test_list() -> Result<()> {
        // HttpConfig is now in opendal-service-http crate, but tests in core
        // can't access it directly. These tests are disabled until we can
        // access http service through facade or move tests to appropriate location.
        todo!("HttpConfig is now in opendal-service-http crate, needs facade access");
    }

    #[tokio::test]
    #[ignore = "HttpConfig is now in opendal-service-http crate, needs facade access"]
    async fn test_scan() -> Result<()> {
        // HttpConfig is now in opendal-service-http crate, but tests in core
        // can't access it directly. These tests are disabled until we can
        // access http service through facade or move tests to appropriate location.
        todo!("HttpConfig is now in opendal-service-http crate, needs facade access");
    }

    #[tokio::test]
    #[ignore = "HttpConfig is now in opendal-service-http crate, needs facade access"]
    async fn test_list_dir() -> Result<()> {
        // HttpConfig is now in opendal-service-http crate, but tests in core
        // can't access it directly. These tests are disabled until we can
        // access http service through facade or move tests to appropriate location.
        todo!("HttpConfig is now in opendal-service-http crate, needs facade access");
    }

    #[tokio::test]
    #[ignore = "HttpConfig is now in opendal-service-http crate, needs facade access"]
    async fn test_walk_top_down_dir() -> Result<()> {
        // HttpConfig is now in opendal-service-http crate, but tests in core
        // can't access it directly. These tests are disabled until we can
        // access http service through facade or move tests to appropriate location.
        todo!("HttpConfig is now in opendal-service-http crate, needs facade access");
    }
}
