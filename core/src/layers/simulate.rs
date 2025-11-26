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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::raw::oio::FlatLister;
use crate::raw::oio::PrefixLister;
use crate::raw::*;
use crate::*;

/// Simulate missing capabilities for backends in a configurable way.
#[derive(Debug, Clone)]
pub struct SimulateLayer {
    list_recursive: bool,
    list_start_after: bool,
    stat_dir: bool,
    create_dir: bool,
}

impl Default for SimulateLayer {
    fn default() -> Self {
        Self {
            list_recursive: true,
            list_start_after: true,
            stat_dir: true,
            create_dir: true,
        }
    }
}

impl SimulateLayer {
    /// Enable or disable recursive list simulation. Default: true.
    pub fn with_list_recursive(mut self, enabled: bool) -> Self {
        self.list_recursive = enabled;
        self
    }

    /// Enable or disable start_after simulation. Default: true.
    pub fn with_list_start_after(mut self, enabled: bool) -> Self {
        self.list_start_after = enabled;
        self
    }

    /// Enable or disable stat dir simulation. Default: true.
    pub fn with_stat_dir(mut self, enabled: bool) -> Self {
        self.stat_dir = enabled;
        self
    }

    /// Enable or disable create_dir simulation. Default: true.
    pub fn with_create_dir(mut self, enabled: bool) -> Self {
        self.create_dir = enabled;
        self
    }
}

impl<A: Access> Layer<A> for SimulateLayer {
    type LayeredAccess = SimulateAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();
        info.update_full_capability(|mut cap| {
            if self.list_start_after && cap.list {
                cap.list_with_start_after = true;
            }
            if self.list_recursive && cap.list {
                cap.list_with_recursive = true;
            }
            if self.create_dir && cap.list && cap.write_can_empty {
                cap.create_dir = true;
            }
            cap
        });

        SimulateAccessor {
            config: self.clone(),
            info,
            inner: Arc::new(inner),
        }
    }
}

/// Accessor that applies capability simulation.
pub struct SimulateAccessor<A: Access> {
    config: SimulateLayer,
    info: Arc<AccessorInfo>,
    inner: Arc<A>,
}

impl<A: Access> Debug for SimulateAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<A: Access> SimulateAccessor<A> {
    async fn simulate_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let capability = self.info.native_capability();

        if capability.create_dir || !self.config.create_dir {
            return self.inner().create_dir(path, args).await;
        }

        if capability.write_can_empty && capability.list {
            let (_, mut w) = self.inner.write(path, OpWrite::default()).await?;
            oio::Write::close(&mut w).await?;
            return Ok(RpCreateDir::default());
        }

        self.inner.create_dir(path, args).await
    }

    async fn simulate_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let capability = self.info.native_capability();

        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        if path.ends_with('/') {
            if capability.create_dir {
                let meta = self.inner.stat(path, args.clone()).await?.into_metadata();

                if meta.is_file() {
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        "stat expected a directory, but found a file",
                    ));
                }

                return Ok(RpStat::new(meta));
            }

            if self.config.stat_dir {
                let (_, mut l) = self
                    .inner
                    .list(path, OpList::default().with_recursive(true).with_limit(1))
                    .await?;

                return if oio::List::next(&mut l).await?.is_some() {
                    Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
                } else {
                    Err(Error::new(
                        ErrorKind::NotFound,
                        "the directory is not found",
                    ))
                };
            }
        }

        self.inner.stat(path, args).await
    }

    async fn simulate_list(
        &self,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, StartAfterLister<SimulateLister<A, A::Lister>>)> {
        let cap = self.info.native_capability();

        let recursive = args.recursive();
        let start_after = args.start_after().map(|v| v.to_string());
        let simulate_start_after =
            start_after.is_some() && self.config.list_start_after && !cap.list_with_start_after;

        let mut forward = OpList::new().with_recursive(recursive);
        if let Some(limit) = args.limit() {
            forward = forward.with_limit(limit);
        }
        if args.versions() {
            forward = forward.with_versions(true);
        }
        if args.deleted() {
            forward = forward.with_deleted(true);
        }
        if !simulate_start_after {
            if let Some(v) = args.start_after() {
                forward = forward.with_start_after(v);
            }
        }

        let (rp, lister) = match (
            recursive,
            cap.list_with_recursive,
            self.config.list_recursive,
        ) {
            // Backend supports recursive list, forward directly.
            (_, true, _) => {
                let (rp, p) = self.inner.list(path, forward).await?;
                (rp, SimulateLister::One(p))
            }
            // Simulate recursive via flat list when enabled.
            (true, false, true) => {
                if path.ends_with('/') {
                    let p = FlatLister::new(self.inner.clone(), path);
                    (RpList::default(), SimulateLister::Two(p))
                } else {
                    let parent = get_parent(path);
                    let p = FlatLister::new(self.inner.clone(), parent);
                    let p = PrefixLister::new(p, path);
                    (RpList::default(), SimulateLister::Four(p))
                }
            }
            // Recursive requested but simulation disabled; rely on backend and propagate errors.
            (true, false, false) => {
                let (rp, p) = self.inner.list(path, forward).await?;
                (rp, SimulateLister::One(p))
            }
            // Non-recursive list: keep existing prefix handling semantics.
            (false, false, _) => {
                if path.ends_with('/') {
                    let (rp, p) = self.inner.list(path, forward).await?;
                    (rp, SimulateLister::One(p))
                } else {
                    let parent = get_parent(path);
                    let (rp, p) = self.inner.list(parent, forward).await?;
                    let p = PrefixLister::new(p, path);
                    (rp, SimulateLister::Three(p))
                }
            }
        };

        let lister = StartAfterLister::new(
            lister,
            if simulate_start_after {
                start_after
            } else {
                None
            },
        );

        Ok((rp, lister))
    }
}

impl<A: Access> LayeredAccess for SimulateAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = StartAfterLister<SimulateLister<A, A::Lister>>;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.simulate_create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.simulate_stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner().delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.simulate_list(path, args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).await
    }
}

pub type SimulateLister<A, P> =
    FourWays<P, FlatLister<Arc<A>, P>, PrefixLister<P>, PrefixLister<FlatLister<Arc<A>, P>>>;

pub struct StartAfterLister<L> {
    inner: L,
    start_after: Option<String>,
}

impl<L> StartAfterLister<L> {
    pub fn new(inner: L, start_after: Option<String>) -> Self {
        Self { inner, start_after }
    }
}

impl<L: oio::List> oio::List for StartAfterLister<L> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            let Some(entry) = self.inner.next().await? else {
                return Ok(None);
            };

            if let Some(start_after) = self.start_after.as_deref() {
                if entry.path() <= start_after {
                    continue;
                }
            }

            return Ok(Some(entry));
        }
    }
}
