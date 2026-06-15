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

use crate::raw::oio::Delete;
use crate::raw::oio::List;
use crate::raw::oio::PrefixLister;
use crate::raw::*;
use crate::*;

/// Simulate missing capabilities for backends in a configurable way.
#[derive(Debug, Clone)]
pub struct SimulateLayer {
    list_recursive: bool,
    stat_dir: bool,
    create_dir: bool,
    delete_recursive: bool,
}

impl Default for SimulateLayer {
    fn default() -> Self {
        Self {
            list_recursive: true,
            stat_dir: true,
            create_dir: true,
            delete_recursive: true,
        }
    }
}

impl SimulateLayer {
    /// Enable or disable recursive list simulation. Default: true.
    pub fn with_list_recursive(mut self, enabled: bool) -> Self {
        self.list_recursive = enabled;
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

    /// Enable or disable recursive delete simulation. Default: true.
    pub fn with_delete_recursive(mut self, enabled: bool) -> Self {
        self.delete_recursive = enabled;
        self
    }
}

impl Layer for SimulateLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(self.layer(inner))
    }
}

impl SimulateLayer {
    fn layer(&self, inner: Servicer) -> SimulateService {
        SimulateService {
            config: self.clone(),
            inner,
        }
    }
}

/// Service that applies capability simulation.
pub struct SimulateService {
    config: SimulateLayer,
    inner: Servicer,
}

impl Debug for SimulateService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl SimulateService {
    fn simulate_capability(&self, mut cap: Capability) -> Capability {
        if self.config.create_dir && cap.list && cap.write_can_empty {
            cap.create_dir = true;
        }
        if self.config.delete_recursive && cap.list && cap.delete {
            cap.delete_with_recursive = true;
        }
        cap
    }

    async fn simulate_create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let capability = self.inner.capability();

        if capability.create_dir || !self.config.create_dir {
            return self.inner.create_dir(ctx, path, args).await;
        }

        if capability.write_can_empty && capability.list {
            let (_, mut w) = self.inner.write(ctx, path, OpWrite::default()).await?;
            oio::Write::close(&mut w).await?;
            return Ok(RpCreateDir::default());
        }

        self.inner.create_dir(ctx, path, args).await
    }

    async fn simulate_stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> Result<RpStat> {
        let capability = self.inner.capability();

        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        if path.ends_with('/') {
            if capability.create_dir {
                let meta = self
                    .inner
                    .stat(ctx, path, args.clone())
                    .await?
                    .into_metadata();

                if meta.is_file() {
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        "stat expected a directory, but found a file",
                    ));
                }

                return Ok(RpStat::new(meta));
            }

            if self.config.stat_dir && capability.list_with_recursive {
                let (_, mut l) = self
                    .inner
                    .list(
                        ctx,
                        path,
                        OpList::default().with_recursive(true).with_limit(1),
                    )
                    .await?;

                return if l.next().await?.is_some() {
                    Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
                } else {
                    Err(Error::new(
                        ErrorKind::NotFound,
                        "the directory is not found",
                    ))
                };
            }
        }

        self.inner.stat(ctx, path, args).await
    }

    async fn simulate_list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, SimulateLister)> {
        let cap = self.inner.capability();

        let recursive = args.recursive();
        let forward = args;

        let (rp, lister) = match (
            recursive,
            cap.list_with_recursive,
            self.config.list_recursive,
        ) {
            // Backend supports recursive list, forward directly.
            (_, true, _) => {
                let (rp, p) = self.inner.list(ctx, path, forward).await?;
                (rp, SimulateLister::One(p))
            }
            // Simulate recursive via flat list when enabled.
            (true, false, true) => {
                if path.ends_with('/') {
                    let p = ServicerFlatLister::new(ctx.clone(), self.inner.clone(), path);
                    (RpList::default(), SimulateLister::Two(p))
                } else {
                    let parent = get_parent(path);
                    let p = ServicerFlatLister::new(ctx.clone(), self.inner.clone(), parent);
                    let p = PrefixLister::new(p, path);
                    (RpList::default(), SimulateLister::Four(p))
                }
            }
            // Recursive requested but simulation disabled; rely on backend and propagate errors.
            (true, false, false) => {
                let (rp, p) = self.inner.list(ctx, path, forward).await?;
                (rp, SimulateLister::One(p))
            }
            // Non-recursive list: keep existing prefix handling semantics.
            (false, false, _) => {
                if path.ends_with('/') {
                    let (rp, p) = self.inner.list(ctx, path, forward).await?;
                    (rp, SimulateLister::One(p))
                } else {
                    let parent = get_parent(path);
                    let (rp, p) = self.inner.list(ctx, parent, forward).await?;
                    let p = PrefixLister::new(p, path);
                    (rp, SimulateLister::Three(p))
                }
            }
        };

        Ok((rp, lister))
    }

    async fn simulate_delete_with_recursive(
        &self,
        ctx: &OperationContext,
        deleter: &mut oio::Deleter,
        path: &str,
        args: OpDelete,
    ) -> Result<()> {
        if !self.capability().delete_with_recursive {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "recursive delete is not supported",
            ));
        }

        let non_recursive = args.clone().with_recursive(false);

        let (_rp, mut lister) = self
            .simulate_list(ctx, path, OpList::new().with_recursive(true))
            .await?;

        while let Some(entry) = lister.next().await? {
            let entry = entry.into_entry();
            let mut entry_args = non_recursive.clone();
            if let Some(version) = entry.metadata().version() {
                entry_args = entry_args.with_version(version);
            }
            deleter.delete(entry.path(), entry_args).await?;
        }

        Ok(())
    }
}

impl Service for SimulateService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = SimulateLister;
    type Deleter = SimulateDeleter;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.simulate_capability(self.inner.capability())
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.simulate_create_dir(ctx, path, args).await
    }

    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(ctx, path, args).await
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(ctx, path, args).await
    }

    async fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        self.inner.copy(ctx, from, to, args, opts).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.simulate_stat(ctx, path, args).await
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, deleter) = self.inner.delete(ctx).await?;
        Ok((
            rp,
            SimulateDeleter::new(
                self.config.clone(),
                self.inner.clone(),
                ctx.clone(),
                deleter,
            ),
        ))
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        self.simulate_list(ctx, path, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

pub type SimulateLister = FourWays<
    oio::Lister,
    ServicerFlatLister,
    PrefixLister<oio::Lister>,
    PrefixLister<ServicerFlatLister>,
>;

pub struct ServicerFlatLister {
    ctx: OperationContext,
    srv: Servicer,
    next_dir: Option<oio::Entry>,
    active_lister: Vec<(Option<oio::Entry>, oio::Lister)>,
}

impl ServicerFlatLister {
    fn new(ctx: OperationContext, srv: Servicer, path: &str) -> Self {
        Self {
            ctx,
            srv,
            next_dir: Some(oio::Entry::new(path, Metadata::new(EntryMode::DIR))),
            active_lister: vec![],
        }
    }
}

impl oio::List for ServicerFlatLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            if let Some(de) = self.next_dir.take() {
                let (_, mut l) = match self.srv.list(&self.ctx, de.path(), OpList::new()).await {
                    Ok(v) => v,
                    Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                        log::warn!(
                            "ServicerFlatLister skipping directory due to permission denied: {}",
                            de.path()
                        );
                        continue;
                    }
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        log::warn!(
                            "ServicerFlatLister skipping directory due to not found during listing: {}",
                            de.path()
                        );
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                let first = loop {
                    match l.next().await {
                        Ok(v) => break v,
                        Err(e) if e.kind() == ErrorKind::NotFound => {
                            log::warn!(
                                "ServicerFlatLister skipping entry due to not found during listing: {}",
                                de.path()
                            );
                            continue;
                        }
                        Err(e) => return Err(e),
                    }
                };
                if let Some(v) = first {
                    self.active_lister.push((Some(de.clone()), l));

                    if v.mode().is_dir() {
                        if v.path() != de.path() {
                            self.next_dir = Some(v);
                            continue;
                        }
                    } else {
                        return Ok(Some(v));
                    }
                }
            }

            if matches!(self.active_lister.last(), Some((None, _))) {
                let _ = self.active_lister.pop();
                continue;
            }

            let (de, lister) = match self.active_lister.last_mut() {
                Some((de, lister)) => (de, lister),
                None => return Ok(None),
            };

            match lister.next().await {
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    let path = de.as_ref().map(|entry| entry.path()).unwrap_or("<unknown>");
                    log::warn!(
                        "ServicerFlatLister skipping entry due to not found during recursive listing: {}",
                        path
                    );
                    continue;
                }
                Err(e) => return Err(e),
                Ok(Some(v)) if v.mode().is_dir() => {
                    if v.path()
                        != de
                            .as_ref()
                            .expect("de must be present before listing")
                            .path()
                    {
                        self.next_dir = Some(v);
                        continue;
                    }
                }
                Ok(Some(v)) => return Ok(Some(v)),
                Ok(None) => match de.take() {
                    Some(de) => return Ok(Some(de)),
                    None => {
                        let _ = self.active_lister.pop();
                        continue;
                    }
                },
            }
        }
    }
}

/// Deleter wrapper that simulates recursive deletion.
pub struct SimulateDeleter {
    config: SimulateLayer,
    inner: Servicer,
    ctx: OperationContext,
    deleter: oio::Deleter,
}

impl SimulateDeleter {
    pub fn new(
        config: SimulateLayer,
        inner: Servicer,
        ctx: OperationContext,
        deleter: oio::Deleter,
    ) -> Self {
        Self {
            config,
            inner,
            ctx,
            deleter,
        }
    }
}

impl oio::Delete for SimulateDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        if args.recursive() {
            let cap = self.inner.capability();

            if cap.delete_with_recursive {
                return self.deleter.delete(path, args).await;
            }

            if self.config.delete_recursive {
                let service = SimulateService {
                    config: self.config.clone(),
                    inner: self.inner.clone(),
                };
                return service
                    .simulate_delete_with_recursive(&self.ctx, &mut self.deleter, path, args)
                    .await;
            }
        }

        self.deleter.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        self.deleter.close().await
    }
}
