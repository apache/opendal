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

use opendal_core::raw::*;
use opendal_core::{Capability, ErrorKind, OperationContext, Result};

use crate::key::subject;
use crate::mutation::{CacheDeleter, Mutation};
use crate::reader::CachedReader;
use crate::{CacheDecision, CacheLayer, CacheReadRequest};

#[derive(Debug)]
pub(crate) struct CacheService {
    pub(crate) inner: Servicer,
    pub(crate) layer: CacheLayer,
}

impl Service for CacheService {
    type Reader = oio::Reader;
    type Writer = Mutation<oio::Writer>;
    type Lister = oio::Lister;
    type Deleter = CacheDeleter;
    type Copier = Mutation<oio::Copier>;
    type Composer = Mutation<oio::Composer>;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let subject = subject(&self.info(), path, args.version());
        let state = &self.layer.state;
        let epoch = state.epoch(&subject).await;
        let observed_at = Timestamp::now();
        let result = self.inner.stat(ctx, path, args).await;
        if result
            .as_ref()
            .is_err_and(|error| error.kind() != ErrorKind::NotFound)
        {
            return result;
        }
        let metadata = result
            .as_ref()
            .ok()
            .map(|response| response.clone().into_metadata());
        state
            .observe_stat(&subject, epoch, observed_at, metadata.as_ref())
            .await;
        result
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let decision = self
            .layer
            .policy
            .on_read(&CacheReadRequest { path, args: &args });
        let ttl = match decision {
            CacheDecision::Bypass => return self.inner.read(ctx, path, args),
            CacheDecision::Cache => self.layer.ttl,
            CacheDecision::CacheFor(ttl) => ttl,
        };
        let capability = self.inner.capability();
        if !capability.read
            || (args.version().is_some() && !capability.read_with_version)
            || args.is_conditional()
            || args.override_content_type().is_some()
            || args.override_cache_control().is_some()
            || args.override_content_disposition().is_some()
        {
            return self.inner.read(ctx, path, args);
        }
        let subject = subject(&self.info(), path, args.version());
        Ok(Box::new(CachedReader {
            state: self.layer.state.clone(),
            source: self.inner.clone(),
            context: ctx.clone(),
            path: path.to_owned(),
            args,
            subject,
            chunk_size: self.layer.chunk_size as u64,
            ttl,
        }))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(Mutation {
            inner: self.inner.write(ctx, path, args)?,
            state: self.layer.state.clone(),
            destination: subject(&self.info(), path, None),
        })
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        Ok(CacheDeleter::new(
            self.inner.delete(ctx)?,
            self.layer.state.clone(),
            self.info(),
        ))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        Ok(Mutation {
            inner: self.inner.copy(ctx, from, to, args)?,
            state: self.layer.state.clone(),
            destination: subject(&self.info(), to, None),
        })
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        Ok(Mutation {
            inner: self.inner.compose(ctx, to, args)?,
            state: self.layer.state.clone(),
            destination: subject(&self.info(), to, None),
        })
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        let info = self.info();
        let subjects = [subject(&info, from, None), subject(&info, to, None)];
        self.layer.state.begin_mutation(&subjects).await;
        let result = self.inner.rename(ctx, from, to, args).await;
        self.layer.state.invalidate(&subjects).await;
        result
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        let info = self.info();
        let mut subjects = vec![subject(&info, path, None)];
        if let Some(version) = args.version() {
            subjects.push(subject(&info, path, Some(version)));
        }
        self.layer.state.begin_mutation(&subjects).await;
        let result = self.inner.restore(ctx, path, args).await;
        self.layer.state.invalidate(&subjects).await;
        result
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
