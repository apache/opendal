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
use std::sync::Arc;

use opendal_core::raw::oio::{self, Compose, Copy, Delete, Write};
use opendal_core::raw::{OpDelete, OpRead, ServiceInfo};
use opendal_core::{Buffer, BytesRange, Metadata, Result};

use crate::key::{Subject, subject};
use crate::state::State;

pub(crate) struct Mutation<T> {
    pub(crate) inner: T,
    pub(crate) state: Arc<State>,
    pub(crate) destination: Subject,
}

impl Write for Mutation<oio::Writer> {
    async fn write(&mut self, data: Buffer) -> Result<()> {
        // In-place and append writers can mutate storage before `close`, while
        // buffered or multipart writers commit on `close`. Guard both paths.
        self.state.begin_mutation(&[self.destination]);
        let result = self.inner.write(data).await;
        self.state.invalidate(&[self.destination]).await;
        result
    }

    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        let result = self.inner.copy_from(path, args, range).await;
        if result.is_err() {
            self.state.invalidate(&[self.destination]).await;
        }
        result
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.state.begin_mutation(&[self.destination]);
        let result = self.inner.close().await;
        self.state.invalidate(&[self.destination]).await;
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let result = self.inner.abort().await;
        self.state.invalidate(&[self.destination]).await;
        result
    }
}

impl Copy for Mutation<oio::Copier> {
    async fn next(&mut self) -> Result<Option<usize>> {
        // One-shot copiers commit the destination during `next`, while
        // multipart copiers commit on `close`. Guard both paths.
        self.state.begin_mutation(&[self.destination]);
        let result = self.inner.next().await;
        self.state.invalidate(&[self.destination]).await;
        result
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.state.begin_mutation(&[self.destination]);
        let result = self.inner.close().await;
        self.state.invalidate(&[self.destination]).await;
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let result = self.inner.abort().await;
        self.state.invalidate(&[self.destination]).await;
        result
    }
}

impl Compose for Mutation<oio::Composer> {
    async fn compose(&mut self, path: &str, args: OpRead) -> Result<()> {
        let result = self.inner.compose(path, args).await;
        if result.is_err() {
            self.state.invalidate(&[self.destination]).await;
        }
        result
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.state.begin_mutation(&[self.destination]);
        let result = self.inner.close().await;
        self.state.invalidate(&[self.destination]).await;
        result
    }
}

pub(crate) struct CacheDeleter {
    inner: oio::Deleter,
    state: Arc<State>,
    source: ServiceInfo,
    subjects: HashSet<Subject>,
}

impl CacheDeleter {
    pub(crate) fn new(inner: oio::Deleter, state: Arc<State>, source: ServiceInfo) -> Self {
        Self {
            inner,
            state,
            source,
            subjects: HashSet::new(),
        }
    }
}

impl Delete for CacheDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        if args.recursive() {
            return self.inner.delete(path, args).await;
        }
        let mut subjects = vec![subject(&self.source, path, None)];
        if let Some(version) = args.version() {
            subjects.push(subject(&self.source, path, Some(version)));
        }
        // Track before calling `inner.delete` so buffered deleters that retain
        // a path on flush error are still invalidated on a subsequent `close`.
        self.subjects.extend(subjects.iter().copied());
        self.state.begin_mutation(&subjects);
        let result = self.inner.delete(path, args).await;
        if result.is_ok() {
            self.state.invalidate(&subjects).await;
        } else {
            let all: Vec<_> = self.subjects.iter().copied().collect();
            self.state.invalidate(&all).await;
        }
        result
    }

    async fn close(&mut self) -> Result<()> {
        let subjects: Vec<_> = self.subjects.iter().copied().collect();
        self.state.begin_mutation(&subjects);
        let result = self.inner.close().await;
        self.state.invalidate(&subjects).await;
        if result.is_ok() {
            self.subjects.clear();
        }
        result
    }
}
