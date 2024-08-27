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

use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::raw::*;
use crate::*;

pub trait MetricsIntercept: Debug + Clone + Send + Sync + Unpin + 'static {
    fn observe_operation_duration_seconds(
        &self,
        scheme: Scheme,
        namespace: &str,
        path: &str,
        op: Operation,
        duration: Duration,
    );
    fn observe_operation_bytes(
        &self,
        scheme: Scheme,
        namespace: &str,
        path: &str,
        op: Operation,
        bytes: usize,
    );
    fn observe_operation_errors_total(
        &self,
        scheme: Scheme,
        namespace: &str,
        path: &str,
        op: Operation,
        error: ErrorKind,
    );
}

pub struct MetricsLayer<I: MetricsIntercept> {
    interceptor: I,
}

#[derive(Clone)]
pub struct MetricsAccessor<A: Access, I: MetricsIntercept> {
    inner: A,
    interceptor: I,
}

impl<A: Access, I: MetricsIntercept> Debug for MetricsAccessor<A, I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<A: Access, I: MetricsIntercept> LayeredAccess for MetricsAccessor<A, I> {
    type Inner = ();
    type Reader = ();
    type BlockingReader = ();
    type Writer = ();
    type BlockingWriter = ();
    type Lister = ();
    type BlockingLister = ();

    fn inner(&self) -> &Self::Inner {
        todo!()
    }

    fn read(
        &self,
        path: &str,
        args: OpRead,
    ) -> impl Future<Output = Result<(RpRead, Self::Reader)>> + MaybeSend {
        todo!()
    }

    fn write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> impl Future<Output = Result<(RpWrite, Self::Writer)>> + MaybeSend {
        todo!()
    }

    fn list(
        &self,
        path: &str,
        args: OpList,
    ) -> impl Future<Output = Result<(RpList, Self::Lister)>> + MaybeSend {
        todo!()
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        todo!()
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        todo!()
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        todo!()
    }
}

pub struct MetricsWrapper<R, I: MetricsIntercept> {
    inner: R,
    interceptor: I,

    scheme: Scheme,
    namespace: Arc<String>,
    path: String,
}

impl<R, I: MetricsIntercept> MetricsWrapper<R, I> {
    fn new(inner: R, interceptor: I, scheme: Scheme, namespace: Arc<String>, path: String) -> Self {
        Self {
            inner,
            interceptor,
            scheme,
            namespace,
            path,
        }
    }
}

impl<R: oio::Read, I: MetricsIntercept> oio::Read for MetricsWrapper<R, I> {
    async fn read(&mut self) -> Result<Buffer> {
        let op = Operation::ReaderRead;

        let start = Instant::now();

        let res = match self.inner.read().await {
            Ok(bs) => {
                self.interceptor.observe_operation_bytes(
                    self.scheme,
                    &self.namespace,
                    &self.path,
                    op,
                    bs.len(),
                );
                Ok(bs)
            }
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.scheme,
                    &self.namespace,
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.scheme,
            &self.namespace,
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }
}
