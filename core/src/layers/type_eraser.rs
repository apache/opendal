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

use crate::raw::*;
use crate::*;
use std::fmt::Debug;
use std::fmt::Formatter;

/// TypeEraseLayer will erase the types on internal accessor.
///
/// For example, we will erase `Self::Reader` to `oio::Reader` (`Box<dyn oio::Read>`).
///
/// # Notes
///
/// TypeEraseLayer is not a public accessible layer that can be used by
/// external users. We use this layer to erase any generic types.
pub struct TypeEraseLayer;

impl<A: Access> Layer<A> for TypeEraseLayer {
    type LayeredAccess = TypeEraseAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        TypeEraseAccessor { inner }
    }
}

/// Provide reader wrapper for backend.
pub struct TypeEraseAccessor<A: Access> {
    inner: A,
}

impl<A: Access> Debug for TypeEraseAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<A: Access> LayeredAccess for TypeEraseAccessor<A> {
    type Inner = A;
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type BlockingReader = oio::BlockingReader;
    type BlockingWriter = oio::BlockingWriter;
    type BlockingLister = oio::BlockingLister;
    type BlockingDeleter = oio::BlockingDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, Box::new(r) as oio::Reader))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, Box::new(w) as oio::Writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner
            .delete()
            .await
            .map(|(rp, p)| (rp, Box::new(p) as oio::Deleter))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner
            .list(path, args)
            .await
            .map(|(rp, p)| (rp, Box::new(p) as oio::Lister))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, Box::new(r) as oio::BlockingReader))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| (rp, Box::new(w) as oio::BlockingWriter))
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        self.inner
            .blocking_delete()
            .map(|(rp, p)| (rp, Box::new(p) as oio::BlockingDeleter))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.inner
            .blocking_list(path, args)
            .map(|(rp, p)| (rp, Box::new(p) as oio::BlockingLister))
    }
}
