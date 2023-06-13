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

use async_trait::async_trait;

use crate::raw::*;
use crate::types::Error;
use crate::types::ErrorKind;
use crate::types::Result;
use crate::Capability;

trait CheckCapability {
    // TODO: Should we return the unsupported option in the result?
    fn check_capability(&self, cap: &Capability) -> bool;
}

macro_rules! impl_check_fn_item {
    ($self:ident, $cap:ident @ $opt_ident:ident => $cap_ident:ident, $($rest:tt)*) => {
        impl_check_fn_item!($self, $cap @ $opt_ident => $cap_ident);
        impl_check_fn_item!($self, $cap @ $($rest)*);
    };
    ($self:ident, $cap:ident @ $opt_ident:ident => $cap_ident:ident) => {
        let has_opt = $self.$opt_ident().is_some();
        if has_opt && !$cap.$cap_ident {
            return false;
        }
    };
    ($self:ident, $cap:ident @) => {};
}

macro_rules! impl_check_fn {
    ($op_type:ty { $($body:tt)* }) => {
        impl CheckCapability for $op_type {
            fn check_capability(&self, cap: &Capability) -> bool {
                impl_check_fn_item!(self, cap @ $($body)*);
                true
            }
        }
    };
}

impl_check_fn!(OpRead {
    if_match => read_with_if_match,
    if_none_match => read_with_if_none_match,
    override_cache_control => read_with_override_cache_control,
    override_content_disposition => read_with_override_content_disposition,
});

macro_rules! check_capability {
    ($msg:literal, $self:ident, $args:ident) => {
        let meta = $self.metadata();
        if !$args.check_capability(&meta.capability()) {
            return Err(Error::new(
                ErrorKind::UnsupportedOption,
                "args contain unsupported options",
            )
            .with_operation($msg));
        }
    };
}

/// Check whether the given operation arguments are supported by the
/// underlying services.
///
/// # Notes
///
/// CapabilityCheckLayer is not a public accessible layer that can be
/// used by external users. OpenDAL will apply it automatically when
/// the operator is in strict mode.
pub struct CapabilityCheckLayer;

impl<A: Accessor> Layer<A> for CapabilityCheckLayer {
    type LayeredAccessor = CapabilityCheckedAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        CapabilityCheckedAccessor { inner }
    }
}

/// Check the operation arguments against the capability of the
/// underlying services.
pub struct CapabilityCheckedAccessor<A: Accessor> {
    inner: A,
}

impl<A: Accessor> Debug for CapabilityCheckedAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for CapabilityCheckedAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Appender = A::Appender;
    type Pager = A::Pager;
    type BlockingPager = A::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner().create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        check_capability!("read", self, args);
        self.inner().read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner().write(path, args).await
    }

    async fn append(&self, path: &str, args: OpAppend) -> Result<(RpAppend, Self::Appender)> {
        self.inner().append(path, args).await
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner().copy(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner().rename(from, to, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner().stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner().delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.inner().list(path, args).await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner().batch(args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner().presign(path, args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner().blocking_create_dir(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        check_capability!("blocking_read", self, args);
        self.inner().blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner().blocking_write(path, args)
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner().blocking_copy(from, to, args)
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner().blocking_rename(from, to, args)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner().blocking_stat(path, args)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner().blocking_delete(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner().blocking_list(path, args)
    }
}
