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

use std::fmt::Display;
use std::fmt::Formatter;

use async_trait::async_trait;
use bytes::Bytes;

use crate::*;

/// AppendOperation is the name for APIs of Append.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub enum AppendOperation {
    /// Operation for [`Append::append`]
    Append,
    /// Operation for [`Append::close`]
    Close,
}

impl AppendOperation {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Display for AppendOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

impl From<AppendOperation> for &'static str {
    fn from(v: AppendOperation) -> &'static str {
        use AppendOperation::*;

        match v {
            Append => "Append::append",
            Close => "Append::close",
        }
    }
}

/// Appender is a type erased [`Append`]
pub type Appender = Box<dyn Append>;

/// Append is the trait that OpenDAL returns to callers.
///
/// # Notes
///
/// Users will call `append` multiple times.
#[async_trait]
pub trait Append: Unpin + Send + Sync {
    /// Append data to the end of file.
    ///
    /// Users will call `append` multiple times.
    /// Please make sure `append` is safe to re-enter.
    async fn append(&mut self, bs: Bytes) -> Result<()>;

    /// Seal the file to mark it as unmodifiable.
    async fn close(&mut self) -> Result<()>;
}

#[async_trait]
impl Append for () {
    async fn append(&mut self, bs: Bytes) -> Result<()> {
        let _ = bs;

        unimplemented!("append is required to be implemented for oio::Append")
    }

    async fn close(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output appender doesn't support close",
        ))
    }
}

/// `Box<dyn Append>` won't implement `Append` automatically.
///
/// To make Appender work as expected, we must add this impl.
#[async_trait]
impl<T: Append + ?Sized> Append for Box<T> {
    async fn append(&mut self, bs: Bytes) -> Result<()> {
        (**self).append(bs).await
    }

    async fn close(&mut self) -> Result<()> {
        (**self).close().await
    }
}
