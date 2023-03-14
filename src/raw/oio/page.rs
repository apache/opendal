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

use super::Entry;
use crate::*;

/// PageOperation is the name for APIs of pager.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub enum PageOperation {
    /// Operation for [`Page::next`]
    Next,
    /// Operation for [`BlockingPage::next`]
    BlockingNext,
}

impl PageOperation {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Display for PageOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

impl From<PageOperation> for &'static str {
    fn from(v: PageOperation) -> &'static str {
        use PageOperation::*;

        match v {
            Next => "Pager::next",
            BlockingNext => "BlockingPager::next",
        }
    }
}

/// Page trait is used by [`crate::raw::Accessor`] to implement `list`
/// or `scan` operation.
#[async_trait]
pub trait Page: Send + Sync + 'static {
    /// Fetch a new page of [`Entry`]
    ///
    /// `Ok(None)` means all pages have been returned. Any following call
    /// to `next` will always get the same result.
    async fn next(&mut self) -> Result<Option<Vec<Entry>>>;
}

/// The boxed version of [`Page`]
pub type Pager = Box<dyn Page>;

#[async_trait]
impl Page for Pager {
    async fn next(&mut self) -> Result<Option<Vec<Entry>>> {
        self.as_mut().next().await
    }
}

#[async_trait]
impl Page for () {
    async fn next(&mut self) -> Result<Option<Vec<Entry>>> {
        Ok(None)
    }
}

#[async_trait]
impl<P: Page> Page for Option<P> {
    async fn next(&mut self) -> Result<Option<Vec<Entry>>> {
        match self {
            Some(p) => p.next().await,
            None => Ok(None),
        }
    }
}

/// BlockingPage is the blocking version of [`Page`].
pub trait BlockingPage: 'static {
    /// Fetch a new page of [`Entry`]
    ///
    /// `Ok(None)` means all pages have been returned. Any following call
    /// to `next` will always get the same result.
    fn next(&mut self) -> Result<Option<Vec<Entry>>>;
}

/// BlockingPager is a boxed [`BlockingPage`]
pub type BlockingPager = Box<dyn BlockingPage>;

impl BlockingPage for BlockingPager {
    fn next(&mut self) -> Result<Option<Vec<Entry>>> {
        self.as_mut().next()
    }
}

impl BlockingPage for () {
    fn next(&mut self) -> Result<Option<Vec<Entry>>> {
        Ok(None)
    }
}

#[async_trait]
impl<P: BlockingPage> BlockingPage for Option<P> {
    fn next(&mut self) -> Result<Option<Vec<Entry>>> {
        match self {
            Some(p) => p.next(),
            None => Ok(None),
        }
    }
}
