// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;

use super::Entry;
use crate::*;

/// Page trait is used by [`crate::raw::Accessor`] to implement `list`
/// or `scan` operation.
#[async_trait]
pub trait Page: Send + Sync + 'static {
    /// Fetch a new page of [`Entry`]
    ///
    /// `Ok(None)` means all object pages have been returned. Any following call
    /// to `next_page` will always get the same result.
    async fn next_page(&mut self) -> Result<Option<Vec<Entry>>>;
}

/// The boxed version of [`Page`]
pub type Pager = Box<dyn Page>;

#[async_trait]
impl Page for Pager {
    async fn next_page(&mut self) -> Result<Option<Vec<Entry>>> {
        self.as_mut().next_page().await
    }
}

#[async_trait]
impl Page for () {
    async fn next_page(&mut self) -> Result<Option<Vec<Entry>>> {
        Ok(None)
    }
}

#[async_trait]
impl<P: Page> Page for Option<P> {
    async fn next_page(&mut self) -> Result<Option<Vec<Entry>>> {
        match self {
            Some(p) => p.next_page().await,
            None => Ok(None),
        }
    }
}

/// BlockingPage is the blocking version of [`Page`].
pub trait BlockingPage: 'static {
    /// Fetch a new page of [`Entry`]
    ///
    /// `Ok(None)` means all object pages have been returned. Any following call
    /// to `next_page` will always get the same result.
    fn next_page(&mut self) -> Result<Option<Vec<Entry>>>;
}

/// BlockingPager is a boxed [`BlockingPage`]
pub type BlockingPager = Box<dyn BlockingPage>;

impl BlockingPage for BlockingPager {
    fn next_page(&mut self) -> Result<Option<Vec<Entry>>> {
        self.as_mut().next_page()
    }
}

impl BlockingPage for () {
    fn next_page(&mut self) -> Result<Option<Vec<Entry>>> {
        Ok(None)
    }
}

#[async_trait]
impl<P: BlockingPage> BlockingPage for Option<P> {
    fn next_page(&mut self) -> Result<Option<Vec<Entry>>> {
        match self {
            Some(p) => p.next_page(),
            None => Ok(None),
        }
    }
}
