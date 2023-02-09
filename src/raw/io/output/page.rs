// Copyright 2022 Datafuse Labs.
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

use super::ObjectEntry;
use crate::*;

/// ObjectPage trait is used by [`Accessor`] to implement `list` operation.
///
/// `list` will return a boxed `ObjectPage` which allow users to call `next_page`
/// to fecth a new page of [`ObjectEntry`].
#[async_trait]
pub trait ObjectPage: Send + Sync + 'static {
    /// Fetch a new page of [`ObjectEntry`]
    ///
    /// `Ok(None)` means all object pages have been returned. Any following call
    /// to `next_page` will always get the same result.
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>>;
}

/// The boxed version of [`ObjectPage`]
pub type ObjectPager = Box<dyn ObjectPage>;

#[async_trait]
impl ObjectPage for ObjectPager {
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        self.as_mut().next_page().await
    }
}

#[async_trait]
impl ObjectPage for () {
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        Ok(None)
    }
}

/// BlockingObjectPage is the blocking version of [`ObjectPage`].
pub trait BlockingObjectPage: 'static {
    /// Fetch a new page of [`ObjectEntry`]
    ///
    /// `Ok(None)` means all object pages have been returned. Any following call
    /// to `next_page` will always get the same result.
    fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>>;
}

/// BlockingObjectPager is a boxed [`BlockingObjectPage`]
pub type BlockingObjectPager = Box<dyn BlockingObjectPage>;

impl BlockingObjectPage for BlockingObjectPager {
    fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        self.as_mut().next_page()
    }
}

impl BlockingObjectPage for () {
    fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        Ok(None)
    }
}
