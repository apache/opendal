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

use crate::*;
use async_trait::async_trait;
use futures::Stream;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[async_trait]
pub trait ObjectPage: Send + 'static {
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>>;
}

pub type ObjectPager = Box<dyn ObjectPage>;

#[async_trait]
impl ObjectPage for ObjectPager {
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        self.as_mut().next_page().await
    }
}

pub struct EmptyObjectPager;

#[async_trait]
impl ObjectPage for EmptyObjectPager {
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        Ok(None)
    }
}

pub struct ObjectLister {
    acc: Arc<dyn Accessor>,
    pager: ObjectPager,
}

impl ObjectLister {
    pub fn new(acc: Arc<dyn Accessor>, pager: ObjectPager) -> Self {
        Self { acc, pager }
    }
}

impl Stream for ObjectLister {
    type Item = Result<Object>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub trait BlockingObjectPage: 'static {
    fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>>;
}

pub type BlockingObjectPager = Box<dyn BlockingObjectPage>;

impl BlockingObjectPage for BlockingObjectPager {
    fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        self.as_mut().next_page()
    }
}

pub struct BlockingObjectLister {
    acc: Arc<dyn Accessor>,
    pager: BlockingObjectPager,
}

impl BlockingObjectLister {
    pub fn new(acc: Arc<dyn Accessor>, pager: BlockingObjectPager) -> Self {
        Self { acc, pager }
    }
}

pub struct EmptyBlockingObjectPager;

impl BlockingObjectPage for EmptyBlockingObjectPager {
    fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        Ok(None)
    }
}
