// Copyright 2021 Datafuse Labs.
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
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bitflags::bitflags;
use futures::future::BoxFuture;
use futures::ready;

use crate::error::Result;
use crate::ops::OpStat;
use crate::ops::{OpDelete, OpList};
use crate::Accessor;
use crate::Reader;
use crate::Writer;

#[derive(Clone, Debug)]
pub struct Object {
    acc: Arc<dyn Accessor>,
    meta: Metadata,
}

impl Object {
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,
            meta: Metadata {
                path: path.to_string(),
                ..Default::default()
            },
        }
    }

    pub fn reader(&self) -> Reader {
        Reader::new(self.acc.clone(), &self.path())
    }

    pub fn writer(&self) -> Writer {
        Writer::new(self.acc.clone(), &self.path())
    }

    pub async fn delete(&self) -> Result<()> {
        let op = &OpDelete::new(&self.path());

        self.acc.delete(op).await
    }

    pub(crate) fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.meta
    }

    /// # Note for implementor
    ///
    /// metadata should never be called insides backend.
    pub async fn metadata(&mut self) -> Result<&Metadata> {
        if self.meta.complete {
            return Ok(&self.meta);
        }

        let op = &OpStat::new(&self.path());
        self.meta = self.acc.stat(op).await?;

        Ok(&self.meta)
    }

    pub fn path(&self) -> &str {
        &self.meta.path
    }

    pub async fn mode(&mut self) -> Result<ObjectMode> {
        if let Some(v) = self.meta.mode {
            return Ok(v);
        }

        let meta = self.metadata().await?;
        if let Some(v) = meta.mode {
            return Ok(v);
        }

        unreachable!("object meta should have mode, but it's not")
    }

    pub async fn content_length(&mut self) -> Result<u64> {
        if let Some(v) = self.meta.content_length {
            return Ok(v);
        }

        let meta = self.metadata().await?;
        if let Some(v) = meta.content_length {
            return Ok(v);
        }

        unreachable!("object meta should have content length, but it's not")
    }
}

#[derive(Debug, Clone, Default)]
pub struct Metadata {
    complete: bool,

    path: String,
    mode: Option<ObjectMode>,

    content_length: Option<u64>,
}

impl Metadata {
    pub fn set_complete(&mut self) -> &mut Self {
        self.complete = true;
        self
    }

    pub fn mode(&self) -> ObjectMode {
        if let Some(v) = self.mode {
            return v;
        }

        unreachable!("object meta should have mode, but it's not")
    }

    pub fn set_mode(&mut self, mode: ObjectMode) -> &mut Self {
        self.mode = Some(mode);
        self
    }

    pub fn content_length(&self) -> u64 {
        if let Some(v) = self.content_length {
            return v;
        }

        unreachable!("object meta should have content length, but it's not")
    }

    pub fn set_content_length(&mut self, content_length: u64) -> &mut Self {
        self.content_length = Some(content_length);
        self
    }
}

bitflags! {
    #[derive(Default)]
    pub struct ObjectMode: u32 {
        const FILE =1<<0;
        const DIR = 1<<1;
        const LINK = 1<<2;
    }
}

pub type BoxedObjectStream = Box<dyn futures::Stream<Item = Result<Object>> + Unpin + Send>;

pub struct ObjectStream {
    acc: Arc<dyn Accessor>,
    path: String,

    state: State,
}

enum State {
    Idle,
    Sending(BoxFuture<'static, Result<BoxedObjectStream>>),
    Listing(BoxedObjectStream),
}

impl ObjectStream {
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,
            path: path.to_string(),
            state: State::Idle,
        }
    }
}

impl futures::Stream for ObjectStream {
    type Item = Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            State::Idle => {
                let acc = self.acc.clone();
                let op = OpList::new(&self.path);

                let future = async move { acc.list(&op).await };

                self.state = State::Sending(Box::pin(future));
                self.poll_next(cx)
            }
            State::Sending(future) => match ready!(Pin::new(future).poll(cx)) {
                Ok(obs) => {
                    self.state = State::Listing(obs);
                    self.poll_next(cx)
                }
                Err(e) => Poll::Ready(Some(Err(e))),
            },
            State::Listing(obs) => Pin::new(obs).poll_next(cx),
        }
    }
}
