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

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;

use crate::error::Result;
use crate::ops::OpRead;
use crate::Accessor;
use crate::BoxedAsyncReader;

#[derive(Default)]
pub struct Builder {
    data: Bytes,
}

impl Builder {
    pub fn data(&mut self, data: Bytes) -> &mut Self {
        self.data = data;

        self
    }

    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        Ok(Arc::new(Backend {
            data: self.data.clone(),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct Backend {
    data: Bytes,
}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }
}

#[async_trait]
impl Accessor for Backend {
    async fn read(&self, args: &OpRead) -> Result<BoxedAsyncReader> {
        let mut data = self.data.clone();
        if let Some(offset) = args.offset {
            data = data.slice(offset as usize..data.len());
        };

        if let Some(size) = args.size {
            data = data.slice(0..size as usize);
        };

        let r: BoxedAsyncReader = Box::new(BytesStream(data).into_async_read());
        Ok(r)
    }
}

struct BytesStream(Bytes);

impl futures::Stream for BytesStream {
    type Item = std::result::Result<bytes::Bytes, std::io::Error>;

    // Always poll the entire stream.
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let size = self.0.len();
        match self.0.len() {
            0 => Poll::Ready(None),
            _ => Poll::Ready(Some(Ok(self.0.split_to(size)))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.0.len(), Some(self.0.len()))
    }
}
