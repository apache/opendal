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

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::ready;
use log::debug;
use log::error;
use tokio::fs;

use super::error::parse_io_error;
use crate::error::Error;
use crate::error::Kind;
use crate::error::Result;
use crate::Accessor;
use crate::Object;

pub struct Readdir {
    acc: Arc<dyn Accessor>,
    root: String,
    path: String,

    rd: fs::ReadDir,
}

impl Readdir {
    pub fn new(acc: Arc<dyn Accessor>, root: &str, path: &str, rd: fs::ReadDir) -> Self {
        Self {
            acc,
            root: root.to_string(),
            path: path.to_string(),
            rd,
        }
    }
}

impl futures::Stream for Readdir {
    type Item = Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.rd).poll_next_entry(cx)) {
            Err(e) => {
                error!("object {} stream poll_next: {:?}", &self.path, e);
                Poll::Ready(Some(Err(parse_io_error(e, "list", &self.path))))
            }
            Ok(None) => {
                debug!("object {} list done", &self.path);
                Poll::Ready(None)
            }
            Ok(Some(de)) => {
                let de_path = de.path();
                let de_path = de_path.strip_prefix(&self.root).map_err(|e| {
                    let e = Error::Object {
                        kind: Kind::Unexpected,
                        op: "list",
                        path: de.path().to_string_lossy().to_string(),
                        source: anyhow::Error::from(e),
                    };
                    error!("object {:?} path strip_prefix: {:?}", &de.path(), e);
                    e
                })?;
                let path = de_path.to_string_lossy();

                let mut o = Object::new(self.acc.clone(), &path);

                let meta = o.metadata_mut();
                meta.set_path(&path);

                debug!("object {} got entry, path: {}", &self.path, meta.path());
                Poll::Ready(Some(Ok(o)))
            }
        }
    }
}
