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

use anyhow::anyhow;
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use log::debug;
use log::error;

use crate::error::other;
use crate::error::ObjectError;
use crate::Accessor;
use crate::Object;
use crate::ObjectMode;

pub struct Readdir {
    acc: Arc<dyn Accessor>,
    root: String,
    path: String,

    rd: hdrs::Readdir,
}

impl Readdir {
    pub fn new(acc: Arc<dyn Accessor>, root: &str, path: &str, rd: hdrs::Readdir) -> Self {
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

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rd.next() {
            None => {
                debug!("object {} list done", &self.path);
                Poll::Ready(None)
            }
            Some(de) => {
                let de_path = de.name().to_string_lossy();
                let path = de_path.strip_prefix(&self.root).ok_or_else(|| {
                    let e = other(ObjectError::new(
                        "list",
                        &de_path,
                        anyhow!("path doesn't have specified prefix"),
                    ));
                    error!("object {:?} path strip_prefix: {:?}", &de_path, e);
                    e
                })?;

                let mut o = Object::new(self.acc.clone(), path);

                let meta = o.metadata_mut();
                if de.is_file() {
                    meta.set_mode(ObjectMode::FILE);
                    meta.set_path(path);
                } else if de.is_dir() {
                    // Make sure we are returning the correct path.
                    meta.set_path(&format!("{}/", path));
                    meta.set_mode(ObjectMode::DIR);
                } else {
                    meta.set_path(path);
                    meta.set_mode(ObjectMode::Unknown);
                }

                debug!("object {} got entry, path: {}", &self.path, meta.path());
                Poll::Ready(Some(Ok(o)))
            }
        }
    }
}
