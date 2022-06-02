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

use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use log::debug;

use crate::Accessor;
use crate::Object;
use crate::ObjectMode;

pub struct Readdir {
    acc: Arc<dyn Accessor>,
    path: String,

    rd: hdrs::Readdir,
}

impl Readdir {
    pub fn new(acc: Arc<dyn Accessor>, path: &str, rd: hdrs::Readdir) -> Self {
        Self {
            acc,
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
                let path = de.path();

                let mut o = Object::new(self.acc.clone(), path);

                let meta = o.metadata_mut();
                meta.set_path(path);
                if de.is_file() {
                    meta.set_mode(ObjectMode::FILE);
                } else if de.is_dir() {
                    meta.set_mode(ObjectMode::DIR);
                } else {
                    meta.set_mode(ObjectMode::Unknown);
                }

                debug!("object {} got entry, path: {}", &self.path, meta.path());
                Poll::Ready(Some(Ok(o)))
            }
        }
    }
}
