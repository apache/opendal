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

use std::cmp::Ordering;

use futures::{Stream, StreamExt};

use crate::raw::*;
use crate::*;

/// HttpBody is the streaming body that opendal's HttpClient returned.
///
/// It implements the `oio::Read` trait, service implementors can return it as
/// `Access::Read`.
pub struct HttpBody {
    stream: Box<dyn Stream<Item = Result<Buffer>> + Send + Sync + Unpin + 'static>,
    size: Option<u64>,
    consumed: u64,
}

impl HttpBody {
    /// Create a new `HttpBody` with given stream and optional size.
    pub fn new<S>(stream: S, size: Option<u64>) -> Self
    where
        S: Stream<Item = Result<Buffer>> + Send + Sync + Unpin + 'static,
    {
        HttpBody {
            stream: Box::new(stream),
            size,
            consumed: 0,
        }
    }

    /// Check if the consumed data is equal to the expected content length.
    #[inline]
    fn check(&self) -> Result<()> {
        let Some(expect) = self.size else {
            return Ok(());
        };

        let actual = self.consumed;
        match actual.cmp(&expect) {
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(Error::new(
                ErrorKind::Unexpected,
                &format!("http response got too little data, expect: {expect}, actual: {actual}"),
            )
            .set_temporary()),
            Ordering::Greater => Err(Error::new(
                ErrorKind::Unexpected,
                &format!("http response got too much data, expect: {expect}, actual: {actual}"),
            )
            .set_temporary()),
        }
    }
}

impl oio::Read for HttpBody {
    async fn read(&mut self) -> Result<Buffer> {
        match self.stream.next().await.transpose()? {
            Some(buf) => {
                self.consumed += buf.len() as u64;
                Ok(buf)
            }
            None => {
                self.check()?;
                Ok(Buffer::new())
            }
        }
    }
}
