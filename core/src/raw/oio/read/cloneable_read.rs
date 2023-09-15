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

use std::io::SeekFrom;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;

use crate::raw::*;
use crate::*;

/// Convert given reader into a wrapper with `std::sync::Mutex` for `Send + Sync + Clone`.
pub fn into_cloneable_reader_within_std<R>(reader: R) -> CloneableReaderWithinStd<R> {
    CloneableReaderWithinStd(Arc::new(std::sync::Mutex::new(reader)))
}

/// CloneableReaderWithinStd is a Send + Sync + Clone with `std::sync::Mutex` wrapper of input
/// reader.
///
/// Caller can clone this reader but only one thread can calling `oio::Read` API at the
/// same time, otherwise, we will return error if lock block happened.
pub struct CloneableReaderWithinStd<R>(Arc<std::sync::Mutex<R>>);

impl<R> CloneableReaderWithinStd<R> {
    /// Consume self to get inner reader.
    pub fn into_inner(self) -> Arc<std::sync::Mutex<R>> {
        self.0
    }
}

impl<R> Clone for CloneableReaderWithinStd<R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<R: oio::Read> oio::Read for CloneableReaderWithinStd<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        match self.0.try_lock() {
            Ok(mut this) => this.poll_read(cx, buf),
            Err(_) => Poll::Ready(Err(Error::new(
                ErrorKind::Unexpected,
                "the cloneable reader is expected to have only one owner, but it's not",
            ))),
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        match self.0.try_lock() {
            Ok(mut this) => this.poll_seek(cx, pos),
            Err(_) => Poll::Ready(Err(Error::new(
                ErrorKind::Unexpected,
                "the cloneable reader is expected to have only one owner, but it's not",
            ))),
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match self.0.try_lock() {
            Ok(mut this) => this.poll_next(cx),
            Err(_) => Poll::Ready(Some(Err(Error::new(
                ErrorKind::Unexpected,
                "the cloneable reader is expected to have only one owner, but it's not",
            )))),
        }
    }
}

/// Convert given reader into a wrapper with `tokio::sync::Mutex` for `Send + Sync + Clone`.
pub fn into_cloneable_reader_within_tokio<R>(reader: R) -> CloneableReaderWithinTokio<R> {
    CloneableReaderWithinTokio(Arc::new(tokio::sync::Mutex::new(reader)))
}

/// CloneableReaderWithinTokio is a Send + Sync + Clone with `tokio::sync::Mutex` wrapper of input
/// reader.
///
/// Caller can clone this reader but only one thread can calling `oio::Read` API at the
/// same time, otherwise, we will return error if lock block happened.
pub struct CloneableReaderWithinTokio<R>(Arc<tokio::sync::Mutex<R>>);

impl<R> CloneableReaderWithinTokio<R> {
    /// Consume self to get inner reader.
    pub fn into_inner(self) -> Arc<tokio::sync::Mutex<R>> {
        self.0
    }
}

impl<R> Clone for CloneableReaderWithinTokio<R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<R: oio::Read> oio::Read for CloneableReaderWithinTokio<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        match self.0.try_lock() {
            Ok(mut this) => this.poll_read(cx, buf),
            Err(_) => Poll::Ready(Err(Error::new(
                ErrorKind::Unexpected,
                "the cloneable reader is expected to have only one owner, but it's not",
            ))),
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        match self.0.try_lock() {
            Ok(mut this) => this.poll_seek(cx, pos),
            Err(_) => Poll::Ready(Err(Error::new(
                ErrorKind::Unexpected,
                "the cloneable reader is expected to have only one owner, but it's not",
            ))),
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match self.0.try_lock() {
            Ok(mut this) => this.poll_next(cx),
            Err(_) => Poll::Ready(Some(Err(Error::new(
                ErrorKind::Unexpected,
                "the cloneable reader is expected to have only one owner, but it's not",
            )))),
        }
    }
}
