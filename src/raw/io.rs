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

use std::io::Error;
use std::io::Read;
use std::io::Result;
use std::io::Seek;

use bytes::Bytes;
use futures::AsyncRead;
use futures::AsyncSeek;
use futures::AsyncWrite;
use futures::Sink;
use futures::Stream;

/// BytesRead represents a reader of bytes.
pub trait BytesRead: AsyncRead + Unpin + Send {}
impl<T> BytesRead for T where T: AsyncRead + Unpin + Send {}

/// BytesReader is a boxed dyn [`BytesRead`].
pub type BytesReader = Box<dyn BytesRead>;

/// OutputBytesRead is the output version of bytes returned by OpenDAL.
pub trait OutputBytesRead: BytesStream + BytesRead + Sync {}
impl<T> OutputBytesRead for T where T: BytesStream + BytesRead + Sync {}

/// OutputBytesReader is a boxed dyn [`OutputBytesRead`].
pub type OutputBytesReader = Box<dyn OutputBytesRead>;

/// BlockingBytesRead represents a blocking reader of bytes.
pub trait BlockingBytesRead: Read + Send {}
impl<T> BlockingBytesRead for T where T: Read + Send {}

/// BlockingBytesReader is a boxed dyn [`BlockingBytesRead`].
pub type BlockingBytesReader = Box<dyn BlockingBytesRead>;

/// BlockingOutputBytesRead is the output version of bytes reader
/// returned by OpenDAL.
pub trait BlockingOutputBytesRead: BlockingBytesRead + Sync {}
impl<T> BlockingOutputBytesRead for T where T: BlockingBytesRead + Sync {}

/// BlockingOutputBytesReader is a boxed dyn `BlockingOutputBytesRead`.
pub type BlockingOutputBytesReader = Box<dyn BlockingOutputBytesRead>;

/// BytesHandle represents a handle of bytes which can be read and seek.
pub trait BytesHandle: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T> BytesHandle for T where T: AsyncRead + AsyncSeek + Unpin + Send {}

/// BytesHandler is a boxed dyn `BytesHandle`.
pub type BytesHandler = Box<dyn BytesHandle>;

/// BytesHandle represents a handle of bytes which can be read an seek.
pub trait BlockingBytesHandle: Read + Seek + Send {}
impl<T> BlockingBytesHandle for T where T: Read + Seek + Send {}

/// BlockingBytesHandler is a boxed dyn `BlockingBytesHandle`.
pub type BlockingBytesHandler = Box<dyn BlockingBytesHandle>;

/// BytesWrite represents a writer of bytes.
pub trait BytesWrite: AsyncWrite + Unpin + Send {}
impl<T> BytesWrite for T where T: AsyncWrite + Unpin + Send {}

/// BytesWriter is a boxed dyn [`BytesWrite`].
pub type BytesWriter = Box<dyn BytesWrite>;

/// BytesStream represents a stream of bytes.
///
/// This trait is used as alias to `Stream<Item = Result<Bytes>> + Unpin + Send`.
pub trait BytesStream: Stream<Item = Result<Bytes>> + Unpin + Send {}
impl<T> BytesStream for T where T: Stream<Item = Result<Bytes>> + Unpin + Send {}

/// BytesSink represents a sink of bytes.
///
/// THis trait is used as alias to `Sink<Bytes, Error = Error> + Unpin + Send`.
pub trait BytesSink: Sink<Bytes, Error = Error> + Unpin + Send {}
impl<T> BytesSink for T where T: Sink<Bytes, Error = Error> + Unpin + Send {}
