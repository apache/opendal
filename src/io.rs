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
use std::io::Result;

use bytes::Bytes;
use futures::AsyncRead;
use futures::AsyncWrite;
use futures::Sink;
use futures::Stream;

/// BytesRead represents a reader of bytes.
pub trait BytesRead: AsyncRead + Unpin + Send {}
impl<T> BytesRead for T where T: AsyncRead + Unpin + Send {}

/// BytesReader is a boxed dyn [`BytesRead`].
pub type BytesReader = Box<dyn BytesRead>;

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

/// BytesStreamer is a boxed dyn [`BytesStream`].
pub type BytesStreamer = Box<dyn BytesStream>;

/// BytesSink represents a sink of bytes.
///
/// THis trait is used as alias to `Sink<Bytes, Error = Error> + Unpin + Send`.
pub trait BytesSink: Sink<Bytes, Error = Error> + Unpin + Send {}
impl<T> BytesSink for T where T: Sink<Bytes, Error = Error> + Unpin + Send {}

/// BytesSinker is a boxed dyn [`BytesSink`]
pub type BytesSinker = Box<dyn BytesSink>;
