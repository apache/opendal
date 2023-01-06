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

//! io Providing IO related functions like `into_sink`, `into_stream`.
//!
//! # NOTE
//!
//! This mod is not a part of OpenDAL's public API. We expose them out to make
//! it easier to develop services and layers outside opendal.

pub mod input;

mod api;
pub use api::BlockingOutputBytesRead;
pub use api::BlockingOutputBytesReader;
pub use api::BytesCursor;
pub use api::BytesSink;
pub use api::BytesStream;
pub use api::BytesStreamer;
pub use api::BytesWrite;
pub use api::BytesWriter;
pub use api::OutputBytesRead;
pub use api::OutputBytesReader;
pub use api::SeekableOutputBytesReader;

mod into_stream;
pub use into_stream::into_stream;

mod into_sink;
pub use into_sink::into_sink;

mod into_reader;
pub use into_reader::into_reader;

mod into_writer;
pub use into_writer::into_writer;

pub mod into_seekable_reader;

mod into_seekable_stream;
pub use into_seekable_stream::into_seekable_stream;

mod read_observer;
pub use read_observer::observe_read;
pub use read_observer::ReadEvent;
pub use read_observer::ReadObserver;

mod write_observer;
pub use write_observer::observe_write;
pub use write_observer::WriteEvent;
pub use write_observer::WriteObserver;

#[cfg(feature = "compress")]
mod compress;
#[cfg(feature = "compress")]
pub use compress::CompressAlgorithm;
#[cfg(feature = "compress")]
pub use compress::DecompressCodec;
#[cfg(feature = "compress")]
pub use compress::DecompressDecoder;
#[cfg(feature = "compress")]
pub use compress::DecompressReader;
#[cfg(feature = "compress")]
pub use compress::DecompressState;

mod walk;
pub use walk::BottomUpWalker;
pub use walk::TopDownWalker;
