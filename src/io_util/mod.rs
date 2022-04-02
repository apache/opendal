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

//! Providing IO utils like `into_sink`, `into_stream`.

mod into_stream;
pub use into_stream::into_stream;

mod into_sink;
pub use into_sink::into_sink;

mod into_reader;
pub use into_reader::into_reader;

mod into_writer;
pub use into_writer::into_writer;

mod read_observer;
pub use read_observer::observe_read;
pub use read_observer::ReadEvent;
pub use read_observer::ReadObserver;

mod write_observer;
pub use write_observer::observe_write;
pub use write_observer::WriteEvent;
pub use write_observer::WriteObserver;

mod http_body;
pub(crate) use http_body::new_http_channel;
pub(crate) use http_body::HttpBodyWriter;

mod seekable_reader;
pub use seekable_reader::seekable_read;
pub use seekable_reader::SeekableReader;
